package job

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// BackfillQueue manages a priority queue of backfill jobs
// Requirement 1.2: Job queue with priority support
type BackfillQueue struct {
	mu sync.RWMutex

	// Priority queue for pending jobs
	queue *PriorityQueue

	// Job repository for persistence
	jobRepo *storage.BackfillJobRepository

	// Worker pool
	workers       int
	workerSem     chan struct{}
	jobExecutor   *BackfillJobService
	stopCh        chan struct{}
	stopped       bool
	progressTrack map[string]*JobProgress
}

// JobProgress tracks the progress of a running backfill job
type JobProgress struct {
	JobID         string
	Address       string
	Status        string
	StartedAt     time.Time
	LastUpdated   time.Time
	ChainProgress map[types.ChainID]*ChainProgressDetail
}

// ChainProgressDetail tracks progress for a specific chain
type ChainProgressDetail struct {
	Chain               types.ChainID
	CurrentBlock        uint64
	TargetBlock         uint64
	TransactionsFetched int64
	LastUpdated         time.Time
}

// NewBackfillQueue creates a new backfill job queue
func NewBackfillQueue(
	jobRepo *storage.BackfillJobRepository,
	jobExecutor *BackfillJobService,
	workers int,
) *BackfillQueue {
	if workers <= 0 {
		workers = 5 // Default to 5 workers
	}

	return &BackfillQueue{
		queue:         &PriorityQueue{},
		jobRepo:       jobRepo,
		workers:       workers,
		workerSem:     make(chan struct{}, workers),
		jobExecutor:   jobExecutor,
		stopCh:        make(chan struct{}),
		progressTrack: make(map[string]*JobProgress),
	}
}

// Start begins processing jobs from the queue
func (q *BackfillQueue) Start(ctx context.Context) error {
	q.mu.Lock()
	if !q.stopped {
		q.mu.Unlock()
		return fmt.Errorf("queue already started")
	}
	q.stopped = false
	q.mu.Unlock()

	// Load queued jobs from database
	if err := q.loadQueuedJobs(ctx); err != nil {
		return fmt.Errorf("failed to load queued jobs: %w", err)
	}

	// Start worker pool
	go q.processJobs(ctx)

	return nil
}

// Stop gracefully stops the queue
func (q *BackfillQueue) Stop() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.stopped {
		return fmt.Errorf("queue already stopped")
	}

	close(q.stopCh)
	q.stopped = true

	return nil
}

// Enqueue adds a new job to the queue
// Requirement 1.2: Add job status tracking
func (q *BackfillQueue) Enqueue(ctx context.Context, input *BackfillJobInput) (*BackfillResult, error) {
	// Create job record
	result, err := q.jobExecutor.Execute(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create job: %w", err)
	}

	// Load job from database
	job, err := q.jobRepo.GetByID(ctx, result.JobID)
	if err != nil {
		return nil, fmt.Errorf("failed to load job: %w", err)
	}

	// Add to priority queue
	q.mu.Lock()
	heap.Push(q.queue, &QueueItem{
		Job:      job,
		Priority: job.Priority,
		Index:    -1,
	})
	q.mu.Unlock()

	return result, nil
}

// processJobs is the main worker loop
func (q *BackfillQueue) processJobs(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-q.stopCh:
			return
		case <-ticker.C:
			q.processNextJob(ctx)
		}
	}
}

// processNextJob processes the next job in the queue
func (q *BackfillQueue) processNextJob(ctx context.Context) {
	// Check if we have capacity
	select {
	case q.workerSem <- struct{}{}:
		// Got a worker slot
	default:
		// No workers available
		return
	}

	// Get next job from queue
	q.mu.Lock()
	if q.queue.Len() == 0 {
		q.mu.Unlock()
		<-q.workerSem // Release worker slot
		return
	}

	item := heap.Pop(q.queue).(*QueueItem)
	job := item.Job
	q.mu.Unlock()

	// Process job in goroutine
	go func() {
		defer func() {
			<-q.workerSem // Release worker slot
		}()

		// Update job status to in_progress
		job.Status = "in_progress"
		if err := q.jobRepo.Update(ctx, job); err != nil {
			fmt.Printf("Failed to update job status: %v\n", err)
			return
		}

		// Track progress
		q.trackJobStart(job)

		// Execute backfill
		q.jobExecutor.executeBackfill(ctx, job)

		// Remove from progress tracking
		q.removeJobProgress(job.JobID)
	}()
}

// loadQueuedJobs loads queued jobs from database on startup
func (q *BackfillQueue) loadQueuedJobs(ctx context.Context) error {
	jobs, err := q.jobRepo.GetQueuedJobs(ctx, 1000) // Load up to 1000 queued jobs
	if err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Initialize heap
	heap.Init(q.queue)

	// Add jobs to queue
	for _, job := range jobs {
		heap.Push(q.queue, &QueueItem{
			Job:      job,
			Priority: job.Priority,
			Index:    -1,
		})
	}

	fmt.Printf("Loaded %d queued jobs\n", len(jobs))
	return nil
}

// GetProgress retrieves progress for a specific job
// Requirement 1.2: Implement progress reporting
func (q *BackfillQueue) GetProgress(jobID string) (*JobProgress, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	progress, exists := q.progressTrack[jobID]
	if !exists {
		return nil, fmt.Errorf("job not found or not in progress: %s", jobID)
	}

	return progress, nil
}

// GetAllProgress returns progress for all running jobs
func (q *BackfillQueue) GetAllProgress() []*JobProgress {
	q.mu.RLock()
	defer q.mu.RUnlock()

	progress := make([]*JobProgress, 0, len(q.progressTrack))
	for _, p := range q.progressTrack {
		progress = append(progress, p)
	}

	return progress
}

// trackJobStart starts tracking a job's progress
func (q *BackfillQueue) trackJobStart(job *models.BackfillJobRecord) {
	q.mu.Lock()
	defer q.mu.Unlock()

	progress := &JobProgress{
		JobID:         job.JobID,
		Address:       job.Address,
		Status:        job.Status,
		StartedAt:     job.StartedAt,
		LastUpdated:   time.Now(),
		ChainProgress: make(map[types.ChainID]*ChainProgressDetail),
	}

	// Initialize chain progress for this job's chain
	progress.ChainProgress[job.Chain] = &ChainProgressDetail{
		Chain:               job.Chain,
		CurrentBlock:        0,
		TargetBlock:         0,
		TransactionsFetched: 0,
		LastUpdated:         time.Now(),
	}

	q.progressTrack[job.JobID] = progress
}

// UpdateChainProgress updates progress for a specific chain
func (q *BackfillQueue) UpdateChainProgress(jobID string, chain types.ChainID, currentBlock, targetBlock uint64, txFetched int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	progress, exists := q.progressTrack[jobID]
	if !exists {
		return
	}

	chainProgress, exists := progress.ChainProgress[chain]
	if !exists {
		chainProgress = &ChainProgressDetail{
			Chain: chain,
		}
		progress.ChainProgress[chain] = chainProgress
	}

	chainProgress.CurrentBlock = currentBlock
	chainProgress.TargetBlock = targetBlock
	chainProgress.TransactionsFetched = txFetched
	chainProgress.LastUpdated = time.Now()

	progress.LastUpdated = time.Now()
}

// removeJobProgress removes a job from progress tracking
func (q *BackfillQueue) removeJobProgress(jobID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	delete(q.progressTrack, jobID)
}

// GetQueueSize returns the current queue size
func (q *BackfillQueue) GetQueueSize() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return q.queue.Len()
}

// GetActiveJobs returns the number of currently running jobs
func (q *BackfillQueue) GetActiveJobs() int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.progressTrack)
}

// QueueItem represents an item in the priority queue
type QueueItem struct {
	Job      *models.BackfillJobRecord
	Priority int
	Index    int
}

// PriorityQueue implements heap.Interface for backfill jobs
// Higher priority values are processed first
type PriorityQueue []*QueueItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// Higher priority first
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*QueueItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
