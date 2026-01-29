.PHONY: help build test clean up down migrate-up migrate-down logs

# Default target
help:
	@echo "Address Scanner - Available Commands:"
	@echo ""
	@echo "  make build          - Build server and worker binaries"
	@echo "  make test           - Run all tests"
	@echo "  make test-unit      - Run unit tests only"
	@echo "  make test-integration - Run integration tests"
	@echo "  make up             - Start all services with docker-compose"
	@echo "  make down           - Stop all services"
	@echo "  make migrate-up     - Run database migrations"
	@echo "  make migrate-down   - Rollback last migration"
	@echo "  make logs           - View logs from all services"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make lint           - Run linters"
	@echo "  make fmt            - Format code"
	@echo ""

# Build binaries
build:
	@echo "Building binaries..."
	@mkdir -p bin
	@go build -o bin/server ./cmd/server
	@go build -o bin/worker ./cmd/worker
	@go build -o bin/migrate ./cmd/migrate
	@echo "Build complete: bin/server, bin/worker, bin/migrate"

# Run tests
test:
	@echo "Running all tests..."
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

test-unit:
	@echo "Running unit tests..."
	@go test -v -short ./...

test-integration:
	@echo "Running integration tests..."
	@go test -v -run Integration ./...

# Docker compose commands
up:
	@echo "Starting services..."
	@docker-compose up -d
	@echo "Waiting for services to be healthy..."
	@sleep 5
	@docker-compose ps

down:
	@echo "Stopping services..."
	@docker-compose down

# Database migrations
migrate-up:
	@echo "Running migrations..."
	@go run cmd/migrate/main.go up

migrate-down:
	@echo "Rolling back last migration..."
	@go run cmd/migrate/main.go down

# View logs
logs:
	@docker-compose logs -f

logs-server:
	@docker-compose logs -f server

logs-worker:
	@docker-compose logs -f worker

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html
	@rm -f server worker migrate
	@echo "Clean complete"

# Linting
lint:
	@echo "Running linters..."
	@golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@goimports -w .

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@go install github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	@echo "Tools installed"

# Run server locally
run-server:
	@echo "Starting API server..."
	@go run cmd/server/main.go

# Run worker locally
run-worker:
	@echo "Starting sync worker..."
	@go run cmd/worker/main.go

# Database commands
db-reset:
	@echo "Resetting database..."
	@docker-compose down -v
	@docker-compose up -d postgres clickhouse redis
	@sleep 5
	@make migrate-up
	@echo "Database reset complete"

# Generate API documentation
docs:
	@echo "Generating API documentation..."
	@swag init -g cmd/server/main.go -o docs/swagger
	@echo "Documentation generated: docs/swagger"

# Docker build
docker-build:
	@echo "Building Docker image..."
	@docker build -t address-scanner:latest .
	@echo "Docker image built: address-scanner:latest"

# Docker push (requires registry configuration)
docker-push:
	@echo "Pushing Docker image..."
	@docker tag address-scanner:latest ${REGISTRY}/address-scanner:${VERSION}
	@docker push ${REGISTRY}/address-scanner:${VERSION}
	@echo "Docker image pushed"
