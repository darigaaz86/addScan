package storage

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// RunClickHouseMigrations runs ClickHouse migrations from a directory
func RunClickHouseMigrations(db *ClickHouseDB, migrationsPath string) error {
	ctx := context.Background()

	// Read migration files
	files, err := os.ReadDir(migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Filter and sort SQL files
	var sqlFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, file.Name())
		}
	}
	sort.Strings(sqlFiles)

	if len(sqlFiles) == 0 {
		log.Println("No migration files found")
		return nil
	}

	// Execute each migration file
	for _, filename := range sqlFiles {
		filePath := filepath.Join(migrationsPath, filename)
		content, err := os.ReadFile(filePath) // #nosec G304 - filePath is constructed from trusted migrationsPath
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", filename, err)
		}

		log.Printf("Processing migration file: %s", filename)

		// Split by semicolon and newline to handle multiple statements
		statements := splitSQLStatements(string(content))

		for i, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}

			log.Printf("  Executing statement %d: %s...", i+1, truncate(stmt, 80))

			if err := db.Exec(ctx, stmt); err != nil {
				log.Printf("  ERROR executing statement: %v", err)
				log.Printf("  Statement was: %s", stmt)
				return fmt.Errorf("failed to execute statement in %s: %w", filename, err)
			}

			log.Printf("  ✓ Statement %d executed successfully", i+1)
		}

		fmt.Printf("Applied migration: %s\n", filename)
	}

	return nil
}

// splitSQLStatements splits SQL content into individual statements
// It handles comments and multi-line statements properly
func splitSQLStatements(content string) []string {
	var statements []string
	var currentStmt strings.Builder

	lines := strings.Split(content, "\n")

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines and comment-only lines
		if trimmedLine == "" || strings.HasPrefix(trimmedLine, "--") {
			continue
		}

		// Add line to current statement
		currentStmt.WriteString(line)
		currentStmt.WriteString("\n")

		// If line ends with semicolon, it's the end of a statement
		if strings.HasSuffix(trimmedLine, ";") {
			stmt := strings.TrimSpace(currentStmt.String())
			// Remove trailing semicolon as ClickHouse doesn't need it
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				statements = append(statements, stmt)
			}
			currentStmt.Reset()
		}
	}

	// Add any remaining statement
	if currentStmt.Len() > 0 {
		stmt := strings.TrimSpace(currentStmt.String())
		stmt = strings.TrimSuffix(stmt, ";")
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	return statements
}

// truncate truncates a string to maxLen characters
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
