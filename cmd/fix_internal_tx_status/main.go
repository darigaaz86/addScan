package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

var chConn clickhouse.Conn

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: Could not load .env file: %v\n", err)
	}

	var err error
	chConn, err = connectClickHouse()
	if err != nil {
		fmt.Printf("Error connecting to ClickHouse: %v\n", err)
		os.Exit(1)
	}
	defer chConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Known tx hashes that need to be fixed
	txHashes := []string{
		"0x030cc19de33957b96178782ad9c2689c0bec472ae15d562f432225e76f79085b",
		"0x804942a6a164dfcd74daeb83ad111f5ce5d0db5354583495ce022aca99f734fe",
		"0xec9971f80141c02903ed07aac4ba06f3169934053f915d50e65adfbca4798b4b",
		"0xd86b909620e87afeee0629ebd21ac938c0a983888fad19ef20e2cb88ecfeac73",
		"0x149acb7d074e22d85475a890a508a98ab50edc97f0e603e7c85237c47948098d",
	}

	fmt.Println("Fixing internal transactions status...")

	fixQuery := `
		ALTER TABLE transactions UPDATE status = 'success' WHERE
		status = 'failed'
		AND transfer_type = 'native'
		AND direction = 'in'
		AND tx_hash IN (?, ?, ?, ?, ?)
	`

	if err := chConn.Exec(ctx, fixQuery, txHashes[0], txHashes[1], txHashes[2], txHashes[3], txHashes[4]); err != nil {
		fmt.Printf("ERROR executing fix: %v\n", err)
		return
	}

	fmt.Println("Fix executed successfully!")
	fmt.Println("Waiting for mutation to complete...")

	// Wait a bit for mutation
	time.Sleep(3 * time.Second)

	// Verify the fix
	fmt.Println("\nVerifying fix...")
	verifyQuery := `
		SELECT tx_hash, status, value
		FROM transactions
		WHERE tx_hash IN (?, ?, ?, ?, ?)
		  AND transfer_type = 'native'
		  AND direction = 'in'
	`

	rows, err := chConn.Query(ctx, verifyQuery, txHashes[0], txHashes[1], txHashes[2], txHashes[3], txHashes[4])
	if err != nil {
		fmt.Printf("ERROR verifying: %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var txHash, status, value string
		rows.Scan(&txHash, &status, &value)
		fmt.Printf("TX: %s -> status: %s, value: %s\n", txHash[:20]+"...", status, value)
	}
}

func connectClickHouse() (clickhouse.Conn, error) {
	host := os.Getenv("CLICKHOUSE_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("CLICKHOUSE_PORT")
	if port == "" {
		port = "9000"
	}
	db := os.Getenv("CLICKHOUSE_DB")
	if db == "" {
		db = "address_scanner"
	}
	user := os.Getenv("CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}
	password := os.Getenv("CLICKHOUSE_PASSWORD")

	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: db,
			Username: user,
			Password: password,
		},
	})
}
