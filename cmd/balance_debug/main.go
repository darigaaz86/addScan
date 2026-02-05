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
	godotenv.Load()
	chConn, _ = connectClickHouse()
	defer chConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Fix all 4 remaining addresses
	fixes := []struct {
		address string
		txHash  string
		value   string
	}{
		{"0xf4a6d658abcee18204aa62bcdf5f67992bbdf45e", "0x030cc19de33957b96178782ad9c2689c0bec472ae15d562f432225e76f79085b", "70509784670618711"},
		{"0x16ca842c2493a75cb4046cd8c1c45b962d981d1d", "0xec9971f80141c02903ed07aac4ba06f3169934053f915d50e65adfbca4798b4b", "5298294348553984"},
		{"0xdc694fec371beaba6af58bb3f28b135a4a0fa4b3", "0xd86b909620e87afeee0629ebd21ac938c0a983888fad19ef20e2cb88ecfeac73", "13997980161386752"},
		{"0x8efc306a77bb56b31ff5a1697f288e1c9588b833", "0x804942a6a164dfcd74daeb83ad111f5ce5d0db5354583495ce022aca99f734fe", "121612015499747454"},
	}

	for _, fix := range fixes {
		fmt.Printf("\n=== Fixing %s ===\n", fix.address)
		fmt.Printf("TX: %s\n", fix.txHash)

		// Get gas values from ERC20 row (log_index=1)
		var gasUsed, gasPrice string
		var blockNumber uint64
		var timestamp time.Time
		err := chConn.QueryRow(ctx, `
			SELECT gas_used, gas_price, block_number, timestamp FROM transactions 
			WHERE tx_hash = ? AND address = ? AND log_index = 1
		`, fix.txHash, fix.address).Scan(&gasUsed, &gasPrice, &blockNumber, &timestamp)
		if err != nil {
			fmt.Printf("ERROR getting gas: %v\n", err)
			continue
		}
		fmt.Printf("Gas from ERC20: %s * %s\n", gasUsed, gasPrice)

		// Step 1: Update native row (log_index=0) to be gas payment (direction=out, value=0)
		update1 := `
			ALTER TABLE transactions UPDATE 
				direction = 'out',
				value = '0',
				tx_from = ?,
				transfer_from = ?,
				transfer_to = '0x74de5d4fcbf63e00296fd95d33236b9794016631',
				gas_used = ?,
				gas_price = ?
			WHERE tx_hash = ? AND address = ? AND log_index = 0 AND transfer_type = 'native'
		`
		if err := chConn.Exec(ctx, update1, fix.address, fix.address, gasUsed, gasPrice, fix.txHash, fix.address); err != nil {
			fmt.Printf("ERROR update1: %v\n", err)
			continue
		}

		// Step 2: Clear gas from ERC20 row
		update2 := `
			ALTER TABLE transactions UPDATE 
				gas_used = '',
				gas_price = ''
			WHERE tx_hash = ? AND address = ? AND log_index = 1
		`
		if err := chConn.Exec(ctx, update2, fix.txHash, fix.address); err != nil {
			fmt.Printf("ERROR update2: %v\n", err)
			continue
		}

		// Step 3: Insert new native IN record for ETH received
		insert := `
			INSERT INTO transactions (
				tx_hash, log_index, chain, address,
				tx_from, tx_to,
				transfer_type, transfer_from, transfer_to, value, direction,
				token_address, token_symbol, token_decimals, token_id,
				block_number, timestamp, status, gas_used, gas_price, method_id, func_name, is_spam
			) VALUES (
				?, 2, 'ethereum', ?,
				?, '0x74de5d4fcbf63e00296fd95d33236b9794016631',
				'native', '0x74de5d4fcbf63e00296fd95d33236b9794016631', ?, ?, 'in',
				'', '', 18, '',
				?, ?, 'success', '', '', '', '', 0
			)
		`
		if err := chConn.Exec(ctx, insert, fix.txHash, fix.address, fix.address, fix.address, fix.value, blockNumber, timestamp); err != nil {
			fmt.Printf("ERROR insert: %v\n", err)
			continue
		}

		fmt.Println("Fixed!")
	}

	fmt.Println("\nWaiting for mutations...")
	time.Sleep(5 * time.Second)

	// Verify
	fmt.Println("\n=== Verification ===")
	for _, fix := range fixes {
		fmt.Printf("\n%s:\n", fix.address)
		rows, _ := chConn.Query(ctx, `
			SELECT log_index, transfer_type, direction, value, gas_used, gas_price
			FROM transactions WHERE address = ? AND tx_hash = ? ORDER BY log_index
		`, fix.address, fix.txHash)
		for rows.Next() {
			var logIndex uint32
			var transferType, direction, value, gasUsed, gasPrice string
			rows.Scan(&logIndex, &transferType, &direction, &value, &gasUsed, &gasPrice)
			fmt.Printf("  log=%d type=%s dir=%s value=%s gas=%s*%s\n",
				logIndex, transferType, direction, value, gasUsed, gasPrice)
		}
		rows.Close()
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
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: os.Getenv("CLICKHOUSE_DB"),
			Username: os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	})
}
