package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

var chConn clickhouse.Conn

type NativeSnapshot struct {
	Date       time.Time
	BalanceRaw string
	TxCountIn  uint64
	TxCountOut uint64
}

func main() {
	addrFlag := flag.String("address", "", "Specific address to check (optional)")
	allFlag := flag.Bool("all", false, "Check all addresses")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: Could not load .env file: %v\n", err)
	}

	// Connect to ClickHouse
	var err error
	chConn, err = connectClickHouse()
	if err != nil {
		fmt.Printf("Error connecting to ClickHouse: %v\n", err)
		os.Exit(1)
	}
	defer chConn.Close()

	// Load addresses
	data, err := os.ReadFile("data/user_addresses.txt")
	if err != nil {
		fmt.Printf("Error reading addresses file: %v\n", err)
		os.Exit(1)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var addresses []string
	for _, line := range lines {
		addr := strings.TrimSpace(line)
		if addr != "" && strings.HasPrefix(addr, "0x") {
			addresses = append(addresses, addr)
		}
	}

	if len(addresses) == 0 {
		fmt.Println("No valid addresses found")
		os.Exit(1)
	}

	if *allFlag {
		// Check all addresses
		checkAllAddresses(addresses)
		return
	}

	var selectedAddr string
	if *addrFlag != "" {
		selectedAddr = *addrFlag
		fmt.Printf("Checking address: %s\n\n", selectedAddr)
	} else {
		selectedAddr = addresses[rand.Intn(len(addresses))]
		fmt.Printf("Randomly selected address: %s\n\n", selectedAddr)
	}

	checkSingleAddress(selectedAddr)
}

func checkAllAddresses(addresses []string) {
	fmt.Printf("Checking %d addresses...\n\n", len(addresses))

	var matched, mismatched, skipped int
	var mismatchedAddrs []string

	for i, addr := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		dbBalance, _, err := getDBBalance(ctx, addr)
		if err != nil {
			fmt.Printf("[%d/%d] %s - ERROR getting DB balance: %v\n", i+1, len(addresses), addr, err)
			cancel()
			skipped++
			continue
		}

		onChainBalance, _, err := getOnChainBalance(ctx, addr)
		if err != nil {
			fmt.Printf("[%d/%d] %s - ERROR getting on-chain balance: %v\n", i+1, len(addresses), addr, err)
			cancel()
			skipped++
			continue
		}

		cancel()

		diff := new(big.Int).Sub(onChainBalance, dbBalance)
		diffAbs := new(big.Int).Abs(diff)
		tolerance := big.NewInt(1000)

		if diffAbs.Cmp(tolerance) <= 0 {
			fmt.Printf("[%d/%d] %s ✅ MATCH (DB: %s, Chain: %s)\n", i+1, len(addresses), addr, dbBalance.String(), onChainBalance.String())
			matched++
		} else {
			fmt.Printf("[%d/%d] %s ❌ MISMATCH (DB: %s, Chain: %s, Diff: %s)\n", i+1, len(addresses), addr, dbBalance.String(), onChainBalance.String(), diff.String())
			mismatched++
			mismatchedAddrs = append(mismatchedAddrs, addr)
		}

		// Small delay to avoid RPC rate limits
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d, Matched: %d, Mismatched: %d, Skipped: %d\n", len(addresses), matched, mismatched, skipped)

	if len(mismatchedAddrs) > 0 {
		fmt.Printf("\nMismatched addresses:\n")
		for _, addr := range mismatchedAddrs {
			fmt.Printf("  %s\n", addr)
		}
	}
}

func checkSingleAddress(selectedAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// === 1. Get DB Balance from transactions ===
	dbBalance, txCount, err := getDBBalance(ctx, selectedAddr)
	if err != nil {
		fmt.Printf("Error getting DB balance: %v\n", err)
	}

	// === 2. Get On-Chain Balance ===
	onChainBalance, blockNum, err := getOnChainBalance(ctx, selectedAddr)
	if err != nil {
		fmt.Printf("Error getting on-chain balance: %v\n", err)
		os.Exit(1)
	}

	// === 3. Get Snapshot Data ===
	snapshots, err := getSnapshots(ctx, selectedAddr)
	if err != nil {
		fmt.Printf("Error getting snapshots: %v\n", err)
	}

	// === Print Results ===
	fmt.Printf("=== Balance Comparison for %s ===\n\n", selectedAddr)
	fmt.Printf("Current block: %d\n\n", blockNum)

	fmt.Printf("DB Balance (calculated from transactions):\n")
	fmt.Printf("  Wei:  %s\n", dbBalance.String())
	fmt.Printf("  ETH:  %.18f\n", weiToEth(dbBalance))
	fmt.Printf("  Tx count: %d\n\n", txCount)

	fmt.Printf("On-Chain Balance:\n")
	fmt.Printf("  Wei:  %s\n", onChainBalance.String())
	fmt.Printf("  ETH:  %.18f\n\n", weiToEth(onChainBalance))

	diff := new(big.Int).Sub(onChainBalance, dbBalance)
	diffAbs := new(big.Int).Abs(diff)

	fmt.Printf("Difference (On-Chain vs DB):\n")
	fmt.Printf("  Wei:  %s\n", diff.String())
	fmt.Printf("  ETH:  %.18f\n\n", weiToEth(diff))

	tolerance := big.NewInt(1000)
	if diffAbs.Cmp(tolerance) <= 0 {
		fmt.Printf("✅ MATCH: DB balance matches on-chain balance!\n\n")
	} else {
		fmt.Printf("❌ MISMATCH: DB balance differs from on-chain by %s wei\n", diffAbs.String())
		if diff.Sign() > 0 {
			fmt.Printf("   (On-chain has MORE than DB)\n\n")
		} else {
			fmt.Printf("   (DB has MORE than on-chain)\n\n")
		}
	}

	// === Print Snapshot Data ===
	fmt.Printf("=== Native Balance Snapshots ===\n")
	if len(snapshots) == 0 {
		fmt.Printf("  No snapshots found for this address\n")
	} else {
		fmt.Printf("  Found %d snapshot(s):\n\n", len(snapshots))
		for _, s := range snapshots {
			snapshotBal := new(big.Int)
			snapshotBal.SetString(s.BalanceRaw, 10)
			fmt.Printf("  Date: %s\n", s.Date.Format("2006-01-02"))
			fmt.Printf("    Balance: %s wei (%.6f ETH)\n", s.BalanceRaw, weiToEth(snapshotBal))
			fmt.Printf("    Tx In: %d, Tx Out: %d\n\n", s.TxCountIn, s.TxCountOut)
		}

		// Compare latest snapshot with current on-chain
		latest := snapshots[len(snapshots)-1]
		latestBal := new(big.Int)
		latestBal.SetString(latest.BalanceRaw, 10)
		snapshotDiff := new(big.Int).Sub(onChainBalance, latestBal)
		snapshotDiffAbs := new(big.Int).Abs(snapshotDiff)

		fmt.Printf("Latest Snapshot vs On-Chain:\n")
		fmt.Printf("  Snapshot date: %s\n", latest.Date.Format("2006-01-02"))
		fmt.Printf("  Difference: %s wei (%.18f ETH)\n", snapshotDiff.String(), weiToEth(snapshotDiff))
		if snapshotDiffAbs.Cmp(tolerance) <= 0 {
			fmt.Printf("  ✅ Snapshot matches current on-chain balance\n")
		} else if snapshotDiff.Sign() > 0 {
			fmt.Printf("  ⚠️  On-chain balance increased since snapshot\n")
		} else {
			fmt.Printf("  ⚠️  On-chain balance decreased since snapshot\n")
		}
	}

	fmt.Printf("\nVerify on Etherscan: https://etherscan.io/address/%s\n", selectedAddr)
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

func getSnapshots(ctx context.Context, address string) ([]NativeSnapshot, error) {
	address = strings.ToLower(address)

	query := `
		SELECT date, balance_raw, tx_count_in, tx_count_out
		FROM native_balance_snapshots
		WHERE address = ? AND chain = 'ethereum'
		ORDER BY date ASC
	`

	rows, err := chConn.Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []NativeSnapshot
	for rows.Next() {
		var s NativeSnapshot
		if err := rows.Scan(&s.Date, &s.BalanceRaw, &s.TxCountIn, &s.TxCountOut); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}

	return snapshots, nil
}

func getDBBalance(ctx context.Context, address string) (*big.Int, int64, error) {
	address = strings.ToLower(address)

	// Query directly from transactions table to avoid double-counting in materialized views
	query := `
		SELECT 
			toString(sum(CASE WHEN direction = 'in' THEN toDecimal256(value, 0) ELSE toDecimal256(0, 0) END)) as total_in,
			toString(sum(CASE WHEN direction = 'out' THEN toDecimal256(value, 0) ELSE toDecimal256(0, 0) END)) as total_out,
			toString(sum(toUInt256OrZero(gas_used) * toUInt256OrZero(gas_price))) as gas_spent,
			count(*) as tx_count
		FROM transactions
		WHERE address = ? AND chain = 'ethereum' AND transfer_type = 'native'
	`

	var totalInStr, totalOutStr, gasSpentStr string
	var txCount uint64

	row := chConn.QueryRow(ctx, query, address)
	if err := row.Scan(&totalInStr, &totalOutStr, &gasSpentStr, &txCount); err != nil {
		return big.NewInt(0), 0, fmt.Errorf("failed to query balance: %w", err)
	}

	totalIn := new(big.Int)
	totalOut := new(big.Int)
	gasSpent := new(big.Int)
	totalIn.SetString(totalInStr, 10)
	totalOut.SetString(totalOutStr, 10)
	gasSpent.SetString(gasSpentStr, 10)

	// Balance = totalIn - totalOut - gasSpent
	balance := new(big.Int).Sub(totalIn, totalOut)
	balance.Sub(balance, gasSpent)

	return balance, int64(txCount), nil
}

func getOnChainBalance(ctx context.Context, address string) (*big.Int, uint64, error) {
	rpcURLs := os.Getenv("ETHEREUM_RPC_URLS")
	if rpcURLs == "" {
		rpcURLs = os.Getenv("ETHEREUM_RPC_PRIMARY")
	}
	if rpcURLs == "" {
		return nil, 0, fmt.Errorf("no Ethereum RPC URL configured")
	}

	rpcURL := strings.Split(rpcURLs, ",")[0]

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Close()

	blockNum, err := client.BlockNumber(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get block number: %w", err)
	}

	addr := common.HexToAddress(address)
	balance, err := client.BalanceAt(ctx, addr, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get balance: %w", err)
	}

	return balance, blockNum, nil
}

func weiToEth(wei *big.Int) float64 {
	ethWei := new(big.Float).SetInt(wei)
	divisor := new(big.Float).SetFloat64(1e18)
	result, _ := new(big.Float).Quo(ethWei, divisor).Float64()
	return result
}
