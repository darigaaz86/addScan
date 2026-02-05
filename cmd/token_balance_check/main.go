package main

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

var chConn clickhouse.Conn

// ERC20 balanceOf ABI
const erc20ABI = `[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]`

type TokenBalance struct {
	Address      string
	TokenAddress string
	TokenSymbol  string
	DBBalance    string
}

var chainConfig = map[string]struct {
	rpcEnvVar   string
	explorerURL string
}{
	"ethereum": {"ETHEREUM_RPC_URLS", "https://etherscan.io/address/"},
	"base":     {"BASE_RPC_URLS", "https://basescan.org/address/"},
	"bnb":      {"BNB_RPC_URLS", "https://bscscan.com/address/"},
}

var selectedChain string

func main() {
	addrFlag := flag.String("address", "", "Specific address to check (optional)")
	tokenFlag := flag.String("token", "", "Specific token address to check (optional)")
	chainFlag := flag.String("chain", "ethereum", "Chain to check (ethereum, base, bnb)")
	flag.Parse()

	selectedChain = strings.ToLower(*chainFlag)
	if _, ok := chainConfig[selectedChain]; !ok {
		fmt.Printf("Unsupported chain: %s (supported: ethereum, base, bnb)\n", selectedChain)
		os.Exit(1)
	}
	fmt.Printf("Chain: %s\n", selectedChain)

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

	// Get token balances from DB
	balances, err := getDBTokenBalances(*addrFlag, *tokenFlag)
	if err != nil {
		fmt.Printf("Error getting DB balances: %v\n", err)
		os.Exit(1)
	}

	if len(balances) == 0 {
		fmt.Println("No token balances found in DB")
		return
	}

	fmt.Printf("Checking %d token balances...\n\n", len(balances))

	// Connect to RPC
	cfg := chainConfig[selectedChain]
	rpcURL := os.Getenv(cfg.rpcEnvVar)
	if rpcURL == "" {
		rpcURL = os.Getenv(strings.Replace(cfg.rpcEnvVar, "_URLS", "_PRIMARY", 1))
	}
	if rpcURL == "" {
		fmt.Printf("Error: No RPC URL configured for %s\n", selectedChain)
		os.Exit(1)
	}
	rpcURL = strings.Split(rpcURL, ",")[0]

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		fmt.Printf("Error connecting to Ethereum: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Parse ERC20 ABI
	parsedABI, err := abi.JSON(strings.NewReader(erc20ABI))
	if err != nil {
		fmt.Printf("Error parsing ABI: %v\n", err)
		os.Exit(1)
	}

	var matched, mismatched, skipped int
	var mismatchedList []string

	for i, bal := range balances {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		onChainBal, err := getOnChainTokenBalance(ctx, client, parsedABI, bal.Address, bal.TokenAddress)
		cancel()

		if err != nil {
			fmt.Printf("[%d/%d] %s %s - ERROR: %v\n", i+1, len(balances), bal.Address[:10], bal.TokenSymbol, err)
			skipped++
			continue
		}

		dbBal := new(big.Int)
		dbBal.SetString(bal.DBBalance, 10)

		diff := new(big.Int).Sub(onChainBal, dbBal)
		diffAbs := new(big.Int).Abs(diff)

		if diffAbs.Cmp(big.NewInt(0)) == 0 {
			fmt.Printf("[%d/%d] %s %s ✅ MATCH (DB: %s, Chain: %s)\n",
				i+1, len(balances), bal.Address[:10], bal.TokenSymbol, bal.DBBalance, onChainBal.String())
			matched++
		} else {
			fmt.Printf("[%d/%d] %s %s ❌ MISMATCH (DB: %s, Chain: %s, Diff: %s)\n",
				i+1, len(balances), bal.Address[:10], bal.TokenSymbol, bal.DBBalance, onChainBal.String(), diff.String())
			mismatched++
			mismatchedList = append(mismatchedList, fmt.Sprintf("%s %s", bal.Address, bal.TokenSymbol))
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total: %d, Matched: %d, Mismatched: %d, Skipped: %d\n", len(balances), matched, mismatched, skipped)

	if len(mismatchedList) > 0 {
		fmt.Printf("\nMismatched:\n")
		for _, m := range mismatchedList {
			fmt.Printf("  %s\n", m)
		}
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

func getDBTokenBalances(address, token string) ([]TokenBalance, error) {
	query := `
		SELECT 
			address,
			token_address,
			token_symbol,
			toString(sumIf(toInt256OrZero(value), direction = 'in') - sumIf(toInt256OrZero(value), direction = 'out')) as balance
		FROM transactions 
		WHERE chain = ?
		AND transfer_type = 'erc20'
		AND is_spam = 0
	`
	args := []any{selectedChain}

	if address != "" {
		query += " AND address = ?"
		args = append(args, strings.ToLower(address))
	}
	if token != "" {
		query += " AND token_address = ?"
		args = append(args, strings.ToLower(token))
	}

	query += `
		GROUP BY address, token_address, token_symbol
		HAVING balance != '0'
		ORDER BY address, token_symbol
	`

	ctx := context.Background()
	rows, err := chConn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []TokenBalance
	for rows.Next() {
		var b TokenBalance
		if err := rows.Scan(&b.Address, &b.TokenAddress, &b.TokenSymbol, &b.DBBalance); err != nil {
			return nil, err
		}
		balances = append(balances, b)
	}
	return balances, nil
}

func getOnChainTokenBalance(ctx context.Context, client *ethclient.Client, parsedABI abi.ABI, owner, token string) (*big.Int, error) {
	tokenAddr := common.HexToAddress(token)
	ownerAddr := common.HexToAddress(owner)

	data, err := parsedABI.Pack("balanceOf", ownerAddr)
	if err != nil {
		return nil, err
	}

	result, err := client.CallContract(ctx, ethereum.CallMsg{
		To:   &tokenAddr,
		Data: data,
	}, nil)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return big.NewInt(0), nil
	}

	return new(big.Int).SetBytes(result), nil
}
