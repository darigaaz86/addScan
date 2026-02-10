// Package storage provides integration tests for balance verification.
// Run after backfill completes: go test -v ./internal/storage -run TestBalanceIntegration -tags=integration
package storage

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

// ERC20 balanceOf ABI
const erc20BalanceOfABI = `[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"type":"function"}]`

var integrationChainConfig = map[string]struct {
	rpcEnvVar    string
	nativeSymbol string
	explorerURL  string
}{
	"ethereum": {"ETHEREUM_RPC_URLS", "ETH", "https://etherscan.io/address/"},
	"base":     {"BASE_RPC_URLS", "ETH", "https://basescan.org/address/"},
	"bnb":      {"BNB_RPC_URLS", "BNB", "https://bscscan.com/address/"},
}

func TestBalanceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Load .env
	if err := godotenv.Load("../../.env"); err != nil {
		t.Logf("Warning: Could not load .env file: %v", err)
	}

	// Connect to ClickHouse
	chConn, err := connectTestClickHouse()
	if err != nil {
		t.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer chConn.Close()

	// Load addresses
	addresses, err := loadTestAddresses()
	if err != nil {
		t.Fatalf("Failed to load addresses: %v", err)
	}
	t.Logf("Loaded %d addresses from data/user_addresses.txt", len(addresses))

	// Get enabled chains
	enabledChains := os.Getenv("ENABLED_CHAINS")
	if enabledChains == "" {
		enabledChains = "ethereum,base,bnb"
	}
	chains := strings.Split(enabledChains, ",")

	for _, chain := range chains {
		chain = strings.TrimSpace(chain)
		t.Run(fmt.Sprintf("Chain_%s", chain), func(t *testing.T) {
			testChainBalances(t, chConn, chain, addresses)
		})
	}
}

func testChainBalances(t *testing.T, chConn clickhouse.Conn, chain string, addresses []string) {
	cfg, ok := integrationChainConfig[chain]
	if !ok {
		t.Skipf("Unsupported chain: %s", chain)
		return
	}

	// Get RPC URL
	rpcURL := os.Getenv(cfg.rpcEnvVar)
	if rpcURL == "" {
		rpcURL = os.Getenv(strings.Replace(cfg.rpcEnvVar, "_URLS", "_PRIMARY", 1))
	}
	if rpcURL == "" {
		t.Skipf("No RPC URL configured for %s", chain)
		return
	}
	rpcURL = strings.Split(rpcURL, ",")[0]

	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		t.Fatalf("Failed to connect to RPC: %v", err)
	}
	defer client.Close()

	// Test native balances
	t.Run("NativeBalances", func(t *testing.T) {
		testNativeBalances(t, chConn, client, chain, addresses, cfg.nativeSymbol)
	})

	// Test token balances
	t.Run("TokenBalances", func(t *testing.T) {
		testTokenBalances(t, chConn, client, chain, addresses)
	})
}

func testNativeBalances(t *testing.T, chConn clickhouse.Conn, client *ethclient.Client, chain string, addresses []string, symbol string) {
	var matched, mismatched, skipped int
	var mismatchedAddrs []string

	for _, addr := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		dbBalance, err := getTestDBNativeBalance(ctx, chConn, addr, chain)
		if err != nil {
			t.Logf("%s - ERROR getting DB balance: %v", addr[:10], err)
			cancel()
			skipped++
			continue
		}

		onChainBalance, err := getTestOnChainBalance(ctx, client, addr)
		if err != nil {
			t.Logf("%s - ERROR getting on-chain balance: %v", addr[:10], err)
			cancel()
			skipped++
			continue
		}
		cancel()

		diff := new(big.Int).Sub(onChainBalance, dbBalance)
		diffAbs := new(big.Int).Abs(diff)
		tolerance := big.NewInt(1000) // Allow tiny rounding differences

		if diffAbs.Cmp(tolerance) <= 0 {
			matched++
		} else {
			t.Errorf("%s MISMATCH: DB=%s, Chain=%s, Diff=%s %s",
				addr[:10], dbBalance.String(), onChainBalance.String(), diff.String(), symbol)
			mismatched++
			mismatchedAddrs = append(mismatchedAddrs, addr)
		}

		time.Sleep(100 * time.Millisecond) // Rate limit
	}

	t.Logf("Native balance summary: Matched=%d, Mismatched=%d, Skipped=%d", matched, mismatched, skipped)

	if mismatched > 0 {
		t.Logf("Mismatched addresses:")
		for _, addr := range mismatchedAddrs {
			t.Logf("  %s", addr)
		}
	}
}

func testTokenBalances(t *testing.T, chConn clickhouse.Conn, client *ethclient.Client, chain string, addresses []string) {
	// Get all token balances from DB
	balances, err := getTestDBTokenBalances(context.Background(), chConn, chain, addresses)
	if err != nil {
		t.Fatalf("Failed to get DB token balances: %v", err)
	}

	if len(balances) == 0 {
		t.Log("No token balances found in DB")
		return
	}

	t.Logf("Checking %d token balances", len(balances))

	// Parse ERC20 ABI
	parsedABI, err := abi.JSON(strings.NewReader(erc20BalanceOfABI))
	if err != nil {
		t.Fatalf("Failed to parse ABI: %v", err)
	}

	var matched, mismatched, skipped int

	for _, bal := range balances {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		onChainBal, err := getTestOnChainTokenBalance(ctx, client, parsedABI, bal.Address, bal.TokenAddress)
		cancel()

		if err != nil {
			skipped++
			continue
		}

		dbBal := new(big.Int)
		dbBal.SetString(bal.DBBalance, 10)

		diff := new(big.Int).Sub(onChainBal, dbBal)

		if diff.Cmp(big.NewInt(0)) == 0 {
			matched++
		} else {
			t.Errorf("%s %s MISMATCH: DB=%s, Chain=%s, Diff=%s",
				bal.Address[:10], bal.TokenSymbol, bal.DBBalance, onChainBal.String(), diff.String())
			mismatched++
		}

		time.Sleep(100 * time.Millisecond) // Rate limit
	}

	t.Logf("Token balance summary: Matched=%d, Mismatched=%d, Skipped=%d", matched, mismatched, skipped)
}

type testTokenBalance struct {
	Address      string
	TokenAddress string
	TokenSymbol  string
	DBBalance    string
}

func connectTestClickHouse() (clickhouse.Conn, error) {
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

func loadTestAddresses() ([]string, error) {
	data, err := os.ReadFile("../../data/user_addresses.txt")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	var addresses []string
	for _, line := range lines {
		addr := strings.TrimSpace(line)
		if addr != "" && strings.HasPrefix(addr, "0x") {
			addresses = append(addresses, strings.ToLower(addr))
		}
	}
	return addresses, nil
}

func getTestDBNativeBalance(ctx context.Context, chConn clickhouse.Conn, address, chain string) (*big.Int, error) {
	address = strings.ToLower(address)

	query := `
		SELECT 
			toString(sum(CASE WHEN direction = 'in' THEN toDecimal256(value, 0) ELSE toDecimal256(0, 0) END)) as total_in,
			toString(sum(CASE WHEN direction = 'out' THEN toDecimal256(value, 0) ELSE toDecimal256(0, 0) END)) as total_out,
			toString(sum(toUInt256OrZero(gas_used) * toUInt256OrZero(gas_price))) as gas_spent,
			toString(sum(toUInt256OrZero(l1_fee))) as l1_fee_spent
		FROM transactions
		WHERE address = ? AND chain = ? AND transfer_type = 'native'
	`

	var totalInStr, totalOutStr, gasSpentStr, l1FeeSpentStr string
	row := chConn.QueryRow(ctx, query, address, chain)
	if err := row.Scan(&totalInStr, &totalOutStr, &gasSpentStr, &l1FeeSpentStr); err != nil {
		return big.NewInt(0), err
	}

	totalIn := new(big.Int)
	totalOut := new(big.Int)
	gasSpent := new(big.Int)
	l1FeeSpent := new(big.Int)
	totalIn.SetString(totalInStr, 10)
	totalOut.SetString(totalOutStr, 10)
	gasSpent.SetString(gasSpentStr, 10)
	l1FeeSpent.SetString(l1FeeSpentStr, 10)

	// Balance = totalIn - totalOut - gasSpent - l1FeeSpent
	balance := new(big.Int).Sub(totalIn, totalOut)
	balance.Sub(balance, gasSpent)
	balance.Sub(balance, l1FeeSpent)

	return balance, nil
}

func getTestOnChainBalance(ctx context.Context, client *ethclient.Client, address string) (*big.Int, error) {
	addr := common.HexToAddress(address)
	return client.BalanceAt(ctx, addr, nil)
}

func getTestDBTokenBalances(ctx context.Context, chConn clickhouse.Conn, chain string, addresses []string) ([]testTokenBalance, error) {
	// Build address list for IN clause
	addrList := make([]string, len(addresses))
	for i, addr := range addresses {
		addrList[i] = strings.ToLower(addr)
	}

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
		AND address IN (?)
		GROUP BY address, token_address, token_symbol
		HAVING balance != '0'
		ORDER BY address, token_symbol
	`

	rows, err := chConn.Query(ctx, query, chain, addrList)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []testTokenBalance
	for rows.Next() {
		var b testTokenBalance
		if err := rows.Scan(&b.Address, &b.TokenAddress, &b.TokenSymbol, &b.DBBalance); err != nil {
			return nil, err
		}
		balances = append(balances, b)
	}
	return balances, nil
}

func getTestOnChainTokenBalance(ctx context.Context, client *ethclient.Client, parsedABI abi.ABI, owner, token string) (*big.Int, error) {
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
