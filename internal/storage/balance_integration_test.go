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
	"github.com/address-scanner/internal/adapter"
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

	// Get RPC URLs (comma-separated for pool)
	rpcURLs := os.Getenv(cfg.rpcEnvVar)
	if rpcURLs == "" {
		// Fallback to legacy single URL
		rpcURLs = os.Getenv(strings.Replace(cfg.rpcEnvVar, "_URLS", "_PRIMARY", 1))
	}
	if rpcURLs == "" {
		t.Skipf("No RPC URL configured for %s", chain)
		return
	}

	// Create RPC pool with failover support
	pool, err := adapter.NewRPCPoolFromURLs(rpcURLs)
	if err != nil {
		t.Fatalf("Failed to create RPC pool: %v", err)
	}
	defer pool.Close()

	t.Logf("RPC pool created with %d endpoints for %s", pool.EndpointCount(), chain)

	// Test native balances
	t.Run("NativeBalances", func(t *testing.T) {
		testNativeBalances(t, chConn, pool, chain, addresses, cfg.nativeSymbol)
	})

	// Test token balances
	t.Run("TokenBalances", func(t *testing.T) {
		testTokenBalances(t, chConn, pool, chain, addresses)
	})
}

func testNativeBalances(t *testing.T, chConn clickhouse.Conn, pool *adapter.RPCPool, chain string, addresses []string, symbol string) {
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

		onChainBalance, err := getTestOnChainBalanceWithPool(ctx, pool, addr)
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

		time.Sleep(200 * time.Millisecond) // Rate limit between calls
	}

	t.Logf("Native balance summary: Matched=%d, Mismatched=%d, Skipped=%d", matched, mismatched, skipped)

	if mismatched > 0 {
		t.Logf("Mismatched addresses:")
		for _, addr := range mismatchedAddrs {
			t.Logf("  %s", addr)
		}
	}
}

func testTokenBalances(t *testing.T, chConn clickhouse.Conn, pool *adapter.RPCPool, chain string, addresses []string) {
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
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		onChainBal, err := getTestOnChainTokenBalanceWithPool(ctx, pool, parsedABI, bal.Address, bal.TokenAddress)
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

		time.Sleep(200 * time.Millisecond) // Rate limit between calls
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
		SELECT toString(balance)
		FROM native_balances_final
		WHERE address = ? AND chain = ?
	`

	var balanceStr string
	row := chConn.QueryRow(ctx, query, address, chain)
	if err := row.Scan(&balanceStr); err != nil {
		return big.NewInt(0), err
	}

	balance := new(big.Int)
	balance.SetString(balanceStr, 10)

	return balance, nil
}

func getTestOnChainBalance(ctx context.Context, client *ethclient.Client, address string) (*big.Int, error) {
	addr := common.HexToAddress(address)
	return client.BalanceAt(ctx, addr, nil)
}

// getTestOnChainBalanceWithPool fetches on-chain balance using the RPC pool.
// On 429/rate-limit errors, it fails over to the next endpoint and retries.
const maxRPCRetries = 3

func getTestOnChainBalanceWithPool(ctx context.Context, pool *adapter.RPCPool, address string) (*big.Int, error) {
	addr := common.HexToAddress(address)

	for attempt := 0; attempt <= maxRPCRetries; attempt++ {
		balance, err := pool.GetClient().BalanceAt(ctx, addr, nil)
		if err == nil {
			return balance, nil
		}

		if adapter.IsRateLimitError(err) {
			if failoverErr := pool.OnRateLimited(ctx); failoverErr != nil {
				return nil, fmt.Errorf("all endpoints exhausted: %w", failoverErr)
			}
			// Brief pause after failover before retry
			time.Sleep(500 * time.Millisecond)
			continue
		}

		return nil, err
	}

	return nil, fmt.Errorf("failed after %d retries", maxRPCRetries)
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
			toString(balance) as balance
		FROM token_balances_final
		WHERE chain = ?
		AND address IN (?)
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

// getTestOnChainTokenBalanceWithPool fetches on-chain token balance using the RPC pool.
func getTestOnChainTokenBalanceWithPool(ctx context.Context, pool *adapter.RPCPool, parsedABI abi.ABI, owner, token string) (*big.Int, error) {
	tokenAddr := common.HexToAddress(token)
	ownerAddr := common.HexToAddress(owner)

	data, err := parsedABI.Pack("balanceOf", ownerAddr)
	if err != nil {
		return nil, err
	}

	for attempt := 0; attempt <= maxRPCRetries; attempt++ {
		result, err := pool.GetClient().CallContract(ctx, ethereum.CallMsg{
			To:   &tokenAddr,
			Data: data,
		}, nil)
		if err == nil {
			if len(result) == 0 {
				return big.NewInt(0), nil
			}
			return new(big.Int).SetBytes(result), nil
		}

		if adapter.IsRateLimitError(err) {
			if failoverErr := pool.OnRateLimited(ctx); failoverErr != nil {
				return nil, fmt.Errorf("all endpoints exhausted: %w", failoverErr)
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		return nil, err
	}

	return nil, fmt.Errorf("failed after %d retries", maxRPCRetries)
}
