package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/types"
	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: Could not load .env file: %v\n", err)
	}

	address := "0xb6DD4cd17CF4aB850944FE368A40c1490699CF81"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	apiKey := os.Getenv("ETHERSCAN_API_KEY")
	if apiKey == "" {
		fmt.Println("Error: ETHERSCAN_API_KEY not set")
		os.Exit(1)
	}

	client := adapter.NewEtherscanClient(apiKey)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	fmt.Printf("Fetching transactions for %s from Etherscan...\n\n", address)

	txs, err := client.FetchAllTransactions(ctx, address, types.ChainEthereum)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Categorize transactions
	var normal, internal, erc20, erc721, erc1155 []*types.NormalizedTransaction
	for _, tx := range txs {
		cat := ""
		if tx.Category != nil {
			cat = *tx.Category
		}
		switch cat {
		case "erc20":
			erc20 = append(erc20, tx)
		case "erc721":
			erc721 = append(erc721, tx)
		case "erc1155":
			erc1155 = append(erc1155, tx)
		default:
			// Check if it's internal by looking at asset
			if tx.Asset != nil && *tx.Asset == "ETH" {
				// Internal txs don't have gas info typically
				if tx.GasUsed == nil || *tx.GasUsed == "" {
					internal = append(internal, tx)
				} else {
					normal = append(normal, tx)
				}
			} else {
				normal = append(normal, tx)
			}
		}
	}

	fmt.Printf("=== Transaction Summary ===\n")
	fmt.Printf("Normal (external): %d\n", len(normal))
	fmt.Printf("Internal:          %d\n", len(internal))
	fmt.Printf("ERC20:             %d\n", len(erc20))
	fmt.Printf("ERC721:            %d\n", len(erc721))
	fmt.Printf("ERC1155:           %d\n", len(erc1155))
	fmt.Printf("Total:             %d\n\n", len(txs))

	// Calculate balance from normal + internal
	addrLower := strings.ToLower(address)
	totalIn := big.NewInt(0)
	totalOut := big.NewInt(0)
	totalGas := big.NewInt(0)

	fmt.Printf("=== Normal Transactions (first 10) ===\n")
	for i, tx := range normal {
		if i < 10 {
			dir := "OUT"
			if strings.EqualFold(tx.To, address) {
				dir = "IN"
			}
			fmt.Printf("%s | %s | %s wei | from: %s | to: %s\n",
				tx.Hash[:16]+"...", dir, tx.Value, tx.From[:10]+"...", tx.To[:10]+"...")
		}

		val := new(big.Int)
		val.SetString(tx.Value, 10)

		if strings.EqualFold(tx.To, address) {
			totalIn.Add(totalIn, val)
		}
		if strings.EqualFold(tx.From, address) {
			totalOut.Add(totalOut, val)
			// Add gas
			if tx.GasUsed != nil && tx.GasPrice != nil {
				gasUsed := new(big.Int)
				gasPrice := new(big.Int)
				gasUsed.SetString(*tx.GasUsed, 10)
				gasPrice.SetString(*tx.GasPrice, 10)
				gasFee := new(big.Int).Mul(gasUsed, gasPrice)
				totalGas.Add(totalGas, gasFee)
			}
		}
	}

	fmt.Printf("\n=== Internal Transactions (first 10) ===\n")
	for i, tx := range internal {
		if i < 10 {
			dir := "OUT"
			if strings.EqualFold(tx.To, address) {
				dir = "IN"
			}
			fmt.Printf("%s | %s | %s wei | from: %s | to: %s\n",
				tx.Hash[:16]+"...", dir, tx.Value, tx.From[:10]+"...", tx.To[:10]+"...")
		}

		val := new(big.Int)
		val.SetString(tx.Value, 10)

		if strings.ToLower(tx.To) == addrLower {
			totalIn.Add(totalIn, val)
		}
		if strings.ToLower(tx.From) == addrLower {
			totalOut.Add(totalOut, val)
		}
	}

	// Calculate balance
	balance := new(big.Int).Sub(totalIn, totalOut)
	balance.Sub(balance, totalGas)

	fmt.Printf("\n=== Balance Calculation ===\n")
	fmt.Printf("Total In:  %s wei (%.6f ETH)\n", totalIn.String(), weiToEth(totalIn))
	fmt.Printf("Total Out: %s wei (%.6f ETH)\n", totalOut.String(), weiToEth(totalOut))
	fmt.Printf("Total Gas: %s wei (%.6f ETH)\n", totalGas.String(), weiToEth(totalGas))
	fmt.Printf("Calculated Balance: %s wei (%.6f ETH)\n", balance.String(), weiToEth(balance))
}

func weiToEth(wei *big.Int) float64 {
	f := new(big.Float).SetInt(wei)
	div := new(big.Float).SetFloat64(1e18)
	result, _ := new(big.Float).Quo(f, div).Float64()
	return result
}
