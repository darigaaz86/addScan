package models

import (
	"strings"
	"time"

	"github.com/address-scanner/internal/types"
)

// Transaction represents a transfer record stored in ClickHouse
// One row per transfer (native or token), multiple rows per tx hash when needed
type Transaction struct {
	// Identity
	TxHash   string        `json:"txHash" ch:"tx_hash"`
	LogIndex uint32        `json:"logIndex" ch:"log_index"` // 0 for native, 1+ for tokens
	Chain    types.ChainID `json:"chain" ch:"chain"`
	Address  string        `json:"address" ch:"address"` // Tracked wallet address

	// Transaction level (who initiated)
	TxFrom string `json:"txFrom" ch:"tx_from"`
	TxTo   string `json:"txTo" ch:"tx_to"`

	// Transfer level (actual value movement)
	TransferType types.TransferType         `json:"transferType" ch:"transfer_type"`
	TransferFrom string                     `json:"transferFrom" ch:"transfer_from"`
	TransferTo   string                     `json:"transferTo" ch:"transfer_to"`
	Value        string                     `json:"value" ch:"value"`
	Direction    types.TransactionDirection `json:"direction" ch:"direction"`

	// Token info (empty for native)
	TokenAddress  string `json:"tokenAddress" ch:"token_address"`
	TokenSymbol   string `json:"tokenSymbol" ch:"token_symbol"`
	TokenDecimals uint8  `json:"tokenDecimals" ch:"token_decimals"`
	TokenID       string `json:"tokenId" ch:"token_id"` // For NFTs

	// Tx metadata
	BlockNumber uint64    `json:"blockNumber" ch:"block_number"`
	Timestamp   time.Time `json:"timestamp" ch:"timestamp"`
	Status      string    `json:"status" ch:"status"`
	GasUsed     string    `json:"gasUsed" ch:"gas_used"`
	GasPrice    string    `json:"gasPrice" ch:"gas_price"`
	L1Fee       string    `json:"l1Fee" ch:"l1_fee"` // L1 data fee for L2 chains
	MethodID    string    `json:"methodId" ch:"method_id"`
	FuncName    string    `json:"funcName" ch:"func_name"`

	// Spam detection
	IsSpam uint8 `json:"isSpam" ch:"is_spam"` // 1 = spam, 0 = not spam
}

// FromNormalizedTransaction creates Transaction records from NormalizedTransaction
// Returns multiple records if there are token transfers
//
// For swap transactions (user swaps token for ETH via DEX):
// - Normal tx (has gas): creates native OUT (value=0, has gas) for gas payment
// - Internal tx (no gas): creates native IN (has value, no gas) for ETH received
// - Token transfer: creates ERC20 OUT (has value, no gas) for token sent
func FromNormalizedTransaction(nt *types.NormalizedTransaction, address string) []*Transaction {
	address = strings.ToLower(address)
	var records []*Transaction

	// Check if this is a token transfer (from tokentx/tokennfttx/token1155tx API)
	// Token transfers should NOT create native records - they only create token records
	// The native OUT (for gas) comes from the txlist API separately
	isTokenTransfer := nt.Category != nil && (*nt.Category == "erc20" || *nt.Category == "erc721" || *nt.Category == "erc1155")

	// Check if this is a normal transaction (has gas) or internal transaction (no gas)
	// Internal transactions from Etherscan don't have gas - gas is paid by the outer transaction
	hasGas := nt.GasUsed != nil && *nt.GasUsed != "" && nt.GasPrice != nil && *nt.GasPrice != ""
	isUserInitiated := strings.EqualFold(nt.From, address)

	// Base transaction info (without gas - gas only goes on native OUT record)
	baseTx := Transaction{
		TxHash:      nt.Hash,
		Chain:       nt.Chain,
		Address:     address,
		TxFrom:      strings.ToLower(nt.From),
		TxTo:        strings.ToLower(nt.To),
		BlockNumber: nt.BlockNumber,
		Timestamp:   time.Unix(nt.Timestamp, 0),
		Status:      string(nt.Status),
		MethodID:    stringVal(nt.MethodID),
		FuncName:    stringVal(nt.FuncName),
	}

	hasNativeValue := nt.Value != "" && nt.Value != "0"
	hasTokenTransfers := len(nt.TokenTransfers) > 0

	// Only create native records for non-token-transfer transactions
	// Token transfers (from tokentx API) should NOT create native OUT records
	// because the "from" in token transfers is the token sender, not the tx initiator
	if !isTokenTransfer {
		// For normal transactions where user is the initiator and has gas:
		// Always create a native OUT record to capture gas payment (even if value=0)
		// This handles swap transactions where user pays gas but sends 0 ETH
		if hasGas && isUserInitiated {
			nativeTx := baseTx
			nativeTx.LogIndex = 0
			nativeTx.TransferType = types.TransferTypeNative
			nativeTx.TransferFrom = strings.ToLower(nt.From)
			nativeTx.TransferTo = strings.ToLower(nt.To)
			nativeTx.Value = nt.Value // Could be 0 for swap transactions
			nativeTx.TokenDecimals = 18
			nativeTx.Direction = types.DirectionOut
			// Gas only on native OUT record where user initiated
			nativeTx.GasUsed = *nt.GasUsed
			nativeTx.GasPrice = *nt.GasPrice
			// L1 fee for L2 chains
			if nt.L1Fee != nil {
				nativeTx.L1Fee = *nt.L1Fee
			}
			records = append(records, &nativeTx)
		} else if hasNativeValue || !hasTokenTransfers {
			// For internal transactions (no gas) or receiving transactions:
			// Create native record only if there's value or no token transfers
			nativeTx := baseTx
			// Use high log_index for internal transactions to avoid collision with normal tx
			// Internal transactions use 1000000 + traceIndex to differentiate from normal tx (log_index=0)
			if nt.IsInternal {
				nativeTx.LogIndex = 1000000 + nt.TraceIndex
			} else {
				nativeTx.LogIndex = 0
			}
			nativeTx.TransferType = types.TransferTypeNative
			nativeTx.TransferFrom = strings.ToLower(nt.From)
			nativeTx.TransferTo = strings.ToLower(nt.To)
			nativeTx.Value = nt.Value
			nativeTx.TokenDecimals = 18
			// No gas for internal transactions or receiving transactions
			nativeTx.GasUsed = ""
			nativeTx.GasPrice = ""

			// Direction based on tracked address
			if strings.EqualFold(nt.From, address) {
				nativeTx.Direction = types.DirectionOut
			} else {
				nativeTx.Direction = types.DirectionIn
			}

			records = append(records, &nativeTx)
		}
	}

	// Create token transfer records (never have gas - gas is on native OUT only)
	// Each token transfer from Etherscan comes as a separate NormalizedTransaction,
	// so we need unique log_index to avoid collisions in ClickHouse.
	// Strategy: use token contract address hash to generate unique offset
	for i, tt := range nt.TokenTransfers {
		tokenTx := baseTx
		tokenTx.TransferFrom = strings.ToLower(tt.From)
		tokenTx.TransferTo = strings.ToLower(tt.To)
		tokenTx.Value = tt.Value
		tokenTx.TokenAddress = strings.ToLower(tt.Token)
		// No gas on token transfers
		tokenTx.GasUsed = ""
		tokenTx.GasPrice = ""

		// Token metadata
		if tt.Symbol != nil {
			tokenTx.TokenSymbol = *tt.Symbol
		}
		if tt.Decimals != nil {
			tokenTx.TokenDecimals = uint8(*tt.Decimals)
		} else {
			tokenTx.TokenDecimals = 18
		}
		// Token ID for NFTs
		if tt.TokenID != nil {
			tokenTx.TokenID = *tt.TokenID
		}

		// Determine transfer type from category or default to erc20
		tokenTx.TransferType = types.TransferTypeERC20
		if nt.Category != nil {
			switch *nt.Category {
			case "erc721":
				tokenTx.TransferType = types.TransferTypeERC721
			case "erc1155":
				tokenTx.TransferType = types.TransferTypeERC1155
			}
		}

		// Direction based on tracked address
		if strings.EqualFold(tt.From, address) {
			tokenTx.Direction = types.DirectionOut
		} else if strings.EqualFold(tt.To, address) {
			tokenTx.Direction = types.DirectionIn
		} else {
			// Skip if tracked address is not involved in this transfer
			continue
		}

		// Generate log_index:
		// 1. If LogIndex is provided (from Moralis), use it directly
		// 2. Otherwise, generate unique log_index using token address hash (for Etherscan)
		if tt.LogIndex != nil {
			tokenTx.LogIndex = uint32(*tt.LogIndex)
		} else {
			// Generate unique log_index using token address hash + direction offset
			// This ensures different tokens in same tx don't collide
			tokenAddrHash := uint32(0)
			for _, b := range []byte(strings.ToLower(tt.Token)) {
				tokenAddrHash = tokenAddrHash*31 + uint32(b)
			}
			// Use lower 16 bits of hash (0-65535) plus base offset
			tokenOffset := (tokenAddrHash & 0xFFFF) + uint32(i+1)

			if tokenTx.Direction == types.DirectionOut {
				// OUT transfers: 1 - 99999
				tokenTx.LogIndex = 1 + (tokenOffset % 99999)
			} else {
				// IN transfers: 100000 - 199999 (below internal tx range of 1000000+)
				tokenTx.LogIndex = 100000 + (tokenOffset % 99999)
			}
		}

		records = append(records, &tokenTx)
	}

	return records
}

func stringVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
