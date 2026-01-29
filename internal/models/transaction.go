package models

import (
	"strings"
	"time"

	"github.com/address-scanner/internal/types"
)

// Transaction represents a blockchain transaction stored in ClickHouse
type Transaction struct {
	Hash           string                     `json:"hash" ch:"hash"`
	Chain          types.ChainID              `json:"chain" ch:"chain"`
	Address        string                     `json:"address" ch:"address"`
	From           string                     `json:"from" ch:"from"`
	To             string                     `json:"to" ch:"to"`
	Value          string                     `json:"value" ch:"value"`
	Asset          *string                    `json:"asset,omitempty" ch:"asset"`
	Category       *string                    `json:"category,omitempty" ch:"category"`
	Direction      types.TransactionDirection `json:"direction" ch:"direction"` // in, out
	Timestamp      time.Time                  `json:"timestamp" ch:"timestamp"`
	BlockNumber    uint64                     `json:"blockNumber" ch:"block_number"`
	Status         string                     `json:"status" ch:"status"` // success, failed
	GasUsed        *string                    `json:"gasUsed,omitempty" ch:"gas_used"`
	GasPrice       *string                    `json:"gasPrice,omitempty" ch:"gas_price"`
	TokenTransfers []types.TokenTransfer      `json:"tokenTransfers,omitempty" ch:"token_transfers"`
	MethodID       *string                    `json:"methodId,omitempty" ch:"method_id"`
	FuncName       *string                    `json:"funcName,omitempty" ch:"func_name"`
	Input          *string                    `json:"input,omitempty" ch:"input"`
}

// ToNormalizedTransaction converts a Transaction to NormalizedTransaction
func (t *Transaction) ToNormalizedTransaction() *types.NormalizedTransaction {
	return &types.NormalizedTransaction{
		Hash:           t.Hash,
		Chain:          t.Chain,
		From:           t.From,
		To:             t.To,
		Value:          t.Value,
		Direction:      t.Direction,
		Timestamp:      t.Timestamp.Unix(),
		BlockNumber:    t.BlockNumber,
		Status:         types.TransactionStatus(t.Status),
		GasUsed:        t.GasUsed,
		GasPrice:       t.GasPrice,
		TokenTransfers: t.TokenTransfers,
		MethodID:       t.MethodID,
		Input:          t.Input,
	}
}

// FromNormalizedTransaction creates a Transaction from NormalizedTransaction
// The direction is computed based on whether the address is the sender or recipient
func FromNormalizedTransaction(nt *types.NormalizedTransaction, address string) *Transaction {
	// Determine direction: if address is the sender, it's outgoing; otherwise incoming
	direction := types.DirectionIn
	if strings.EqualFold(nt.From, address) {
		direction = types.DirectionOut
	}

	return &Transaction{
		Hash:           nt.Hash,
		Chain:          nt.Chain,
		Address:        address,
		From:           nt.From,
		To:             nt.To,
		Value:          nt.Value,
		Asset:          nt.Asset,
		Category:       nt.Category,
		Direction:      direction,
		Timestamp:      time.Unix(nt.Timestamp, 0),
		BlockNumber:    nt.BlockNumber,
		Status:         string(nt.Status),
		GasUsed:        nt.GasUsed,
		GasPrice:       nt.GasPrice,
		TokenTransfers: nt.TokenTransfers,
		MethodID:       nt.MethodID,
		FuncName:       nt.FuncName,
		Input:          nt.Input,
	}
}
