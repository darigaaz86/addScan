package service

import (
	"context"
	"strings"

	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// DeFi event signatures (first 10 chars of keccak256 hash)
const (
	// ERC20 Transfer(address,address,uint256)
	EventTransfer = "0xddf252ad"

	// Aave V2/V3 events
	EventAaveDeposit  = "0xde6857219" // Deposit(address,address,address,uint256,uint16)
	EventAaveWithdraw = "0x3115d1449" // Withdraw(address,address,address,uint256)
	EventAaveBorrow   = "0xc6a898c5"  // Borrow(address,address,address,uint256,uint256,uint256,uint16)
	EventAaveRepay    = "0x4cdde6e09" // Repay(address,address,address,uint256,bool)
	EventAaveSupply   = "0x2b627736"  // Supply(address,address,uint256,uint16)

	// Compound events
	EventCompoundMint   = "0x4c209b5f" // Mint(address,uint256,uint256)
	EventCompoundRedeem = "0xe5b754fb" // Redeem(address,uint256,uint256)
	EventCompoundBorrow = "0x13ed6866" // Borrow(address,uint256,uint256,uint256)
	EventCompoundRepay  = "0x1a2a22cb" // RepayBorrow(address,address,uint256,uint256,uint256)

	// Uniswap V2 events
	EventUniswapV2Swap = "0xd78ad95f" // Swap(address,uint256,uint256,uint256,uint256,address)
	EventUniswapV2Mint = "0x4c209b5f" // Mint(address,uint256,uint256)
	EventUniswapV2Burn = "0xdccd412f" // Burn(address,uint256,uint256,address)

	// Uniswap V3 events
	EventUniswapV3Swap = "0xc42079f9" // Swap(address,address,int256,int256,uint160,uint128,int24)
	EventUniswapV3Mint = "0x7a53080b" // Mint(address,address,int24,int24,uint128,uint256,uint256)
	EventUniswapV3Burn = "0x0c396cd9" // Burn(address,int24,int24,uint128,uint256,uint256)

	// Lido events
	EventLidoSubmitted = "0x96a25c8a" // Submitted(address,uint256,address)

	// Curve events
	EventCurveExchange  = "0x8b3e96f6" // TokenExchange(address,int128,uint256,int128,uint256)
	EventCurveAddLiq    = "0x26f55a85" // AddLiquidity(address,uint256[],uint256[],uint256,uint256)
	EventCurveRemoveLiq = "0x7c363854" // RemoveLiquidity(address,uint256[],uint256[],uint256)
)

// DeFiInteraction represents a detected DeFi protocol interaction
type DeFiInteraction struct {
	Address         string `json:"address"`
	Chain           string `json:"chain"`
	ProtocolID      string `json:"protocolId"`
	ProtocolName    string `json:"protocolName"`
	InteractionType string `json:"interactionType"` // supply, withdraw, borrow, repay, swap, stake, unstake
	TokenAddress    string `json:"tokenAddress"`
	Amount          string `json:"amount"`
	TxHash          string `json:"txHash"`
	BlockNumber     uint64 `json:"blockNumber"`
	Timestamp       int64  `json:"timestamp"`
}

// DeFiDetector detects DeFi protocol interactions from events
type DeFiDetector struct {
	goldskyRepo  *storage.GoldskyRepository
	protocolRepo *storage.ProtocolRepository
}

// NewDeFiDetector creates a new DeFi detector
func NewDeFiDetector(goldskyRepo *storage.GoldskyRepository, protocolRepo *storage.ProtocolRepository) *DeFiDetector {
	return &DeFiDetector{
		goldskyRepo:  goldskyRepo,
		protocolRepo: protocolRepo,
	}
}

// DetectInteractions detects DeFi interactions for an address
func (d *DeFiDetector) DetectInteractions(ctx context.Context, address string, chain types.ChainID, limit int) ([]DeFiInteraction, error) {
	address = strings.ToLower(address)
	interactions := make([]DeFiInteraction, 0)

	// Query Goldsky logs for DeFi events involving this address
	logs, err := d.goldskyRepo.GetLogsByAddress(ctx, address, string(chain), limit)
	if err != nil {
		return nil, err
	}

	for _, log := range logs {
		interaction := d.classifyEvent(ctx, &log, address)
		if interaction != nil {
			interactions = append(interactions, *interaction)
		}
	}

	return interactions, nil
}

// classifyEvent classifies a log event as a DeFi interaction
func (d *DeFiDetector) classifyEvent(ctx context.Context, log *storage.GoldskyLog, address string) *DeFiInteraction {
	sig := log.EventSignature
	if len(sig) < 10 {
		return nil
	}
	sigPrefix := sig[:10]

	var interaction *DeFiInteraction

	switch {
	// Aave events
	case strings.HasPrefix(sigPrefix, "0xde6857219") || strings.HasPrefix(sigPrefix, "0x2b627736"):
		interaction = d.createInteraction(log, address, "aave3", "Aave V3", "supply")
	case strings.HasPrefix(sigPrefix, "0x3115d1449"):
		interaction = d.createInteraction(log, address, "aave3", "Aave V3", "withdraw")
	case strings.HasPrefix(sigPrefix, "0xc6a898c5"):
		interaction = d.createInteraction(log, address, "aave3", "Aave V3", "borrow")
	case strings.HasPrefix(sigPrefix, "0x4cdde6e09"):
		interaction = d.createInteraction(log, address, "aave3", "Aave V3", "repay")

	// Compound events
	case strings.HasPrefix(sigPrefix, EventCompoundMint):
		interaction = d.createInteraction(log, address, "compound3", "Compound V3", "supply")
	case strings.HasPrefix(sigPrefix, EventCompoundRedeem):
		interaction = d.createInteraction(log, address, "compound3", "Compound V3", "withdraw")
	case strings.HasPrefix(sigPrefix, EventCompoundBorrow):
		interaction = d.createInteraction(log, address, "compound3", "Compound V3", "borrow")
	case strings.HasPrefix(sigPrefix, EventCompoundRepay):
		interaction = d.createInteraction(log, address, "compound3", "Compound V3", "repay")

	// Uniswap V2 events
	case strings.HasPrefix(sigPrefix, EventUniswapV2Swap):
		interaction = d.createInteraction(log, address, "uniswap2", "Uniswap V2", "swap")
	case strings.HasPrefix(sigPrefix, EventUniswapV2Burn):
		interaction = d.createInteraction(log, address, "uniswap2", "Uniswap V2", "remove_liquidity")

	// Uniswap V3 events
	case strings.HasPrefix(sigPrefix, EventUniswapV3Swap):
		interaction = d.createInteraction(log, address, "uniswap3", "Uniswap V3", "swap")
	case strings.HasPrefix(sigPrefix, EventUniswapV3Mint):
		interaction = d.createInteraction(log, address, "uniswap3", "Uniswap V3", "add_liquidity")
	case strings.HasPrefix(sigPrefix, EventUniswapV3Burn):
		interaction = d.createInteraction(log, address, "uniswap3", "Uniswap V3", "remove_liquidity")

	// Lido events
	case strings.HasPrefix(sigPrefix, EventLidoSubmitted):
		interaction = d.createInteraction(log, address, "lido", "Lido", "stake")

	// Curve events
	case strings.HasPrefix(sigPrefix, EventCurveExchange):
		interaction = d.createInteraction(log, address, "curve", "Curve", "swap")
	case strings.HasPrefix(sigPrefix, EventCurveAddLiq):
		interaction = d.createInteraction(log, address, "curve", "Curve", "add_liquidity")
	case strings.HasPrefix(sigPrefix, EventCurveRemoveLiq):
		interaction = d.createInteraction(log, address, "curve", "Curve", "remove_liquidity")
	}

	// Try to detect protocol from contract address if not matched by event
	if interaction == nil {
		protocol, contract, _ := d.protocolRepo.GetProtocolByContract(ctx, log.ContractAddress, log.Chain)
		if protocol != nil {
			interactionType := "interact"
			if contract != nil && contract.PositionType != nil {
				interactionType = *contract.PositionType
			}
			interaction = d.createInteraction(log, address, protocol.Slug, protocol.Name, interactionType)
		}
	}

	return interaction
}

// createInteraction creates a DeFi interaction from a log
func (d *DeFiDetector) createInteraction(log *storage.GoldskyLog, address, protocolID, protocolName, interactionType string) *DeFiInteraction {
	return &DeFiInteraction{
		Address:         address,
		Chain:           log.Chain,
		ProtocolID:      protocolID,
		ProtocolName:    protocolName,
		InteractionType: interactionType,
		TokenAddress:    log.ContractAddress,
		Amount:          log.Amount,
		TxHash:          log.TransactionHash,
		BlockNumber:     log.BlockNumber,
		Timestamp:       log.BlockTimestamp.Unix(),
	}
}

// GetProtocolActivity gets aggregated protocol activity for an address
func (d *DeFiDetector) GetProtocolActivity(ctx context.Context, address string, chain types.ChainID) (map[string]*ProtocolActivity, error) {
	interactions, err := d.DetectInteractions(ctx, address, chain, 1000)
	if err != nil {
		return nil, err
	}

	// Aggregate by protocol
	activity := make(map[string]*ProtocolActivity)
	for _, i := range interactions {
		if activity[i.ProtocolID] == nil {
			activity[i.ProtocolID] = &ProtocolActivity{
				ProtocolID:   i.ProtocolID,
				ProtocolName: i.ProtocolName,
				Chain:        i.Chain,
				Interactions: make(map[string]int),
			}
		}
		activity[i.ProtocolID].TotalInteractions++
		activity[i.ProtocolID].Interactions[i.InteractionType]++
		if i.Timestamp > activity[i.ProtocolID].LastInteraction {
			activity[i.ProtocolID].LastInteraction = i.Timestamp
		}
	}

	return activity, nil
}

// ProtocolActivity represents aggregated protocol activity
type ProtocolActivity struct {
	ProtocolID        string         `json:"protocolId"`
	ProtocolName      string         `json:"protocolName"`
	Chain             string         `json:"chain"`
	TotalInteractions int            `json:"totalInteractions"`
	Interactions      map[string]int `json:"interactions"` // type -> count
	LastInteraction   int64          `json:"lastInteraction"`
}
