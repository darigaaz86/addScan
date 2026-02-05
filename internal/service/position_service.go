package service

import (
	"context"
	"math/big"
	"strings"

	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// TokenBalance represents a token balance with protocol info (DeBank-aligned)
type TokenBalance struct {
	TokenAddress string `json:"tokenAddress"`
	Chain        string `json:"chain"`
	Symbol       string `json:"symbol"`
	Name         string `json:"name"`
	Decimals     int    `json:"decimals"`
	RawAmount    string `json:"rawAmount"`    // uint256 as string
	Amount       string `json:"amount"`       // human-readable (raw / 10^decimals)
	ProtocolID   string `json:"protocolId"`   // empty = wallet token
	PositionType string `json:"positionType"` // wallet, supplied, borrowed, staked, lp, reward
	LogoURL      string `json:"logoUrl"`
	IsNative     bool   `json:"isNative"`
	IsStablecoin bool   `json:"isStablecoin"`
}

// ProtocolPosition represents a position in a DeFi protocol (DeBank-aligned)
type ProtocolPosition struct {
	ProtocolID   string         `json:"protocolId"`
	ProtocolName string         `json:"protocolName"`
	Category     string         `json:"category"`
	Chain        string         `json:"chain"`
	LogoURL      string         `json:"logoUrl"`
	SiteURL      string         `json:"siteUrl"`
	Positions    []PositionItem `json:"positions"`
}

// PositionItem represents a single position within a protocol
type PositionItem struct {
	Name         string         `json:"name"`         // "Lending", "Staking", "LP", etc.
	PositionType string         `json:"positionType"` // lending, staking, lp, farming
	SupplyTokens []TokenBalance `json:"supplyTokens"` // Supplied/deposited tokens
	BorrowTokens []TokenBalance `json:"borrowTokens"` // Borrowed tokens (debt)
	RewardTokens []TokenBalance `json:"rewardTokens"` // Pending rewards
}

// AddressBalances represents all balances for an address
type AddressBalances struct {
	Address        string         `json:"address"`
	Chain          string         `json:"chain"`
	NativeBalance  *TokenBalance  `json:"nativeBalance"`
	WalletTokens   []TokenBalance `json:"walletTokens"`
	ProtocolTokens []TokenBalance `json:"protocolTokens"`
	TxCountIn      int64          `json:"txCountIn"`
	TxCountOut     int64          `json:"txCountOut"`
	FirstTxTime    int64          `json:"firstTxTime"`
	LastTxTime     int64          `json:"lastTxTime"`
}

// PortfolioBalances represents aggregated balances for multiple addresses
type PortfolioBalances struct {
	AddressCount int                       `json:"addressCount"`
	ChainCount   int                       `json:"chainCount"`
	Balances     []TokenBalance            `json:"balances"`
	ByChain      map[string][]TokenBalance `json:"byChain"`
	ByToken      map[string][]TokenBalance `json:"byToken"`
	Protocols    []ProtocolPosition        `json:"protocols"`
}

// PositionService handles balance and position calculations
type PositionService struct {
	balanceRepo  *storage.BalanceRepository
	protocolRepo *storage.ProtocolRepository
	tokenRepo    *storage.TokenRegistryRepository
}

// NewPositionService creates a new position service
func NewPositionService(
	balanceRepo *storage.BalanceRepository,
	protocolRepo *storage.ProtocolRepository,
	tokenRepo *storage.TokenRegistryRepository,
) *PositionService {
	return &PositionService{
		balanceRepo:  balanceRepo,
		protocolRepo: protocolRepo,
		tokenRepo:    tokenRepo,
	}
}

// GetAddressBalances gets all balances for an address on a chain
func (s *PositionService) GetAddressBalances(ctx context.Context, address string, chain types.ChainID) (*AddressBalances, error) {
	address = strings.ToLower(address)

	// Get raw balances from ClickHouse
	rawBalance, err := s.balanceRepo.GetAddressBalance(ctx, address, chain)
	if err != nil {
		return nil, err
	}

	// Get native token info
	nativeToken, _ := s.tokenRepo.GetNativeToken(ctx, string(chain))

	var nativeBalance *TokenBalance
	if nativeToken != nil {
		symbol := "ETH"
		if nativeToken.Symbol != nil {
			symbol = *nativeToken.Symbol
		}
		name := symbol
		if nativeToken.Name != nil {
			name = *nativeToken.Name
		}

		nativeBalance = &TokenBalance{
			TokenAddress: "0x0000000000000000000000000000000000000000",
			Chain:        string(chain),
			Symbol:       symbol,
			Name:         name,
			Decimals:     nativeToken.Decimals,
			RawAmount:    rawBalance.NativeBalance,
			Amount:       formatAmount(rawBalance.NativeBalance, nativeToken.Decimals),
			ProtocolID:   "",
			PositionType: "wallet",
			IsNative:     true,
		}
	} else {
		// Fallback for unknown chains
		nativeBalance = &TokenBalance{
			TokenAddress: "0x0000000000000000000000000000000000000000",
			Chain:        string(chain),
			Symbol:       "ETH",
			Name:         "Native Token",
			Decimals:     18,
			RawAmount:    rawBalance.NativeBalance,
			Amount:       formatAmount(rawBalance.NativeBalance, 18),
			ProtocolID:   "",
			PositionType: "wallet",
			IsNative:     true,
		}
	}

	// Process token balances
	walletTokens := make([]TokenBalance, 0)
	protocolTokens := make([]TokenBalance, 0)

	for _, tb := range rawBalance.TokenBalances {
		tokenBalance := s.enrichTokenBalance(ctx, tb, string(chain))

		if tokenBalance.ProtocolID == "" {
			walletTokens = append(walletTokens, tokenBalance)
		} else {
			protocolTokens = append(protocolTokens, tokenBalance)
		}
	}

	return &AddressBalances{
		Address:        address,
		Chain:          string(chain),
		NativeBalance:  nativeBalance,
		WalletTokens:   walletTokens,
		ProtocolTokens: protocolTokens,
		TxCountIn:      rawBalance.TxCountIn,
		TxCountOut:     rawBalance.TxCountOut,
		FirstTxTime:    rawBalance.FirstTxTime,
		LastTxTime:     rawBalance.LastTxTime,
	}, nil
}

// GetAddressAllChains gets balances for an address across all chains
func (s *PositionService) GetAddressAllChains(ctx context.Context, address string, chains []types.ChainID) (map[string]*AddressBalances, error) {
	result := make(map[string]*AddressBalances)

	for _, chain := range chains {
		balances, err := s.GetAddressBalances(ctx, address, chain)
		if err != nil {
			continue // Skip chains with errors
		}
		result[string(chain)] = balances
	}

	return result, nil
}

// GetProtocolPositions gets DeFi positions grouped by protocol
func (s *PositionService) GetProtocolPositions(ctx context.Context, address string, chain types.ChainID) ([]ProtocolPosition, error) {
	address = strings.ToLower(address)

	// Get raw balances
	rawBalance, err := s.balanceRepo.GetAddressBalance(ctx, address, chain)
	if err != nil {
		return nil, err
	}

	// Group tokens by protocol
	protocolMap := make(map[string]*ProtocolPosition)

	for _, tb := range rawBalance.TokenBalances {
		tokenBalance := s.enrichTokenBalance(ctx, tb, string(chain))

		if tokenBalance.ProtocolID == "" {
			continue // Skip wallet tokens
		}

		// Get or create protocol position
		pos, exists := protocolMap[tokenBalance.ProtocolID]
		if !exists {
			protocol, _ := s.protocolRepo.GetProtocol(ctx, tokenBalance.ProtocolID)
			if protocol == nil {
				continue
			}

			category := ""
			if protocol.Category != nil {
				category = *protocol.Category
			}
			siteURL := ""
			if protocol.SiteURL != nil {
				siteURL = *protocol.SiteURL
			}
			logoURL := ""
			if protocol.LogoURL != nil {
				logoURL = *protocol.LogoURL
			}

			pos = &ProtocolPosition{
				ProtocolID:   protocol.Slug,
				ProtocolName: protocol.Name,
				Category:     category,
				Chain:        string(chain),
				LogoURL:      logoURL,
				SiteURL:      siteURL,
				Positions:    []PositionItem{},
			}
			protocolMap[tokenBalance.ProtocolID] = pos
		}

		// Add token to appropriate position
		s.addTokenToPosition(pos, tokenBalance)
	}

	// Convert map to slice
	result := make([]ProtocolPosition, 0, len(protocolMap))
	for _, pos := range protocolMap {
		result = append(result, *pos)
	}

	return result, nil
}

// GetPortfolioBalances gets aggregated balances for multiple addresses
// Uses batch queries for performance
func (s *PositionService) GetPortfolioBalances(ctx context.Context, addresses []string, chains []types.ChainID) (*PortfolioBalances, error) {
	// Pre-fetch native tokens for all chains (cache them)
	nativeTokens := make(map[string]*storage.RegisteredToken)
	for _, chain := range chains {
		nativeToken, _ := s.tokenRepo.GetNativeToken(ctx, string(chain))
		nativeTokens[string(chain)] = nativeToken
	}

	// Use batch query for all addresses at once
	balanceSummary, err := s.balanceRepo.GetBalanceSummary(ctx, addresses)
	if err != nil {
		return nil, err
	}

	allBalances := make([]TokenBalance, 0)
	byChain := make(map[string][]TokenBalance)
	byToken := make(map[string][]TokenBalance)

	chainSet := make(map[string]bool)

	for _, address := range addresses {
		addressBalances, exists := balanceSummary[strings.ToLower(address)]
		if !exists {
			continue
		}

		for _, chain := range chains {
			rawBalance, exists := addressBalances[chain]
			if !exists {
				continue
			}

			chainSet[string(chain)] = true

			// Get native token info from cache
			nativeToken := nativeTokens[string(chain)]
			if nativeToken != nil && rawBalance.NativeBalance != "0" && rawBalance.NativeBalance != "" {
				symbol := "ETH"
				if nativeToken.Symbol != nil {
					symbol = *nativeToken.Symbol
				}
				name := symbol
				if nativeToken.Name != nil {
					name = *nativeToken.Name
				}

				nativeBalance := TokenBalance{
					TokenAddress: "0x0000000000000000000000000000000000000000",
					Chain:        string(chain),
					Symbol:       symbol,
					Name:         name,
					Decimals:     nativeToken.Decimals,
					RawAmount:    rawBalance.NativeBalance,
					Amount:       formatAmount(rawBalance.NativeBalance, nativeToken.Decimals),
					ProtocolID:   "",
					PositionType: "wallet",
					IsNative:     true,
				}

				allBalances = append(allBalances, nativeBalance)
				byChain[string(chain)] = append(byChain[string(chain)], nativeBalance)
				byToken[symbol] = append(byToken[symbol], nativeBalance)
			}

			// Process token balances (skip enrichment for now - just use raw data)
			for _, tb := range rawBalance.TokenBalances {
				tokenBalance := TokenBalance{
					TokenAddress: tb.TokenAddress,
					Chain:        string(chain),
					Symbol:       tb.Symbol,
					Name:         tb.Symbol,
					Decimals:     tb.Decimals,
					RawAmount:    tb.Balance,
					Amount:       formatAmount(tb.Balance, tb.Decimals),
					PositionType: "wallet",
				}

				allBalances = append(allBalances, tokenBalance)
				byChain[string(chain)] = append(byChain[string(chain)], tokenBalance)
				byToken[tokenBalance.Symbol] = append(byToken[tokenBalance.Symbol], tokenBalance)
			}
		}
	}

	return &PortfolioBalances{
		AddressCount: len(addresses),
		ChainCount:   len(chainSet),
		Balances:     allBalances,
		ByChain:      byChain,
		ByToken:      byToken,
		Protocols:    []ProtocolPosition{},
	}, nil
}

// enrichTokenBalance adds protocol and registry info to a token balance
func (s *PositionService) enrichTokenBalance(ctx context.Context, tb storage.TokenBalanceResult, chain string) TokenBalance {
	tokenBalance := TokenBalance{
		TokenAddress: tb.TokenAddress,
		Chain:        chain,
		Symbol:       tb.Symbol,
		Name:         tb.Symbol, // Default to symbol
		Decimals:     tb.Decimals,
		RawAmount:    tb.Balance,
		Amount:       formatAmount(tb.Balance, tb.Decimals),
		PositionType: "wallet",
	}

	// Try to get token info from registry
	registeredToken, _ := s.tokenRepo.GetToken(ctx, tb.TokenAddress, chain)
	if registeredToken != nil {
		if registeredToken.Name != nil {
			tokenBalance.Name = *registeredToken.Name
		}
		if registeredToken.LogoURL != nil {
			tokenBalance.LogoURL = *registeredToken.LogoURL
		}
		tokenBalance.IsStablecoin = registeredToken.IsStablecoin

		if registeredToken.ProtocolID != nil {
			protocol, _ := s.protocolRepo.GetProtocolByID(ctx, *registeredToken.ProtocolID)
			if protocol != nil {
				tokenBalance.ProtocolID = protocol.Slug
			}
		}
	}

	// Try to detect protocol from symbol if not found in registry
	if tokenBalance.ProtocolID == "" {
		protocol, positionType, _ := s.protocolRepo.DetectProtocolBySymbol(ctx, tb.Symbol)
		if protocol != nil {
			tokenBalance.ProtocolID = protocol.Slug
			tokenBalance.PositionType = positionType
		}
	}

	// Try to detect protocol from contract address
	if tokenBalance.ProtocolID == "" {
		protocol, contract, _ := s.protocolRepo.GetProtocolByContract(ctx, tb.TokenAddress, chain)
		if protocol != nil {
			tokenBalance.ProtocolID = protocol.Slug
			if contract != nil && contract.PositionType != nil {
				tokenBalance.PositionType = *contract.PositionType
			}
		}
	}

	return tokenBalance
}

// addTokenToPosition adds a token to the appropriate position in a protocol
func (s *PositionService) addTokenToPosition(pos *ProtocolPosition, token TokenBalance) {
	// Find or create position item based on position type
	positionName := getPositionName(token.PositionType)

	var posItem *PositionItem
	for i := range pos.Positions {
		if pos.Positions[i].Name == positionName {
			posItem = &pos.Positions[i]
			break
		}
	}

	if posItem == nil {
		pos.Positions = append(pos.Positions, PositionItem{
			Name:         positionName,
			PositionType: token.PositionType,
			SupplyTokens: []TokenBalance{},
			BorrowTokens: []TokenBalance{},
			RewardTokens: []TokenBalance{},
		})
		posItem = &pos.Positions[len(pos.Positions)-1]
	}

	// Add token to appropriate list
	switch token.PositionType {
	case "borrowed":
		posItem.BorrowTokens = append(posItem.BorrowTokens, token)
	case "reward":
		posItem.RewardTokens = append(posItem.RewardTokens, token)
	default:
		posItem.SupplyTokens = append(posItem.SupplyTokens, token)
	}
}

// getPositionName returns a human-readable name for a position type
func getPositionName(positionType string) string {
	switch positionType {
	case "supplied":
		return "Lending"
	case "borrowed":
		return "Borrowing"
	case "staked":
		return "Staking"
	case "lp":
		return "Liquidity"
	case "farming":
		return "Farming"
	case "reward":
		return "Rewards"
	case "locked":
		return "Locked"
	default:
		return "Position"
	}
}

// formatAmount converts raw amount to human-readable format
func formatAmount(rawAmount string, decimals int) string {
	if rawAmount == "" || rawAmount == "0" {
		return "0"
	}

	raw := new(big.Int)
	raw.SetString(rawAmount, 10)

	// Calculate divisor (10^decimals)
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)

	// Integer division
	intPart := new(big.Int).Div(raw, divisor)

	// Remainder for decimal part
	remainder := new(big.Int).Mod(raw, divisor)

	if remainder.Sign() == 0 {
		return intPart.String()
	}

	// Format decimal part with leading zeros
	decimalStr := remainder.String()
	for len(decimalStr) < decimals {
		decimalStr = "0" + decimalStr
	}

	// Trim trailing zeros
	decimalStr = strings.TrimRight(decimalStr, "0")

	if decimalStr == "" {
		return intPart.String()
	}

	return intPart.String() + "." + decimalStr
}
