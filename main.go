package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nonya123456/poly/internal/polymarket"
)

func main() {
	gamma := polymarket.NewGammaService(polymarket.GammaBaseURL)

	slug := polymarket.GetCurrentBTCMarketSlug()
	fmt.Printf("Fetching market: %s\n", slug)

	market, err := gamma.GetMarketBySlug(slug)
	if err != nil {
		log.Fatalf("failed to get market: %v", err)
	}

	fmt.Printf("Market: %s\n", market.Question)
	fmt.Printf("Outcomes: %v\n", market.Outcomes)

	outputDir := "out"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	subscriber, err := polymarket.NewMarketSubscriber(outputDir)
	if err != nil {
		log.Fatalf("failed to create subscriber: %v", err)
	}

	fmt.Printf("Subscribing to market channel...\n")
	tokens := getTokenMetadataByOutcome(market, "up")
	if err := subscriber.Subscribe(tokens); err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	fmt.Println("Listening for market events... Press Ctrl+C to stop.")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		fmt.Println("\nShutting down...")
	case <-subscriber.Done():
		fmt.Println("\nWebsocket connection closed.")
	}

	if err := subscriber.Close(); err != nil {
		log.Printf("error closing subscriber: %v", err)
	}
}

func getTokenMetadataByOutcome(market polymarket.Market, want string) []polymarket.TokenMetadata {
	tokens := make([]polymarket.TokenMetadata, 0)
	for i, outcome := range market.Outcomes {
		if i >= len(market.ClobTokenIDs) {
			break
		}
		if strings.Compare(strings.ToLower(outcome), strings.ToLower(want)) != 0 {
			continue
		}
		tokens = append(tokens, polymarket.TokenMetadata{
			ID:          market.ClobTokenIDs[i],
			MarketSlug:  market.Slug,
			OutcomeName: outcome,
		})
	}
	return tokens
}
