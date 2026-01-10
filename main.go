package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nonya123456/poly/internal/polymarket"
)

func main() {
	gamma := polymarket.NewGammaService(polymarket.GammaBaseURL)

	outputDir := "out"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	subscriber, err := polymarket.NewMarketSubscriber(outputDir)
	if err != nil {
		log.Fatalf("failed to create subscriber: %v", err)
	}

	currentSlug := polymarket.GetCurrentBTCMarketSlug()
	fmt.Printf("Fetching market: %s\n", currentSlug)

	market, err := gamma.GetMarketBySlug(currentSlug)
	if err != nil {
		log.Fatalf("failed to get market: %v", err)
	}

	fmt.Printf("Market: %s\n", market.Question)
	fmt.Printf("Outcomes: %v\n", market.Outcomes)

	currentTokens := getTokenMetadataByOutcome(market, "up")
	fmt.Printf("Subscribing to market channel...\n")
	if err := subscriber.Subscribe(currentTokens); err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	fmt.Println("Listening for market events... Press Ctrl+C to stop.")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			fmt.Println("\nShutting down...")
			if err := subscriber.Close(); err != nil {
				log.Printf("error closing subscriber: %v", err)
			}
			return
		case <-subscriber.Done():
			fmt.Println("\nWebsocket connection closed.")
			return
		case <-ticker.C:
			newSlug := polymarket.GetCurrentBTCMarketSlug()
			if newSlug == currentSlug {
				continue
			}

			fmt.Printf("\nMarket changed: %s -> %s\n", currentSlug, newSlug)

			newMarket, err := gamma.GetMarketBySlug(newSlug)
			if err != nil {
				log.Printf("failed to get new market: %v", err)
				continue
			}

			fmt.Printf("Unsubscribing from old market...\n")
			if err := subscriber.Unsubscribe(currentTokens); err != nil {
				log.Printf("failed to unsubscribe: %v", err)
			}

			newTokens := getTokenMetadataByOutcome(newMarket, "up")
			fmt.Printf("Subscribing to new market: %s\n", newMarket.Question)
			if err := subscriber.Subscribe(newTokens); err != nil {
				log.Printf("failed to subscribe to new market: %v", err)
				continue
			}

			currentSlug = newSlug
			currentTokens = newTokens
			fmt.Println("Listening for market events...")
		}
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
