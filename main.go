package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
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

	fmt.Printf("Market ID: %s\n", market.ID)
	fmt.Printf("Token IDs: %v\n", market.ClobTokenIDs)

	// Create output directory if it doesn't exist
	outputDir := "data"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	// Subscribe to market websocket
	subscriber := polymarket.NewMarketSubscriber(outputDir)

	fmt.Printf("Subscribing to market channel for assets: %v\n", market.ClobTokenIDs)
	if err := subscriber.Subscribe(market.ClobTokenIDs); err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}

	fmt.Println("Listening for market events... Press Ctrl+C to stop.")

	// Wait for interrupt signal
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
