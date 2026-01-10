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

	fmt.Printf("Market: %s\n", market.Question)
	fmt.Printf("Outcomes: %v\n", market.Outcomes)

	outputDir := "out"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("failed to create output directory: %v", err)
	}

	subscriber := polymarket.NewMarketSubscriber(outputDir)

	fmt.Printf("Subscribing to market channel...\n")
	if err := subscriber.Subscribe(market); err != nil {
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
