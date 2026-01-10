package main

import (
	"fmt"
	"log"

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
}
