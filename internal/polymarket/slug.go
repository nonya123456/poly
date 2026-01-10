package polymarket

import (
	"fmt"
	"time"
)

func GetCurrentBTCMarketSlug() string {
	now := time.Now().UTC()

	minutes := now.Minute()
	currentIntervalStart := (minutes / 15) * 15

	startTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), currentIntervalStart, 0, 0, time.UTC)
	timestamp := startTime.Unix()

	return fmt.Sprintf("btc-updown-15m-%d", timestamp)
}
