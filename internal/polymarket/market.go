package polymarket

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/nonya123456/poly/internal/util"
)

const WebSocketMarketURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

type EventType string

const (
	EventTypeBook            EventType = "book"
	EventTypePriceChange     EventType = "price_change"
	EventTypeTickSizeChange  EventType = "tick_size_change"
	EventTypeLastTradePrice  EventType = "last_trade_price"
)

// BaseEvent contains common fields for all events
type BaseEvent struct {
	EventType EventType `json:"event_type"`
	Timestamp string    `json:"timestamp"`
	Market    string    `json:"market"`
	AssetID   string    `json:"asset_id"`
}

// BookLevel represents a price/size pair in the orderbook
type BookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// BookEvent is sent when subscribing or when trades affect the orderbook
type BookEvent struct {
	BaseEvent
	Bids []BookLevel `json:"bids"`
	Asks []BookLevel `json:"asks"`
}

func (e BookEvent) CSVHeader() []string {
	return []string{"timestamp", "market", "asset_id", "side", "price", "size"}
}

func (e BookEvent) CSVRow() []string {
	return nil // Not used directly; we expand to multiple rows
}

// BookLevelRecord is used for CSV output of book events
type BookLevelRecord struct {
	Timestamp string
	Market    string
	AssetID   string
	Side      string
	Price     string
	Size      string
}

func (r BookLevelRecord) CSVHeader() []string {
	return []string{"timestamp", "market", "asset_id", "side", "price", "size"}
}

func (r BookLevelRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.Market,
		r.AssetID,
		r.Side,
		r.Price,
		r.Size,
	}
}

// PriceChangeItem represents a single price change
type PriceChangeItem struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	Hash    string `json:"hash"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

// PriceChangeEvent is triggered when orders are placed or canceled
type PriceChangeEvent struct {
	BaseEvent
	Changes []PriceChangeItem `json:"price_changes"`
}

// PriceChangeRecord is used for CSV output
type PriceChangeRecord struct {
	Timestamp string
	Market    string
	AssetID   string
	Price     string
	Size      string
	Side      string
	Hash      string
	BestBid   string
	BestAsk   string
}

func (r PriceChangeRecord) CSVHeader() []string {
	return []string{"timestamp", "market", "asset_id", "price", "size", "side", "hash", "best_bid", "best_ask"}
}

func (r PriceChangeRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.Market,
		r.AssetID,
		r.Price,
		r.Size,
		r.Side,
		r.Hash,
		r.BestBid,
		r.BestAsk,
	}
}

// TickSizeChangeEvent is emitted when tick size adjusts
type TickSizeChangeEvent struct {
	BaseEvent
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
}

func (e TickSizeChangeEvent) CSVHeader() []string {
	return []string{"timestamp", "market", "asset_id", "old_tick_size", "new_tick_size"}
}

func (e TickSizeChangeEvent) CSVRow() []string {
	return []string{
		e.Timestamp,
		e.Market,
		e.AssetID,
		e.OldTickSize,
		e.NewTickSize,
	}
}

// LastTradePriceEvent is emitted when orders match
type LastTradePriceEvent struct {
	BaseEvent
	Price      string `json:"price"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	FeeRateBps string `json:"fee_rate_bps"`
}

func (e LastTradePriceEvent) CSVHeader() []string {
	return []string{"timestamp", "market", "asset_id", "price", "size", "side", "fee_rate_bps"}
}

func (e LastTradePriceEvent) CSVRow() []string {
	return []string{
		e.Timestamp,
		e.Market,
		e.AssetID,
		e.Price,
		e.Size,
		e.Side,
		e.FeeRateBps,
	}
}

// MarketSubscriber subscribes to the market websocket channel and writes events to CSV
type MarketSubscriber struct {
	conn      *websocket.Conn
	outputDir string
	done      chan struct{}
	mu        sync.Mutex
}

// NewMarketSubscriber creates a new market subscriber
func NewMarketSubscriber(outputDir string) *MarketSubscriber {
	return &MarketSubscriber{
		outputDir: outputDir,
		done:      make(chan struct{}),
	}
}

// subscribeMessage is the message sent to subscribe to market updates
type subscribeMessage struct {
	Type    string   `json:"type"`
	Channel string   `json:"channel"`
	Assets  []string `json:"assets_ids"`
}

// Subscribe connects to the websocket and subscribes to market updates for the given asset IDs
func (s *MarketSubscriber) Subscribe(assetIDs []string) error {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketMarketURL, nil)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}
	s.conn = conn

	msg := subscribeMessage{
		Type:    "subscribe",
		Channel: "market",
		Assets:  assetIDs,
	}

	if err := conn.WriteJSON(msg); err != nil {
		return fmt.Errorf("send subscribe message: %w", err)
	}

	go s.readLoop()

	return nil
}

func (s *MarketSubscriber) readLoop() {
	defer close(s.done)

	for {
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				return
			}
			log.Printf("read error: %v", err)
			return
		}

		if err := s.handleMessage(message); err != nil {
			log.Printf("handle message error: %v", err)
		}
	}
}

func (s *MarketSubscriber) handleMessage(data []byte) error {
	// Check if it's an array (batch of events)
	if len(data) > 0 && data[0] == '[' {
		var events []json.RawMessage
		if err := json.Unmarshal(data, &events); err != nil {
			return fmt.Errorf("unmarshal event array: %w", err)
		}
		for _, eventData := range events {
			if err := s.handleSingleEvent(eventData); err != nil {
				log.Printf("handle event in array: %v", err)
			}
		}
		return nil
	}
	return s.handleSingleEvent(data)
}

func (s *MarketSubscriber) handleSingleEvent(data []byte) error {
	// First, determine the event type
	var base BaseEvent
	if err := json.Unmarshal(data, &base); err != nil {
		return fmt.Errorf("unmarshal base event: %w", err)
	}

	switch base.EventType {
	case EventTypeBook:
		return s.handleBookEvent(data)
	case EventTypePriceChange:
		return s.handlePriceChangeEvent(data)
	case EventTypeTickSizeChange:
		return s.handleTickSizeChangeEvent(data)
	case EventTypeLastTradePrice:
		return s.handleLastTradePriceEvent(data)
	default:
		// Unknown event type, log and skip
		log.Printf("unknown event type: %s", base.EventType)
		return nil
	}
}

func (s *MarketSubscriber) csvPath(marketID string, eventType EventType) string {
	sanitized := SanitizeMarketID(marketID)
	filename := fmt.Sprintf("%s_%s.csv", sanitized, eventType)
	return filepath.Join(s.outputDir, filename)
}

func (s *MarketSubscriber) handleBookEvent(data []byte) error {
	var event BookEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal book event: %w", err)
	}

	var records []BookLevelRecord

	for _, bid := range event.Bids {
		records = append(records, BookLevelRecord{
			Timestamp: event.Timestamp,
			Market:    event.Market,
			AssetID:   event.AssetID,
			Side:      "BID",
			Price:     bid.Price,
			Size:      bid.Size,
		})
	}

	for _, ask := range event.Asks {
		records = append(records, BookLevelRecord{
			Timestamp: event.Timestamp,
			Market:    event.Market,
			AssetID:   event.AssetID,
			Side:      "ASK",
			Price:     ask.Price,
			Size:      ask.Size,
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return util.AppendCSV(s.csvPath(event.Market, EventTypeBook), records)
}

func (s *MarketSubscriber) handlePriceChangeEvent(data []byte) error {
	var event PriceChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal price_change event: %w", err)
	}

	var records []PriceChangeRecord
	for _, change := range event.Changes {
		records = append(records, PriceChangeRecord{
			Timestamp: event.Timestamp,
			Market:    event.Market,
			AssetID:   change.AssetID,
			Price:     change.Price,
			Size:      change.Size,
			Side:      change.Side,
			Hash:      change.Hash,
			BestBid:   change.BestBid,
			BestAsk:   change.BestAsk,
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return util.AppendCSV(s.csvPath(event.Market, EventTypePriceChange), records)
}

func (s *MarketSubscriber) handleTickSizeChangeEvent(data []byte) error {
	var event TickSizeChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal tick_size_change event: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return util.AppendCSV(s.csvPath(event.Market, EventTypeTickSizeChange), []TickSizeChangeEvent{event})
}

func (s *MarketSubscriber) handleLastTradePriceEvent(data []byte) error {
	var event LastTradePriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal last_trade_price event: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return util.AppendCSV(s.csvPath(event.Market, EventTypeLastTradePrice), []LastTradePriceEvent{event})
}

// Close closes the websocket connection
func (s *MarketSubscriber) Close() error {
	if s.conn != nil {
		err := s.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			return fmt.Errorf("send close message: %w", err)
		}
		return s.conn.Close()
	}
	return nil
}

// Done returns a channel that is closed when the subscriber stops
func (s *MarketSubscriber) Done() <-chan struct{} {
	return s.done
}

// SanitizeMarketID removes characters that are invalid for filenames
func SanitizeMarketID(marketID string) string {
	// Replace characters that are problematic in filenames
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	return replacer.Replace(marketID)
}
