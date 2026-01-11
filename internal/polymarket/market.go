package polymarket

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nonya123456/poly/internal/util"
)

const WebSocketMarketURL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

type EventType string

const (
	EventTypeBook           EventType = "book"
	EventTypePriceChange    EventType = "price_change"
	EventTypeTickSizeChange EventType = "tick_size_change"
	EventTypeLastTradePrice EventType = "last_trade_price"
)

type BaseEvent struct {
	EventType EventType `json:"event_type"`
	Timestamp string    `json:"timestamp"`
	Market    string    `json:"market"`
	AssetID   string    `json:"asset_id"`
}

type BookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type BookEvent struct {
	BaseEvent
	Bids []BookLevel `json:"bids"`
	Asks []BookLevel `json:"asks"`
}

type BookRecord struct {
	Timestamp string
	Bids      []BookLevel
	Asks      []BookLevel
}

func (r BookRecord) CSVHeader() []string {
	return []string{"timestamp", "bids", "asks"}
}

func (r BookRecord) CSVRow() []string {
	bb := strings.Builder{}
	for _, bid := range r.Bids {
		_, _ = bb.WriteString(bid.Price)
		_, _ = bb.WriteString(" ")
		_, _ = bb.WriteString(bid.Size)
		_, _ = bb.WriteString(" ")
	}

	ab := strings.Builder{}
	for _, ask := range r.Asks {
		_, _ = ab.WriteString(ask.Price)
		_, _ = ab.WriteString(" ")
		_, _ = ab.WriteString(ask.Size)
		_, _ = ab.WriteString(" ")
	}

	return []string{
		r.Timestamp,
		strings.Trim(bb.String(), " "),
		strings.Trim(ab.String(), " "),
	}
}

type PriceChangeItem struct {
	AssetID string `json:"asset_id"`
	Price   string `json:"price"`
	Size    string `json:"size"`
	Side    string `json:"side"`
	Hash    string `json:"hash"`
	BestBid string `json:"best_bid"`
	BestAsk string `json:"best_ask"`
}

type PriceChangeEvent struct {
	BaseEvent
	Changes []PriceChangeItem `json:"price_changes"`
}

type PriceChangeRecord struct {
	Timestamp string
	Price     string
	Size      string
	Side      string
	BestBid   string
	BestAsk   string
}

func (r PriceChangeRecord) CSVHeader() []string {
	return []string{"timestamp", "price", "size", "side", "best_bid", "best_ask"}
}

func (r PriceChangeRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.Price,
		r.Size,
		r.Side,
		r.BestBid,
		r.BestAsk,
	}
}

type TickSizeChangeEvent struct {
	BaseEvent
	OldTickSize string `json:"old_tick_size"`
	NewTickSize string `json:"new_tick_size"`
}

type TickSizeChangeRecord struct {
	Timestamp   string
	OldTickSize string
	NewTickSize string
}

func (r TickSizeChangeRecord) CSVHeader() []string {
	return []string{"timestamp", "old_tick_size", "new_tick_size"}
}

func (r TickSizeChangeRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.OldTickSize,
		r.NewTickSize,
	}
}

type LastTradePriceEvent struct {
	BaseEvent
	Price      string `json:"price"`
	Size       string `json:"size"`
	Side       string `json:"side"`
	FeeRateBps string `json:"fee_rate_bps"`
}

type LastTradePriceRecord struct {
	Timestamp  string
	Price      string
	Size       string
	Side       string
	FeeRateBps string
}

func (r LastTradePriceRecord) CSVHeader() []string {
	return []string{"timestamp", "price", "size", "side", "fee_rate_bps"}
}

func (r LastTradePriceRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.Price,
		r.Size,
		r.Side,
		r.FeeRateBps,
	}
}

type TokenMetadata struct {
	ID          string
	MarketSlug  string
	OutcomeName string
}

type MarketSubscriber struct {
	conn        *websocket.Conn
	outputDir   string
	tokens      map[string]TokenMetadata
	done        chan struct{}
	mu          sync.Mutex
	closeCh     chan struct{} // signals intentional close
	connMu      sync.Mutex    // protects conn during reconnect
	reconnectMu sync.Mutex    // prevents concurrent reconnects
}

func NewMarketSubscriber(outputDir string) (*MarketSubscriber, error) {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketMarketURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	s := &MarketSubscriber{
		conn:      conn,
		outputDir: outputDir,
		tokens:    make(map[string]TokenMetadata),
		done:      make(chan struct{}),
		closeCh:   make(chan struct{}),
	}

	go s.readLoop()

	return s, nil
}

func (s *MarketSubscriber) reconnect() error {
	const (
		maxRetries     = 10
		initialBackoff = 1 * time.Second
		maxBackoff     = 60 * time.Second
	)

	backoff := initialBackoff
	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-s.closeCh:
			return fmt.Errorf("subscriber closed during reconnect")
		default:
		}

		log.Printf("market: reconnect attempt %d/%d (backoff: %v)", attempt, maxRetries, backoff)

		conn, _, err := websocket.DefaultDialer.Dial(WebSocketMarketURL, nil)
		if err != nil {
			log.Printf("market: reconnect failed: %v", err)
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		s.connMu.Lock()
		s.conn = conn
		s.connMu.Unlock()

		if err := s.resubscribe(); err != nil {
			log.Printf("market: resubscribe failed: %v", err)
			_ = conn.Close()
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		log.Printf("market: reconnected successfully")
		return nil
	}
	return fmt.Errorf("market: max reconnect attempts (%d) exceeded", maxRetries)
}

func (s *MarketSubscriber) resubscribe() error {
	s.mu.Lock()
	tokens := make([]TokenMetadata, 0, len(s.tokens))
	for _, token := range s.tokens {
		tokens = append(tokens, token)
	}
	s.mu.Unlock()

	if len(tokens) == 0 {
		return nil
	}

	assets := make([]string, 0, len(tokens))
	for _, token := range tokens {
		assets = append(assets, token.ID)
	}

	msg := subscribeMessage{
		Type:      "MARKET",
		Operation: "subscribe",
		Assets:    assets,
	}
	return s.writeJSON(msg)
}

type subscribeMessage struct {
	Type      string   `json:"type"`
	Operation string   `json:"operation"`
	Assets    []string `json:"assets_ids"`
}

func (s *MarketSubscriber) writeJSON(msg interface{}) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn.WriteJSON(msg)
}

func (s *MarketSubscriber) Subscribe(tokens []TokenMetadata) error {
	s.mu.Lock()
	assets := make([]string, 0, len(tokens))
	for _, token := range tokens {
		s.tokens[token.ID] = token
		assets = append(assets, token.ID)
	}
	s.mu.Unlock()

	msg := subscribeMessage{
		Type:      "MARKET",
		Operation: "subscribe",
		Assets:    assets,
	}
	return s.writeJSON(msg)
}

func (s *MarketSubscriber) Unsubscribe(tokens []TokenMetadata) error {
	s.mu.Lock()
	assets := make([]string, 0, len(tokens))
	for _, token := range tokens {
		delete(s.tokens, token.ID)
		assets = append(assets, token.ID)
	}
	s.mu.Unlock()

	msg := subscribeMessage{
		Type:      "MARKET",
		Operation: "unsubscribe",
		Assets:    assets,
	}
	return s.writeJSON(msg)
}

func (s *MarketSubscriber) readLoop() {
	defer close(s.done)

	for {
		s.connMu.Lock()
		conn := s.conn
		s.connMu.Unlock()

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				return
			}

			select {
			case <-s.closeCh:
				return
			default:
			}

			log.Printf("market: read error: %v", err)

			if err := s.reconnect(); err != nil {
				log.Printf("market: reconnect failed permanently: %v", err)
				return
			}
			continue
		}

		if err := s.handleMessage(message); err != nil {
			log.Printf("handle message error: %v", err)
		}
	}
}

func (s *MarketSubscriber) handleMessage(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	// Skip non-JSON messages (text responses like acknowledgments)
	if data[0] != '[' && data[0] != '{' {
		return nil
	}
	if data[0] == '[' {
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
		log.Printf("unknown event type: %s", base.EventType)
		return nil
	}
}

func (s *MarketSubscriber) csvPath(assetID string, eventType EventType) (string, bool) {
	meta, ok := s.tokens[assetID]
	if !ok {
		return "", false
	}

	slug := strings.ToLower(meta.MarketSlug)
	outcomeName := strings.ToLower(meta.OutcomeName)
	filename := fmt.Sprintf("%s_%s_%s.csv", slug, outcomeName, eventType)
	return filepath.Join(s.outputDir, filename), true
}

func (s *MarketSubscriber) handleBookEvent(data []byte) error {
	var event BookEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal book event: %w", err)
	}

	record := BookRecord{
		Timestamp: event.Timestamp,
		Bids:      event.Bids,
		Asks:      event.Asks,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path, ok := s.csvPath(event.AssetID, EventTypeBook)
	if !ok {
		return nil // ignore
	}
	return util.AppendCSV(path, []BookRecord{record})
}

func (s *MarketSubscriber) handlePriceChangeEvent(data []byte) error {
	var event PriceChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal price_change event: %w", err)
	}

	recordsByAsset := make(map[string][]PriceChangeRecord)
	for _, change := range event.Changes {
		record := PriceChangeRecord{
			Timestamp: event.Timestamp,
			Price:     change.Price,
			Size:      change.Size,
			Side:      change.Side,
			BestBid:   change.BestBid,
			BestAsk:   change.BestAsk,
		}
		recordsByAsset[change.AssetID] = append(recordsByAsset[change.AssetID], record)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for assetID, records := range recordsByAsset {
		path, ok := s.csvPath(assetID, EventTypePriceChange)
		if !ok {
			continue // ignore
		}
		if err := util.AppendCSV(path, records); err != nil {
			return err
		}
	}
	return nil
}

func (s *MarketSubscriber) handleTickSizeChangeEvent(data []byte) error {
	var event TickSizeChangeEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal tick_size_change event: %w", err)
	}

	record := TickSizeChangeRecord{
		Timestamp:   event.Timestamp,
		OldTickSize: event.OldTickSize,
		NewTickSize: event.NewTickSize,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path, ok := s.csvPath(event.AssetID, EventTypeTickSizeChange)
	if !ok {
		return nil // ignore
	}
	return util.AppendCSV(path, []TickSizeChangeRecord{record})
}

func (s *MarketSubscriber) handleLastTradePriceEvent(data []byte) error {
	var event LastTradePriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("unmarshal last_trade_price event: %w", err)
	}

	record := LastTradePriceRecord{
		Timestamp:  event.Timestamp,
		Price:      event.Price,
		Size:       event.Size,
		Side:       event.Side,
		FeeRateBps: event.FeeRateBps,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path, ok := s.csvPath(event.AssetID, EventTypeLastTradePrice)
	if !ok {
		return nil // ignore
	}
	return util.AppendCSV(path, []LastTradePriceRecord{record})
}

func (s *MarketSubscriber) Close() error {
	close(s.closeCh)

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.conn != nil {
		_ = s.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		return s.conn.Close()
	}
	return nil
}

func (s *MarketSubscriber) Done() <-chan struct{} {
	return s.done
}
