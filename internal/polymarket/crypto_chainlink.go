package polymarket

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nonya123456/poly/internal/util"
)

type ChainlinkPriceEvent struct {
	Topic     string               `json:"topic"`
	Type      string               `json:"type"`
	Timestamp int64                `json:"timestamp"`
	Payload   ChainlinkPriceData   `json:"payload"`
}

type ChainlinkPriceData struct {
	Symbol    string  `json:"symbol"` // e.g., "eth/usd"
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type ChainlinkSubscriber struct {
	conn       *websocket.Conn
	outputDir  string
	marketSlug string
	symbols    map[string]struct{} // stores symbols like "btc/usd"
	done       chan struct{}
	mu         sync.Mutex
	pingStop   chan struct{}
	closeCh    chan struct{}
	connMu     sync.Mutex
}

func NewChainlinkSubscriber(outputDir string) (*ChainlinkSubscriber, error) {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketRTDSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	s := &ChainlinkSubscriber{
		conn:      conn,
		outputDir: outputDir,
		symbols:   make(map[string]struct{}),
		done:      make(chan struct{}),
		pingStop:  make(chan struct{}),
		closeCh:   make(chan struct{}),
	}

	go s.pingLoop()
	go s.readLoop()

	return s, nil
}

func (s *ChainlinkSubscriber) reconnect() error {
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

		log.Printf("chainlink: reconnect attempt %d/%d (backoff: %v)", attempt, maxRetries, backoff)

		conn, _, err := websocket.DefaultDialer.Dial(WebSocketRTDSURL, nil)
		if err != nil {
			log.Printf("chainlink: reconnect failed: %v", err)
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
			log.Printf("chainlink: resubscribe failed: %v", err)
			conn.Close()
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		log.Printf("chainlink: reconnected successfully")
		return nil
	}
	return fmt.Errorf("chainlink: max reconnect attempts (%d) exceeded", maxRetries)
}

func (s *ChainlinkSubscriber) resubscribe() error {
	s.mu.Lock()
	symbols := make([]string, 0, len(s.symbols))
	for sym := range s.symbols {
		symbols = append(symbols, sym)
	}
	s.mu.Unlock()

	if len(symbols) == 0 {
		return nil
	}

	msg := rtdsMessage{
		Action: "subscribe",
		Subscriptions: []rtdsSubscription{
			{
				Topic: "crypto_prices_chainlink",
				Type:  "*",
			},
		},
	}
	return s.conn.WriteJSON(msg)
}

func (s *ChainlinkSubscriber) pingLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.connMu.Lock()
			conn := s.conn
			s.connMu.Unlock()
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("chainlink ping error: %v", err)
			}
		case <-s.pingStop:
			return
		}
	}
}

// Subscribe subscribes to Chainlink price feeds.
// Symbols should be in slash format: "btc/usd", "eth/usd", "sol/usd", "xrp/usd"
func (s *ChainlinkSubscriber) Subscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		s.symbols[sym] = struct{}{}
	}
	s.mu.Unlock()

	msg := rtdsMessage{
		Action: "subscribe",
		Subscriptions: []rtdsSubscription{
			{
				Topic: "crypto_prices_chainlink",
				Type:  "*",
			},
		},
	}
	return s.conn.WriteJSON(msg)
}

func (s *ChainlinkSubscriber) Unsubscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		delete(s.symbols, sym)
	}
	s.mu.Unlock()

	msg := rtdsMessage{
		Action: "unsubscribe",
		Subscriptions: []rtdsSubscription{
			{
				Topic:   "crypto_prices_chainlink",
				Type:    "*",
				Filters: strings.Join(symbols, ","),
			},
		},
	}
	return s.conn.WriteJSON(msg)
}

func (s *ChainlinkSubscriber) SetMarketSlug(slug string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marketSlug = slug
}

func (s *ChainlinkSubscriber) readLoop() {
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

			log.Printf("chainlink: read error: %v", err)

			if err := s.reconnect(); err != nil {
				log.Printf("chainlink: reconnect failed permanently: %v", err)
				return
			}
			continue
		}

		if err := s.handleMessage(message); err != nil {
			log.Printf("chainlink handle message error: %v", err)
		}
	}
}

func (s *ChainlinkSubscriber) handleMessage(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if data[0] != '{' && data[0] != '[' {
		return nil
	}

	var event ChainlinkPriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil
	}

	if event.Topic != "crypto_prices_chainlink" || event.Type != "update" {
		return nil
	}

	return s.handlePriceEvent(event)
}

func (s *ChainlinkSubscriber) handlePriceEvent(event ChainlinkPriceEvent) error {
	symbol := event.Payload.Symbol // e.g., "btc/usd"
	timestamp := event.Payload.Timestamp

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.symbols[symbol]; !ok {
		return nil
	}

	if s.marketSlug == "" {
		return nil
	}

	// Convert symbol for filename: "btc/usd" -> "btc_usd"
	safeSymbol := strings.ReplaceAll(symbol, "/", "_")

	record := CryptoPriceRecord{
		Timestamp: fmt.Sprintf("%d", timestamp),
		Value:     strconv.FormatFloat(event.Payload.Value, 'f', -1, 64),
	}
	tickPath := filepath.Join(s.outputDir, fmt.Sprintf("%s_chainlink_%s.csv", s.marketSlug, safeSymbol))
	return util.AppendCSV(tickPath, []CryptoPriceRecord{record})
}

func (s *ChainlinkSubscriber) Close() error {
	close(s.closeCh)
	close(s.pingStop)

	s.connMu.Lock()
	conn := s.conn
	s.connMu.Unlock()

	if conn != nil {
		err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			return fmt.Errorf("send close message: %w", err)
		}
		return conn.Close()
	}
	return nil
}

func (s *ChainlinkSubscriber) Done() <-chan struct{} {
	return s.done
}
