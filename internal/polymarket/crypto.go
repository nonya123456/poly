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

const WebSocketRTDSURL = "wss://ws-live-data.polymarket.com"

type CryptoPriceEvent struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"timestamp"`
	Payload   CryptoPriceData `json:"payload"`
}

type CryptoPriceData struct {
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type CryptoPriceRecord struct {
	Timestamp string
	Value     string
}

func (r CryptoPriceRecord) CSVHeader() []string {
	return []string{"timestamp", "value"}
}

func (r CryptoPriceRecord) CSVRow() []string {
	return []string{
		r.Timestamp,
		r.Value,
	}
}

type rtdsSubscription struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Filters string `json:"filters,omitempty"`
}

type rtdsMessage struct {
	Action        string             `json:"action"`
	Subscriptions []rtdsSubscription `json:"subscriptions"`
}

type CryptoSubscriber struct {
	conn       *websocket.Conn
	outputDir  string
	marketSlug string
	symbols    map[string]struct{}
	done       chan struct{}
	mu         sync.Mutex
	pingStop   chan struct{}
}

func NewCryptoSubscriber(outputDir string) (*CryptoSubscriber, error) {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketRTDSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	s := &CryptoSubscriber{
		conn:      conn,
		outputDir: outputDir,
		symbols:   make(map[string]struct{}),
		done:      make(chan struct{}),
		pingStop:  make(chan struct{}),
	}

	go s.pingLoop()
	go s.readLoop()

	return s, nil
}

func (s *CryptoSubscriber) pingLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("ping error: %v", err)
				return
			}
		case <-s.pingStop:
			return
		}
	}
}

func (s *CryptoSubscriber) Subscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		s.symbols[sym] = struct{}{}
	}
	s.mu.Unlock()

	msg := rtdsMessage{
		Action: "subscribe",
		Subscriptions: []rtdsSubscription{
			{
				Topic: "crypto_prices",
				Type:  "update",
			},
		},
	}
	return s.conn.WriteJSON(msg)
}

func (s *CryptoSubscriber) Unsubscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		delete(s.symbols, sym)
	}
	s.mu.Unlock()

	msg := rtdsMessage{
		Action: "unsubscribe",
		Subscriptions: []rtdsSubscription{
			{
				Topic:   "crypto_prices",
				Type:    "update",
				Filters: strings.Join(symbols, ","),
			},
		},
	}
	return s.conn.WriteJSON(msg)
}

func (s *CryptoSubscriber) SetMarketSlug(slug string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marketSlug = slug
}

func (s *CryptoSubscriber) readLoop() {
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

func (s *CryptoSubscriber) handleMessage(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	if data[0] != '{' && data[0] != '[' {
		return nil
	}

	var event CryptoPriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil
	}

	if event.Topic != "crypto_prices" || event.Type != "update" {
		return nil
	}

	return s.handlePriceEvent(event)
}

func (s *CryptoSubscriber) handlePriceEvent(event CryptoPriceEvent) error {
	symbol := event.Payload.Symbol
	timestamp := event.Payload.Timestamp

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.symbols[symbol]; !ok {
		return nil
	}

	if s.marketSlug == "" {
		return nil
	}

	record := CryptoPriceRecord{
		Timestamp: fmt.Sprintf("%d", timestamp),
		Value:     strconv.FormatFloat(event.Payload.Value, 'f', -1, 64),
	}
	tickPath := filepath.Join(s.outputDir, fmt.Sprintf("%s_%s.csv", s.marketSlug, symbol))
	return util.AppendCSV(tickPath, []CryptoPriceRecord{record})
}

func (s *CryptoSubscriber) Close() error {
	close(s.pingStop)

	if s.conn != nil {
		err := s.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			return fmt.Errorf("send close message: %w", err)
		}
		return s.conn.Close()
	}
	return nil
}

func (s *CryptoSubscriber) Done() <-chan struct{} {
	return s.done
}
