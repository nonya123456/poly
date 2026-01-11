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

type rtdsSubscription struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Filters string `json:"filters,omitempty"`
}

type rtdsMessage struct {
	Action        string             `json:"action"`
	Subscriptions []rtdsSubscription `json:"subscriptions"`
}

type PriceEvent struct {
	Topic     string    `json:"topic"`
	Type      string    `json:"type"`
	Timestamp int64     `json:"timestamp"`
	Payload   PriceData `json:"payload"`
}

type PriceData struct {
	Symbol    string  `json:"symbol"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

type PriceRecord struct {
	Timestamp string
	Value     string
}

func (r PriceRecord) CSVHeader() []string {
	return []string{"timestamp", "value"}
}

func (r PriceRecord) CSVRow() []string {
	return []string{r.Timestamp, r.Value}
}

type PriceSubscriber struct {
	conn        *websocket.Conn
	outputDir   string
	marketSlug  string
	symbols     map[string]struct{}
	done        chan struct{}
	closeCh     chan struct{}
	closeOnce   sync.Once
	connMu      sync.Mutex
	mu          sync.Mutex
	reconnectMu sync.Mutex
	pongCh      chan struct{}
	reconnectCh chan struct{}
	name        string
	topic       string
	topicType   string
	filePrefix  string
}

type PriceSubscriberConfig struct {
	Name       string
	Topic      string
	TopicType  string
	FilePrefix string
}

func NewPriceSubscriber(outputDir string, cfg PriceSubscriberConfig) (*PriceSubscriber, error) {
	conn, _, err := websocket.DefaultDialer.Dial(WebSocketRTDSURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	s := &PriceSubscriber{
		conn:        conn,
		outputDir:   outputDir,
		symbols:     make(map[string]struct{}),
		done:        make(chan struct{}),
		closeCh:     make(chan struct{}),
		pongCh:      make(chan struct{}, 1),
		reconnectCh: make(chan struct{}, 1),
		name:        cfg.Name,
		topic:       cfg.Topic,
		topicType:   cfg.TopicType,
		filePrefix:  cfg.FilePrefix,
	}

	conn.SetPongHandler(s.pongHandler)
	go s.pingLoop()
	go s.readLoop()

	return s, nil
}

func (s *PriceSubscriber) pongHandler(appData string) error {
	select {
	case s.pongCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *PriceSubscriber) reconnect() error {
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

		log.Printf("%s: reconnect attempt %d/%d (backoff: %v)", s.name, attempt, maxRetries, backoff)

		conn, _, err := websocket.DefaultDialer.Dial(WebSocketRTDSURL, nil)
		if err != nil {
			log.Printf("%s: reconnect failed: %v", s.name, err)
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		conn.SetPongHandler(s.pongHandler)

		s.connMu.Lock()
		s.conn = conn
		s.connMu.Unlock()

		if err := s.resubscribe(); err != nil {
			log.Printf("%s: resubscribe failed: %v", s.name, err)
			_ = conn.Close()
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 2)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		log.Printf("%s: reconnected successfully", s.name)
		return nil
	}
	return fmt.Errorf("%s: max reconnect attempts (%d) exceeded", s.name, maxRetries)
}

func (s *PriceSubscriber) resubscribe() error {
	s.mu.Lock()
	hasSymbols := len(s.symbols) > 0
	s.mu.Unlock()

	if !hasSymbols {
		return nil
	}

	return s.writeJSON(rtdsMessage{
		Action: "subscribe",
		Subscriptions: []rtdsSubscription{
			{Topic: s.topic, Type: s.topicType},
		},
	})
}

func (s *PriceSubscriber) pingLoop() {
	const (
		pingInterval = 30 * time.Second
		pongTimeout  = 10 * time.Second
	)

	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		s.connMu.Lock()
		err := s.conn.WriteMessage(websocket.PingMessage, nil)
		s.connMu.Unlock()

		if err != nil {
			log.Printf("%s: ping error: %v", s.name, err)
			s.triggerReconnect()
			continue
		}

		select {
		case <-s.pongCh:
			select {
			case <-time.After(pingInterval):
			case <-s.closeCh:
				return
			}
		case <-time.After(pongTimeout):
			log.Printf("%s: pong timeout, reconnecting", s.name)
			s.triggerReconnect()
		case <-s.closeCh:
			return
		}
	}
}

func (s *PriceSubscriber) triggerReconnect() {
	if !s.reconnectMu.TryLock() {
		return // another goroutine is already reconnecting
	}
	defer s.reconnectMu.Unlock()

	s.connMu.Lock()
	oldConn := s.conn
	s.connMu.Unlock()

	if oldConn != nil {
		_ = oldConn.Close()
	}

	if err := s.reconnect(); err != nil {
		log.Printf("%s: reconnect failed: %v", s.name, err)
		s.closeOnce.Do(func() { close(s.closeCh) })
		return
	}

	select {
	case s.reconnectCh <- struct{}{}:
	default:
	}
}

func (s *PriceSubscriber) readLoop() {
	defer close(s.done)

	for {
		s.connMu.Lock()
		conn := s.conn
		s.connMu.Unlock()

		_, message, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			case <-s.reconnectCh:
				continue
			}
		}

		s.handleMessage(message)
	}
}

func (s *PriceSubscriber) handleMessage(data []byte) {
	if len(data) == 0 || (data[0] != '{' && data[0] != '[') {
		return
	}

	var event PriceEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return
	}

	if event.Topic != s.topic || event.Type != "update" {
		return
	}

	s.handlePriceEvent(event)
}

func (s *PriceSubscriber) handlePriceEvent(event PriceEvent) {
	symbol := event.Payload.Symbol
	timestamp := event.Payload.Timestamp

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.symbols[symbol]; !ok {
		return
	}

	if s.marketSlug == "" {
		return
	}

	safeSymbol := strings.ReplaceAll(symbol, "/", "")
	record := PriceRecord{
		Timestamp: fmt.Sprintf("%d", timestamp),
		Value:     strconv.FormatFloat(event.Payload.Value, 'f', -1, 64),
	}

	filename := fmt.Sprintf("%s_%s_%s.csv", s.marketSlug, s.filePrefix, safeSymbol)
	tickPath := filepath.Join(s.outputDir, filename)
	if err := util.AppendCSV(tickPath, []PriceRecord{record}); err != nil {
		log.Printf("%s: failed to write CSV: %v", s.name, err)
	}
}

func (s *PriceSubscriber) writeJSON(msg interface{}) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return s.conn.WriteJSON(msg)
}

func (s *PriceSubscriber) Subscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		s.symbols[sym] = struct{}{}
	}
	s.mu.Unlock()

	return s.writeJSON(rtdsMessage{
		Action: "subscribe",
		Subscriptions: []rtdsSubscription{
			{Topic: s.topic, Type: s.topicType},
		},
	})
}

func (s *PriceSubscriber) Unsubscribe(symbols []string) error {
	s.mu.Lock()
	for _, sym := range symbols {
		delete(s.symbols, sym)
	}
	s.mu.Unlock()

	return s.writeJSON(rtdsMessage{
		Action: "unsubscribe",
		Subscriptions: []rtdsSubscription{
			{Topic: s.topic, Type: s.topicType, Filters: strings.Join(symbols, ",")},
		},
	})
}

func (s *PriceSubscriber) SetMarketSlug(slug string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marketSlug = slug
}

func (s *PriceSubscriber) Close() error {
	s.closeOnce.Do(func() { close(s.closeCh) })

	s.connMu.Lock()
	defer s.connMu.Unlock()

	if s.conn != nil {
		_ = s.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		return s.conn.Close()
	}
	return nil
}

func (s *PriceSubscriber) Done() <-chan struct{} {
	return s.done
}

func NewCryptoSubscriber(outputDir string) (*PriceSubscriber, error) {
	return NewPriceSubscriber(outputDir, PriceSubscriberConfig{
		Name:       "binance",
		Topic:      "crypto_prices",
		TopicType:  "update",
		FilePrefix: "binance",
	})
}

func NewChainlinkSubscriber(outputDir string) (*PriceSubscriber, error) {
	return NewPriceSubscriber(outputDir, PriceSubscriberConfig{
		Name:       "chainlink",
		Topic:      "crypto_prices_chainlink",
		TopicType:  "*",
		FilePrefix: "chainlink",
	})
}
