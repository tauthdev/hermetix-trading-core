package hermetix

// 토스증권 실시간 스트림. 공식 AsyncAPI 1.2.2 문서 기반, 실측 전 (모의투자 서버가 없어 실계좌로만 검증 가능). Kotlin TossMarketStream 과 동일 프로토콜.
//
// 프로토콜 (wss://openapi-ws.tossinvest.com/ws/v1):
//   - 핸드셰이크에 Authorization: Bearer {access_token} — REST 와 같은 토큰. 토큰은 접속 때만 검사된다
//   - 구독은 선언형: 클라이언트가 보내는 JSON 배열 하나가 현재 구독 집합 전체다. 새 배열이 이전 집합을 통째로 대체하고 빠진 항목은 자동 해제.
//     요소는 {"type":"trade:kr","codes":["005930"]} 꼴, 첫 요소 {"id":"req-N"} 은 응답에 echo 된다.
//     type: trade:us / trade:kr / orderbook:us / orderbook:kr (codes = 종목코드, 미국은 대문자 티커), personal:order (codes = accountSeq 문자열)
//   - 서버 프레임은 type 으로 구분: subscriptions(구독 결과, subscribed[]·rejected[]{target,code,message}), message(데이터, topic = trade:kr:005930),
//     error(선언 전체 실패·서버 재시작 server-shutdown), pong
//   - 180초 동안 클라이언트가 아무것도 보내지 않으면 서버가 끊는다 → 60초마다 텍스트 프레임 PING (JSON 아님) → {"type":"pong"}
//   - 한도: 계정당 연결 2개, 연결당 구독 100개, 선언 5회/초 → 구독 변경은 declareDelay 동안 모아 한 번에 보낸다
//   - trade/orderbook 은 유실 가능(백프레셔 시 최신 우선), personal:order 는 세션 내 무손실. 재접속 뒤 놓친 이벤트는 재전송되지 않는다
//
// 프레임 (모든 숫자는 문자열): trade data price/volume(이 체결 수량)/timestamp(ISO-8601 +09:00)/currency — 누적거래량·등락·매수매도 구분 없음.
// orderbook data timestamp(null 가능)/currency/asks[]·bids[] {price, volume} (최우선부터, 단계 수 미명시).
// personal:order data event(PENDING/PARTIAL_FILL/FILL/CANCELING/CANCELED/REPLACING/REPLACED/REJECTED/CANCEL_REJECTED/REPLACE_REJECTED)/accountSeq/order
// (REST 주문 상세와 같은 스냅샷 — execution.filledQuantity 는 누적이라 이번 체결량은 직전 스냅샷과의 차이).
//
// 문서로 확정하지 못한 점(실측 필요): 호가 단계 수, 취소·정정 시 어떤 orderId 로 이벤트가 오는지(취소는 REST 에서 새 orderId 발급), 표준 ping 이 유휴 타이머를 리셋하는지.

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const TossWsURL = "wss://openapi-ws.tossinvest.com/ws/v1"

type TossMarketStream struct {
	*reconnectingWebSocket
	wsURL        string
	token        func() (string, error)
	accountSeq   func() (string, error)
	declareDelay time.Duration
	// topic 키(kr:005930) → 리스너, topic 키 → 구독 요청 표기
	listeners *symbolListeners

	mu              sync.Mutex
	rejectedTargets map[string]bool            // 서버가 거부한 target(trade:kr:999999) — 다시 선언하면 또 거부되므로 뺀다
	filledSoFar     map[string]decimal.Decimal // orderId → 직전 스냅샷의 누적 체결량 (이번 체결량 = 차이)
	pendingDeclare  *time.Timer
	requestCounter  int
}

func newTossMarketStream(wsURL string, token, accountSeq func() (string, error)) *TossMarketStream {
	s := &TossMarketStream{
		wsURL: wsURL, token: token, accountSeq: accountSeq, declareDelay: 200 * time.Millisecond,
		listeners:       newSymbolListeners(tossTopicKeyOrEmpty),
		rejectedTargets: map[string]bool{}, filledSoFar: map[string]decimal.Decimal{},
	}
	s.reconnectingWebSocket = newReconnectingWebSocket("toss", s)
	s.reconnectingWebSocket.idleTimeout = 0
	s.reconnectingWebSocket.heartbeat = 60 * time.Second
	return s
}

func tossTopicKeyOrEmpty(symbol string) string {
	key, err := TossTopicKey(symbol)
	if err != nil {
		log.Printf("WARN hermetix toss stream: %v", err)
		return ""
	}
	return key
}

// TossTopicKey - Hermetix 심볼 → topic 키 (KRX:005930/005930 → kr:005930, US:AAPL → us:AAPL).
func TossTopicKey(symbol string) (string, error) {
	market, code := ParseSymbol(symbol)
	switch market {
	case "", "KRX":
		return "kr:" + code, nil
	case "US":
		return "us:" + strings.ToUpper(code), nil
	}
	return "", fmt.Errorf("토스 어댑터가 지원하지 않는 시장: %s (%s)", market, symbol)
}

// TossCanonicalSymbol - topic 의 시장·코드 → 정규 표기 (KRX:005930, US:AAPL).
func TossCanonicalSymbol(market, code string) string {
	if market == "us" {
		return "US:" + code
	}
	return "KRX:" + code
}

func (s *TossMarketStream) IsConnected() bool { return s.IsSocketOpen() }
func (s *TossMarketStream) URI() string       { return s.wsURL }
func (s *TossMarketStream) OnDisconnected()   {}

func (s *TossMarketStream) Headers() map[string]string {
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix toss stream: 토큰 발급 실패 - %v", err)
		return nil
	}
	return map[string]string{"Authorization": "Bearer " + t}
}

func (s *TossMarketStream) OnOpen()      { s.declareNow() }
func (s *TossMarketStream) OnHeartbeat() { s.Send("PING") }

func (s *TossMarketStream) Close() error {
	s.mu.Lock()
	if s.pendingDeclare != nil {
		s.pendingDeclare.Stop()
	}
	s.mu.Unlock()
	return s.reconnectingWebSocket.Close()
}

// ------------------------------------------------------------------ subscribe

func (s *TossMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	if len(s.listeners.addTrades(symbols, listener)) > 0 {
		s.scheduleDeclare()
	}
}

func (s *TossMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	if len(s.listeners.addBooks(symbols, listener)) > 0 {
		s.scheduleDeclare()
	}
	return nil
}

func (s *TossMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if s.listeners.addOrders(listener) {
		s.scheduleDeclare()
	}
	return nil
}

func (s *TossMarketStream) scheduleDeclare() {
	if !s.IsSocketOpen() {
		return // 접속되면 OnOpen 이 전체를 선언한다
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingDeclare != nil {
		s.pendingDeclare.Stop()
	}
	s.pendingDeclare = time.AfterFunc(s.declareDelay, s.declareNow)
}

// declareNow - 현재 구독 집합 전체를 한 배열로 보낸다.
func (s *TossMarketStream) declareNow() {
	declaration := s.BuildDeclaration()
	if len(declaration) <= 1 {
		return // id 만 있으면 보낼 게 없다
	}
	raw, _ := json.Marshal(declaration)
	s.Send(string(raw))
}

// BuildDeclaration - [{"id":"req-N"},{"type":"trade:kr","codes":[…]},…] — 거부된 target 은 뺀다.
func (s *TossMarketStream) BuildDeclaration() []map[string]any {
	s.mu.Lock()
	s.requestCounter++
	items := []map[string]any{{"id": fmt.Sprintf("req-%d", s.requestCounter)}}
	rejected := make(map[string]bool, len(s.rejectedTargets))
	for k, v := range s.rejectedTargets {
		rejected[k] = v
	}
	s.mu.Unlock()
	codesFor := func(prefix string, keys []string, market string) []string {
		codes := make([]string, 0)
		for _, key := range keys {
			if !strings.HasPrefix(key, market+":") {
				continue
			}
			code := strings.TrimPrefix(key, market+":")
			if rejected[prefix+":"+market+":"+code] {
				continue
			}
			codes = append(codes, code)
		}
		sort.Strings(codes)
		return codes
	}
	tradeKeys, bookKeys := s.listeners.tradeKeys(), s.listeners.bookKeys()
	for _, market := range []string{"kr", "us"} {
		if codes := codesFor("trade", tradeKeys, market); len(codes) > 0 {
			items = append(items, map[string]any{"type": "trade:" + market, "codes": codes})
		}
	}
	for _, market := range []string{"kr", "us"} {
		if codes := codesFor("orderbook", bookKeys, market); len(codes) > 0 {
			items = append(items, map[string]any{"type": "orderbook:" + market, "codes": codes})
		}
	}
	if s.listeners.hasOrders() {
		seq, err := s.accountSeq()
		if err != nil {
			log.Printf("WARN hermetix toss stream: accountSeq 조회 실패 - 주문 이벤트 구독 보류: %v", err)
		} else if !rejected["personal:order:"+seq] {
			items = append(items, map[string]any{"type": "personal:order", "codes": []string{seq}})
		}
	}
	return items
}

// ------------------------------------------------------------------ frames

func (s *TossMarketStream) OnMessage(text string) {
	var node struct {
		Type       string          `json:"type"`
		ID         string          `json:"id"`
		Topic      string          `json:"topic"`
		Data       json.RawMessage `json:"data"`
		Subscribed []string        `json:"subscribed"`
		Rejected   []struct {
			Target  string `json:"target"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"rejected"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(text), &node); err != nil {
		return
	}
	switch node.Type {
	case "pong":
	case "subscriptions":
		log.Printf("INFO hermetix toss stream: subscribed %d (id=%s)", len(node.Subscribed), node.ID)
		for _, r := range node.Rejected {
			log.Printf("WARN hermetix toss stream: 구독 거부 %s %s %s — 선언에서 제외한다", r.Target, r.Code, r.Message)
			if r.Target != "" {
				s.mu.Lock()
				s.rejectedTargets[r.Target] = true
				s.mu.Unlock()
			}
		}
	case "error":
		log.Printf("WARN hermetix toss stream: error %s %s (id=%s)", node.Error.Code, node.Error.Message, node.ID)
	case "message":
		s.onData(node.Topic, node.Data)
	default:
		logUnknownFrame("toss", text)
	}
}

func (s *TossMarketStream) onData(topic string, data json.RawMessage) {
	parts := strings.SplitN(topic, ":", 3)
	if len(parts) < 3 {
		return
	}
	key := parts[1] + ":" + parts[2]
	switch parts[0] {
	case "trade":
		if tick, ok := ParseTossTrade(topic, data); ok {
			s.listeners.deliverTrade("toss", key, tick)
		}
	case "orderbook":
		if book, ok := ParseTossOrderBook(topic, data); ok {
			s.listeners.deliverBook("toss", key, book)
		}
	case "personal":
		var probe struct {
			Order struct {
				OrderID   string `json:"orderId"`
				Execution struct {
					FilledQuantity string `json:"filledQuantity"`
				} `json:"execution"`
			} `json:"order"`
		}
		if err := json.Unmarshal(data, &probe); err != nil {
			return
		}
		s.mu.Lock()
		previous, hasPrevious := s.filledSoFar[probe.Order.OrderID]
		s.mu.Unlock()
		var previousPtr *decimal.Decimal
		if hasPrevious {
			previousPtr = &previous
		}
		event, ok := ParseTossOrderEvent(data, previousPtr)
		s.mu.Lock()
		if filled, err := decimal.NewFromString(strings.TrimSpace(probe.Order.Execution.FilledQuantity)); err == nil {
			s.filledSoFar[probe.Order.OrderID] = filled
		}
		if ok && (event.Type == OrderCanceled || event.Type == OrderRejected) {
			delete(s.filledSoFar, probe.Order.OrderID)
		}
		s.mu.Unlock()
		if ok {
			s.listeners.deliverOrder("toss", event)
		}
	}
}

// ---------------------------------------------------------------- parsers

func tossDecimal(raw *string) *decimal.Decimal {
	if raw == nil {
		return nil
	}
	text := strings.TrimSpace(*raw)
	if text == "" {
		return nil
	}
	d, err := decimal.NewFromString(text)
	if err != nil {
		return nil
	}
	return &d
}

func tossInstant(raw *string) (time.Time, bool) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, strings.TrimSpace(*raw)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ParseTossTrade - trade:{kr|us}:{code} 데이터 → 체결. 누적거래량·등락·호가는 프레임에 없어 nil.
func ParseTossTrade(topic string, data []byte) (TradeTick, bool) {
	parts := strings.SplitN(topic, ":", 3)
	if len(parts) < 3 || parts[0] != "trade" {
		return TradeTick{}, false
	}
	var d struct {
		Price     *string `json:"price"`
		Volume    *string `json:"volume"`
		Timestamp *string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return TradeTick{}, false
	}
	price := tossDecimal(d.Price)
	if price == nil {
		return TradeTick{}, false
	}
	tick := TradeTick{Symbol: TossCanonicalSymbol(parts[1], parts[2]), Price: *price, Quantity: decimal.Zero, Timestamp: time.Now()}
	if q := tossDecimal(d.Volume); q != nil {
		tick.Quantity = *q
	}
	if ts, ok := tossInstant(d.Timestamp); ok {
		tick.Timestamp = ts
	}
	return tick, true
}

// ParseTossOrderBook - orderbook:{kr|us}:{code} 데이터 → 호가창. asks 오름차순·bids 내림차순으로 오므로 순서 그대로가 최우선부터.
func ParseTossOrderBook(topic string, data []byte) (OrderBookTick, bool) {
	parts := strings.SplitN(topic, ":", 3)
	if len(parts) < 3 || parts[0] != "orderbook" {
		return OrderBookTick{}, false
	}
	type level struct {
		Price  *string `json:"price"`
		Volume *string `json:"volume"`
	}
	var d struct {
		Timestamp *string `json:"timestamp"`
		Asks      []level `json:"asks"`
		Bids      []level `json:"bids"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return OrderBookTick{}, false
	}
	levels := func(in []level) []OrderBookLevel {
		out := make([]OrderBookLevel, 0, len(in))
		for _, l := range in {
			price := tossDecimal(l.Price)
			if price == nil {
				continue
			}
			quantity := decimal.Zero
			if q := tossDecimal(l.Volume); q != nil {
				quantity = *q
			}
			out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
		}
		return out
	}
	book := OrderBookTick{Symbol: TossCanonicalSymbol(parts[1], parts[2]), Timestamp: time.Now(), Asks: levels(d.Asks), Bids: levels(d.Bids)}
	if ts, ok := tossInstant(d.Timestamp); ok {
		book.Timestamp = ts
	}
	return book, true
}

// ParseTossOrderEvent - personal:order 데이터 → 주문 이벤트. previousFilled 는 같은 orderId 의 직전 누적 체결량 (nil 이면 이번이 첫 스냅샷).
// CANCELING/REPLACING 은 중간 상태라 false.
func ParseTossOrderEvent(data []byte, previousFilled *decimal.Decimal) (OrderEvent, bool) {
	var d struct {
		Event string `json:"event"`
		Order struct {
			OrderID   string  `json:"orderId"`
			Symbol    string  `json:"symbol"`
			Side      string  `json:"side"`
			Price     *string `json:"price"`
			Quantity  *string `json:"quantity"`
			Currency  string  `json:"currency"`
			OrderedAt *string `json:"orderedAt"`
			Execution struct {
				FilledQuantity     *string `json:"filledQuantity"`
				AverageFilledPrice *string `json:"averageFilledPrice"`
			} `json:"execution"`
		} `json:"order"`
	}
	if err := json.Unmarshal(data, &d); err != nil || d.Order.OrderID == "" {
		return OrderEvent{}, false
	}
	var eventType OrderEventType
	switch d.Event {
	case "PENDING":
		eventType = OrderAccepted
	case "PARTIAL_FILL", "FILL":
		eventType = OrderFilled
	case "CANCELED":
		eventType = OrderCanceled
	case "REPLACED":
		eventType = OrderModified
	case "REJECTED", "CANCEL_REJECTED", "REPLACE_REJECTED":
		eventType = OrderRejected
	default:
		return OrderEvent{}, false // CANCELING, REPLACING, 미지 값
	}
	filled := decimal.Zero
	if f := tossDecimal(d.Order.Execution.FilledQuantity); f != nil {
		filled = *f
	}
	quantity := tossDecimal(d.Order.Quantity)
	price := tossDecimal(d.Order.Price)
	market := "KRX"
	if d.Order.Currency == "USD" {
		market = "US"
	}
	event := OrderEvent{OrderID: d.Order.OrderID, Type: eventType, Timestamp: time.Now()}
	if d.Order.Symbol != "" {
		event.Symbol = market + ":" + d.Order.Symbol
	}
	switch d.Order.Side {
	case "BUY":
		v := Buy
		event.Side = &v
	case "SELL":
		v := Sell
		event.Side = &v
	}
	if ts, ok := tossInstant(d.Order.OrderedAt); ok {
		event.Timestamp = ts
	}
	if eventType == OrderFilled {
		fillQuantity := filled
		if previousFilled != nil {
			fillQuantity = filled.Sub(*previousFilled)
			if fillQuantity.IsNegative() {
				fillQuantity = filled
			}
		}
		event.Quantity = &fillQuantity
		event.Price = tossDecimal(d.Order.Execution.AverageFilledPrice)
		if event.Price == nil {
			event.Price = price
		}
	} else {
		event.Quantity, event.Price = quantity, price
	}
	if quantity != nil {
		remaining := quantity.Sub(filled)
		event.RemainingQuantity = &remaining
	}
	if eventType == OrderRejected {
		event.Reason = d.Event
	}
	return event, true
}
