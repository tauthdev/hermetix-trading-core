package hermetix

// 키움 REST API 실시간 스트림. 체결 0B·호가 0D 는 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과(00 등록도 return_code 0),
// 주문체결 00 프레임은 문서 기반 (통보 실측 전). Kotlin 레퍼런스와 동일 프로토콜.
//
//   - 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/…
//   - 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} → {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
//   - 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]} → {"trnm":"REG","return_code":0}.
//     주문체결(00)은 계좌 단위라 item 을 빈 문자열 하나로 등록한다
//   - 데이터: {"data":[{"values":{…},"type":"0B","name":"주식체결","item":"005930"}],"trnm":"REAL"} (data 키가 trnm 보다 앞에 온다)
//   - {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다
//
// FID:
//   - 0B 주식체결: 20 체결시각, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가, 15 거래량(+매수/-매도), 13 누적거래량
//   - 0D 주식호가잔량: 21 호가시각, 41–50 매도호가1–10, 61–70 매도잔량1–10, 51–60 매수호가1–10, 71–80 매수잔량1–10, 121 총매도잔량, 125 총매수잔량
//   - 00 주문체결: 9203 주문번호, 904 원주문번호, 9001 종목코드(A접두), 913 주문상태(접수/체결/확인), 905 주문구분(+매수/-매도/매수취소…), 907 매도수구분(1 매도/2 매수),
//     900 주문수량, 901 주문가격, 902 미체결수량, 910 체결가, 911 체결량, 908 주문/체결시각, 919 거부사유
//
// REST 와 같이 가격·호가·수량에 등락 부호가 붙으므로 절대값으로 파싱한다. 실측 프레임은 conformance/fixtures/kiwoom.json#stream.

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const (
	KiwoomWsPaperURL      = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket"
	KiwoomWsLiveURL       = "wss://api.kiwoom.com:10000/api/dostk/websocket"
	kiwoomTypeTrade       = "0B"
	kiwoomTypeOrderBook   = "0D"
	kiwoomTypeOrderEvents = "00"
)

type KiwoomMarketStream struct {
	*reconnectingWebSocket
	wsURL string
	token func() (string, error)

	mu               sync.Mutex
	loggedIn         bool
	listeners        map[string][]TradeListener
	bookListeners    map[string][]OrderBookListener
	orderListeners   []OrderEventListener
	requestedSymbols map[string]string
}

func newKiwoomMarketStream(wsURL string, token func() (string, error)) *KiwoomMarketStream {
	s := &KiwoomMarketStream{
		wsURL: wsURL, token: token,
		listeners: map[string][]TradeListener{}, bookListeners: map[string][]OrderBookListener{},
		requestedSymbols: map[string]string{},
	}
	s.reconnectingWebSocket = newReconnectingWebSocket("kiwoom", s)
	return s
}

// IsConnected - 소켓이 열려 있고 LOGIN 까지 끝났는지.
func (s *KiwoomMarketStream) IsConnected() bool {
	s.mu.Lock()
	loggedIn := s.loggedIn
	s.mu.Unlock()
	return s.IsSocketOpen() && loggedIn
}

func (s *KiwoomMarketStream) URI() string { return s.wsURL }

func (s *KiwoomMarketStream) OnOpen() {
	s.setLoggedIn(false)
	token, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix kiwoom stream: 토큰 발급 실패 - %v", err)
		return
	}
	raw, _ := json.Marshal(map[string]string{"trnm": "LOGIN", "token": token})
	s.Send(string(raw))
}

func (s *KiwoomMarketStream) OnDisconnected() { s.setLoggedIn(false) }

func (s *KiwoomMarketStream) setLoggedIn(v bool) {
	s.mu.Lock()
	s.loggedIn = v
	s.mu.Unlock()
}

func registerKiwoom[L any](s *KiwoomMarketStream, symbols []string, listener L, target map[string][]L) []string {
	newCodes := make([]string, 0)
	for _, symbol := range symbols {
		code := SymbolCode(symbol)
		if _, ok := s.requestedSymbols[code]; !ok {
			s.requestedSymbols[code] = symbol
		}
		if _, ok := target[code]; !ok {
			newCodes = append(newCodes, code)
		}
		target[code] = append(target[code], listener)
	}
	return newCodes
}

func (s *KiwoomMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.mu.Lock()
	newCodes := registerKiwoom(s, symbols, listener, s.listeners)
	s.mu.Unlock()
	s.usageSubscribed(StreamTrades, len(newCodes))
	if len(newCodes) > 0 && s.IsConnected() {
		s.Send(s.registerMessage(newCodes, kiwoomTypeTrade))
	}
}

func (s *KiwoomMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.mu.Lock()
	newCodes := registerKiwoom(s, symbols, listener, s.bookListeners)
	s.mu.Unlock()
	s.usageSubscribed(StreamOrderBook, len(newCodes))
	if len(newCodes) > 0 && s.IsConnected() {
		s.Send(s.registerMessage(newCodes, kiwoomTypeOrderBook))
	}
	return nil
}

func (s *KiwoomMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	s.mu.Lock()
	first := len(s.orderListeners) == 0
	s.orderListeners = append(s.orderListeners, listener)
	s.mu.Unlock()
	if first {
		s.usageSubscribed(StreamOrderEvents, 1)
	}
	if first && s.IsConnected() {
		s.Send(s.registerMessage([]string{""}, kiwoomTypeOrderEvents))
	}
	return nil
}

// registerAll - 로그인 직후 모든 그룹을 등록한다.
func (s *KiwoomMarketStream) registerAll() {
	s.mu.Lock()
	tradeCodes := keys(s.listeners)
	bookCodes := keys(s.bookListeners)
	hasOrders := len(s.orderListeners) > 0
	s.mu.Unlock()
	if len(tradeCodes) > 0 {
		s.Send(s.registerMessage(tradeCodes, kiwoomTypeTrade))
	}
	if len(bookCodes) > 0 {
		s.Send(s.registerMessage(bookCodes, kiwoomTypeOrderBook))
	}
	if hasOrders {
		s.Send(s.registerMessage([]string{""}, kiwoomTypeOrderEvents))
	}
}

func (s *KiwoomMarketStream) OnMessage(text string) {
	var node struct {
		Trnm       string `json:"trnm"`
		ReturnCode *int   `json:"return_code"`
		ReturnMsg  string `json:"return_msg"`
	}
	if err := json.Unmarshal([]byte(text), &node); err != nil {
		return
	}
	returnCode := -1
	if node.ReturnCode != nil {
		returnCode = *node.ReturnCode
	}
	switch node.Trnm {
	case "PING":
		s.Send(text)
	case "LOGIN":
		if returnCode == 0 {
			s.setLoggedIn(true)
			log.Printf("INFO hermetix kiwoom stream: logged in")
			s.registerAll()
		} else {
			log.Printf("ERROR hermetix kiwoom stream: 로그인 실패 return_code=%d %s", returnCode, node.ReturnMsg)
		}
	case "REG":
		if returnCode == 0 {
			log.Printf("INFO hermetix kiwoom stream: registered")
		} else {
			log.Printf("WARN hermetix kiwoom stream: 등록 실패 return_code=%d %s", returnCode, node.ReturnMsg)
		}
	case "REAL":
		today := time.Now().In(kst)
		raw := []byte(text)
		for _, tick := range ParseKiwoomReal(raw, today) {
			s.deliver(tick)
		}
		for _, tick := range ParseKiwoomOrderBook(raw, today) {
			s.deliverBook(tick)
		}
		for _, event := range ParseKiwoomOrderEvents(raw, today) {
			s.deliverOrderEvent(event)
		}
	}
}

func (s *KiwoomMarketStream) deliver(tick TradeTick) {
	code := tick.Symbol
	s.mu.Lock()
	symbol, ok := s.requestedSymbols[code]
	listeners := append([]TradeListener(nil), s.listeners[code]...)
	s.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	s.usageMessage(StreamTrades)
	for _, listener := range listeners {
		safeCall("kiwoom", tick.Symbol, func() { listener(tick) })
	}
}

func (s *KiwoomMarketStream) deliverBook(tick OrderBookTick) {
	code := tick.Symbol
	s.mu.Lock()
	symbol, ok := s.requestedSymbols[code]
	listeners := append([]OrderBookListener(nil), s.bookListeners[code]...)
	s.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	s.usageMessage(StreamOrderBook)
	for _, listener := range listeners {
		safeCall("kiwoom", tick.Symbol, func() { listener(tick) })
	}
}

func (s *KiwoomMarketStream) deliverOrderEvent(event OrderEvent) {
	s.mu.Lock()
	listeners := append([]OrderEventListener(nil), s.orderListeners...)
	s.mu.Unlock()
	s.usageMessage(StreamOrderEvents)
	for _, listener := range listeners {
		safeCall("kiwoom", event.OrderID, func() { listener(event) })
	}
}

func (s *KiwoomMarketStream) registerMessage(items []string, realType string) string {
	raw, _ := json.Marshal(map[string]any{
		"trnm": "REG", "grp_no": "1", "refresh": "1",
		"data": []map[string]any{{"item": items, "type": []string{realType}}},
	})
	return string(raw)
}

type kiwoomRealItem struct {
	Type   string            `json:"type"`
	Item   string            `json:"item"`
	Values map[string]string `json:"values"`
}

func kiwoomItems(raw []byte, realType string) []kiwoomRealItem {
	var frame struct {
		Data []kiwoomRealItem `json:"data"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil
	}
	items := make([]kiwoomRealItem, 0, len(frame.Data))
	for _, item := range frame.Data {
		if item.Type == realType {
			items = append(items, item)
		}
	}
	return items
}

func (item kiwoomRealItem) signed(fid string) *decimal.Decimal { return dOrNil(item.Values[fid]) }

func (item kiwoomRealItem) abs(fid string) *decimal.Decimal {
	v := item.signed(fid)
	if v == nil {
		return nil
	}
	a := v.Abs()
	return &a
}

func (item kiwoomRealItem) code() string {
	return strings.TrimPrefix(strings.TrimSpace(item.Item), "A")
}

// ParseKiwoomReal - REAL 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (A 프리픽스 제거).
func ParseKiwoomReal(raw []byte, today time.Time) []TradeTick {
	items := kiwoomItems(raw, kiwoomTypeTrade)
	ticks := make([]TradeTick, 0, len(items))
	for _, item := range items {
		price := item.abs("10")
		if price == nil {
			continue
		}
		timestamp, ok := kstTimeOfDay(today, item.Values["20"])
		if !ok {
			continue
		}
		tick := TradeTick{
			Symbol: item.code(), Price: *price, Quantity: decimal.Zero, Timestamp: timestamp,
			AskPrice: item.abs("27"), BidPrice: item.abs("28"), Change: item.signed("11"),
		}
		if qty := item.abs("15"); qty != nil {
			tick.Quantity = *qty
		}
		if volume := item.abs("13"); volume != nil {
			v := volume.IntPart()
			tick.CumulativeVolume = &v
		}
		if rate := item.signed("12"); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100))
			tick.ChangeRate = &converted
		}
		ticks = append(ticks, tick)
	}
	return ticks
}

// ParseKiwoomOrderBook - REAL 프레임 → 호가창 목록 (0D 만). 0 호가는 빈 단계로 보고 뺀다.
func ParseKiwoomOrderBook(raw []byte, today time.Time) []OrderBookTick {
	items := kiwoomItems(raw, kiwoomTypeOrderBook)
	ticks := make([]OrderBookTick, 0, len(items))
	for _, item := range items {
		timestamp, ok := kstTimeOfDay(today, item.Values["21"])
		if !ok {
			continue
		}
		levels := func(priceFrom, qtyFrom int) []OrderBookLevel {
			out := make([]OrderBookLevel, 0, 10)
			for i := 0; i < 10; i++ {
				price := item.abs(strconv.Itoa(priceFrom + i))
				if price == nil || price.IsZero() {
					continue
				}
				quantity := decimal.Zero
				if q := item.abs(strconv.Itoa(qtyFrom + i)); q != nil {
					quantity = *q
				}
				out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
			}
			return out
		}
		ticks = append(ticks, OrderBookTick{
			Symbol: item.code(), Timestamp: timestamp,
			Asks: levels(41, 61), Bids: levels(51, 71),
			TotalAskQuantity: item.abs("121"), TotalBidQuantity: item.abs("125"),
		})
	}
	return ticks
}

// ParseKiwoomOrderEvents - REAL 프레임 → 주문 통보 목록 (00 만).
func ParseKiwoomOrderEvents(raw []byte, today time.Time) []OrderEvent {
	items := kiwoomItems(raw, kiwoomTypeOrderEvents)
	events := make([]OrderEvent, 0, len(items))
	for _, item := range items {
		clock := strings.TrimSpace(item.Values["908"])
		if clock == "" {
			clock = "0"
		}
		timestamp, ok := kstTimeOfDay(today, clock)
		if !ok {
			continue
		}
		status := strings.TrimSpace(item.Values["913"])
		kind := strings.TrimSpace(item.Values["905"])
		reason := strings.TrimSpace(item.Values["919"])
		filledQty := item.abs("911")
		eventType := OrderAccepted
		switch {
		case reason != "":
			eventType = OrderRejected
		case strings.Contains(status, "체결") && filledQty != nil && filledQty.IsPositive():
			eventType = OrderFilled
		case strings.Contains(kind, "취소"):
			eventType = OrderCanceled
		case strings.Contains(kind, "정정"):
			eventType = OrderModified
		}
		event := OrderEvent{
			OrderID: strings.TrimSpace(item.Values["9203"]), Type: eventType, Timestamp: timestamp,
			Symbol:            strings.TrimPrefix(strings.TrimSpace(item.Values["9001"]), "A"),
			RemainingQuantity: item.abs("902"), Reason: reason,
		}
		if eventType == OrderFilled {
			event.Quantity, event.Price = filledQty, item.abs("910")
		} else {
			event.Quantity, event.Price = item.abs("900"), item.abs("901")
		}
		switch strings.TrimSpace(item.Values["907"]) {
		case "1":
			side := Sell
			event.Side = &side
		case "2":
			side := Buy
			event.Side = &side
		}
		if original := strings.TrimSpace(item.Values["904"]); original != "" && strings.TrimLeft(original, "0") != "" {
			event.OriginalOrderID = original
		}
		events = append(events, event)
	}
	return events
}
