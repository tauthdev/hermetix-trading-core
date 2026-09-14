package hermetix

// LS증권 OPEN API 실시간 스트림. 문서 기반 구현, 모의 실측 전 — 포털 실시간 TR 문서와 커뮤니티 클라이언트(ebest·LsApiHelper·krsec)에서 역추적.
// Kotlin LsMarketStream 과 동일 프로토콜.
//
//   - 접속: 모의 wss://openapi.ls-sec.co.kr:29443/websocket, 실전 :9443/websocket. 핸드셰이크 헤더·로그인 프레임 없음
//   - 인증: 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음). 토큰은 익일 07:00 만료 → 재접속 시 캐시 토큰을 다시 싣는다
//   - 시세 등록/해제: {"header":{"token","tr_type":"3"|"4"},"body":{"tr_cd":"S3_","tr_key":"005930"}}
//   - 계좌 등록/해제: tr_type "1"/"2", tr_cd SC0(접수)·SC1(체결)·SC2(정정)·SC3(취소)·SC4(거부), tr_key 빈 문자열. TR 마다 따로 보낸다
//   - 응답: 등록 ACK 는 header 에 rsp_cd/rsp_msg 가 있고 body 가 없다. 데이터는 {"header":{"tr_cd","tr_key"},"body":{…}}, 값은 전부 문자열
//   - 하트비트 없음(문서 미기재) — 유휴 감시를 끈다
//
// KOSPI/KOSDAQ 선택: 서버는 종목으로 시장을 고르지 않는다 — KOSDAQ 코드를 S3_ 에 넣으면 아무것도 오지 않는다. 종목마스터(t8436) 조회 대신
// 종목마다 KOSPI TR(S3_/H1_)과 KOSDAQ TR(K3_/HA_)을 둘 다 등록한다. 맞지 않는 쪽은 조용히 비고, 등록 수만 2배가 된다 (한도 미문서).
//
// 실측 시 확인할 것: ACK JSON 키·rsp_cd 값, SC4 본문·거부 사유, 세션당 등록 한도, 앱키당 세션 수, 07:00 토큰 만료 시 소켓 동작.
// 픽스처(문서 재구성값): conformance/fixtures/ls.json#stream.

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	LsWsPaperURL = "wss://openapi.ls-sec.co.kr:29443/websocket"
	LsWsLiveURL  = "wss://openapi.ls-sec.co.kr:9443/websocket"

	lsTrTradeKospi  = "S3_"
	lsTrTradeKosdaq = "K3_"
	lsTrBookKospi   = "H1_"
	lsTrBookKosdaq  = "HA_"
	lsTrOrderAccept = "SC0"
	lsTrOrderFill   = "SC1"
	lsTrOrderModify = "SC2"
	lsTrOrderCancel = "SC3"
	lsTrOrderReject = "SC4"

	lsTrTypeAccountRegister   = "1"
	lsTrTypeAccountUnregister = "2"
	lsTrTypeSubscribe         = "3"
	lsTrTypeUnsubscribe       = "4"
	lsRspOK                   = "00000"
)

var (
	lsTradeTRs = []string{lsTrTradeKospi, lsTrTradeKosdaq}
	lsBookTRs  = []string{lsTrBookKospi, lsTrBookKosdaq}
	lsOrderTRs = []string{lsTrOrderAccept, lsTrOrderFill, lsTrOrderModify, lsTrOrderCancel, lsTrOrderReject}
)

type LsMarketStream struct {
	*reconnectingWebSocket
	wsURL     string
	token     func() (string, error)
	listeners *symbolListeners
}

func newLsMarketStream(wsURL string, token func() (string, error)) *LsMarketStream {
	s := &LsMarketStream{wsURL: wsURL, token: token, listeners: newSymbolListeners(nil)}
	s.reconnectingWebSocket = newReconnectingWebSocket("ls", s)
	s.reconnectingWebSocket.idleTimeout = 0
	return s
}

func (s *LsMarketStream) IsConnected() bool { return s.IsSocketOpen() }
func (s *LsMarketStream) URI() string       { return s.wsURL }
func (s *LsMarketStream) OnDisconnected()   {}

func (s *LsMarketStream) OnOpen() {
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix ls stream: 토큰 발급 실패 - %v", err)
		return
	}
	s.sendMarket(t, lsTrTypeSubscribe, lsTradeTRs, s.listeners.tradeKeys())
	s.sendMarket(t, lsTrTypeSubscribe, lsBookTRs, s.listeners.bookKeys())
	if s.listeners.hasOrders() {
		s.sendAccount(t, lsTrTypeAccountRegister)
	}
}

func (s *LsMarketStream) sendMarket(t, trType string, trs []string, codes []string) {
	for _, code := range codes {
		for _, tr := range trs {
			s.Send(s.message(t, trType, tr, code))
		}
	}
}

func (s *LsMarketStream) sendAccount(t, trType string) {
	for _, tr := range lsOrderTRs {
		s.Send(s.message(t, trType, tr, ""))
	}
}

func (s *LsMarketStream) sendMarketIfOpen(trType string, trs []string, codes []string) {
	if len(codes) == 0 || !s.IsSocketOpen() {
		return
	}
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix ls stream: 토큰 발급 실패 - %v", err)
		return
	}
	s.sendMarket(t, trType, trs, codes)
}

func (s *LsMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.sendMarketIfOpen(lsTrTypeSubscribe, lsTradeTRs, s.listeners.addTrades(symbols, listener))
}

func (s *LsMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.sendMarketIfOpen(lsTrTypeSubscribe, lsBookTRs, s.listeners.addBooks(symbols, listener))
	return nil
}

func (s *LsMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if s.listeners.addOrders(listener) && s.IsSocketOpen() {
		t, err := s.token()
		if err != nil {
			return err
		}
		s.sendAccount(t, lsTrTypeAccountRegister)
	}
	return nil
}

// UnsubscribeTrades - 체결 구독 해제 (tr_type 4). 리스너도 지운다.
func (s *LsMarketStream) UnsubscribeTrades(symbols []string) {
	codes := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		codes = append(codes, SymbolCode(symbol))
	}
	s.sendMarketIfOpen(lsTrTypeUnsubscribe, lsTradeTRs, s.listeners.removeTrades(codes))
}

// UnsubscribeOrderBook - 호가 구독 해제 (tr_type 4). 리스너도 지운다.
func (s *LsMarketStream) UnsubscribeOrderBook(symbols []string) {
	codes := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		codes = append(codes, SymbolCode(symbol))
	}
	s.sendMarketIfOpen(lsTrTypeUnsubscribe, lsBookTRs, s.listeners.removeBooks(codes))
}

// UnsubscribeOrderEvents - 계좌 통보 해제 (tr_type 2). 리스너도 지운다.
func (s *LsMarketStream) UnsubscribeOrderEvents() {
	if !s.listeners.clearOrders() || !s.IsSocketOpen() {
		return
	}
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix ls stream: 토큰 발급 실패 - %v", err)
		return
	}
	s.sendAccount(t, lsTrTypeAccountUnregister)
}

func (s *LsMarketStream) OnMessage(text string) {
	var frame jsonFrame
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		return
	}
	trCd := frameText(frame.Header, "tr_cd")
	if lsIsControl(frame) {
		rspCd := frameText(frame.Header, "rsp_cd")
		msg := frameText(frame.Header, "rsp_msg")
		if rspCd == "" || rspCd == lsRspOK {
			log.Printf("INFO hermetix ls stream: ack %s %s tr_type=%s (%s)", trCd, frameText(frame.Header, "tr_key"), frameText(frame.Header, "tr_type"), msg)
		} else {
			log.Printf("WARN hermetix ls stream: %s %s rsp_cd=%s %s", trCd, frameText(frame.Header, "tr_key"), rspCd, msg)
		}
		return
	}
	today := time.Now().In(kst)
	switch {
	case containsString(lsTradeTRs, trCd):
		if tick, ok := ParseLsTrade(frame, today); ok {
			s.listeners.deliverTrade("ls", tick.Symbol, tick)
		}
	case containsString(lsBookTRs, trCd):
		if tick, ok := ParseLsOrderBook(frame, today); ok {
			s.listeners.deliverBook("ls", tick.Symbol, tick)
		}
	case containsString(lsOrderTRs, trCd):
		for _, event := range ParseLsOrderEvents(frame, today) {
			s.listeners.deliverOrder("ls", event)
		}
	default:
		logUnknownFrame("ls", text)
	}
}

func (s *LsMarketStream) message(token, trType, trCd, trKey string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"token": token, "tr_type": trType},
		"body":   map[string]string{"tr_cd": trCd, "tr_key": trKey},
	})
	return string(raw)
}

func containsString(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}

// lsIsControl - ACK/오류 프레임(header 에 rsp_*·body 없음)이면 true — 데이터 파서는 건너뛴다.
func lsIsControl(frame jsonFrame) bool {
	_, hasMsg := frame.Header["rsp_msg"]
	_, hasCd := frame.Header["rsp_cd"]
	return hasMsg || hasCd || len(frame.Body) == 0
}

// lsMarketCode - 시세 프레임의 종목코드 — header.tr_key 6자리, 없으면 body.shcode.
func lsMarketCode(frame jsonFrame) string {
	key := frameText(frame.Header, "tr_key")
	if len(key) == 6 {
		return key
	}
	return frameText(frame.Body, "shcode")
}

// lsTimeOfDay - HHMMSS 또는 HHMMSSmmm → 오늘 KST 시각.
func lsTimeOfDay(today time.Time, raw string) (time.Time, bool) {
	text := strings.TrimSpace(raw)
	for len(text) < 6 {
		text = "0" + text
	}
	return kstTimeOfDay(today, text[:6])
}

// ParseLsTrade - 체결 프레임(S3_/K3_) → 체결. 심볼은 단축코드 그대로. TR 불일치·필수 필드 부족이면 false.
func ParseLsTrade(frame jsonFrame, today time.Time) (TradeTick, bool) {
	if lsIsControl(frame) || !containsString(lsTradeTRs, frameText(frame.Header, "tr_cd")) {
		return TradeTick{}, false
	}
	b := frame.Body
	price := frameDecimal(b, "price")
	if price == nil {
		return TradeTick{}, false
	}
	timestamp, ok := lsTimeOfDay(today, frameText(b, "chetime"))
	if !ok {
		return TradeTick{}, false
	}
	tick := TradeTick{
		Symbol: lsMarketCode(frame), Price: *price, Quantity: decimal.Zero, Timestamp: timestamp,
		AskPrice: frameDecimal(b, "offerho"), BidPrice: frameDecimal(b, "bidho"),
	}
	if q := frameDecimal(b, "cvolume"); q != nil {
		tick.Quantity = *q
	}
	if v := frameDecimal(b, "volume"); v != nil {
		cumulative := v.IntPart()
		tick.CumulativeVolume = &cumulative
	}
	if change := frameDecimal(b, "change"); change != nil {
		if lsFallingSigns[frameText(b, "sign")] && change.IsPositive() {
			neg := change.Neg()
			change = &neg
		}
		tick.Change = change
	}
	if rate := frameDecimal(b, "drate"); rate != nil {
		r := rate.Div(hundred)
		tick.ChangeRate = &r
	}
	return tick, true
}

// ParseLsOrderBook - 호가 프레임(H1_/HA_) → 호가창. 0 호가는 빈 단계로 보고 뺀다.
func ParseLsOrderBook(frame jsonFrame, today time.Time) (OrderBookTick, bool) {
	if lsIsControl(frame) || !containsString(lsBookTRs, frameText(frame.Header, "tr_cd")) {
		return OrderBookTick{}, false
	}
	b := frame.Body
	timestamp, ok := lsTimeOfDay(today, frameText(b, "hotime"))
	if !ok {
		return OrderBookTick{}, false
	}
	levels := func(pricePrefix, qtyPrefix string) []OrderBookLevel {
		out := make([]OrderBookLevel, 0, 10)
		for i := 1; i <= 10; i++ {
			suffix := "10"
			if i < 10 {
				suffix = string(rune('0' + i))
			}
			price := frameDecimal(b, pricePrefix+suffix)
			if price == nil || price.IsZero() {
				continue
			}
			quantity := decimal.Zero
			if q := frameDecimal(b, qtyPrefix+suffix); q != nil {
				quantity = *q
			}
			out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
		}
		return out
	}
	return OrderBookTick{
		Symbol: lsMarketCode(frame), Timestamp: timestamp,
		Asks: levels("offerho", "offerrem"), Bids: levels("bidho", "bidrem"),
		TotalAskQuantity: frameDecimal(b, "totofferrem"), TotalBidQuantity: frameDecimal(b, "totbidrem"),
	}, true
}

// ParseLsOrderEvents - 주문 통보 프레임(SC0~SC4) → 이벤트 목록 (프레임당 1건, 파싱 실패면 빈 목록).
func ParseLsOrderEvents(frame jsonFrame, today time.Time) []OrderEvent {
	trCd := frameText(frame.Header, "tr_cd")
	if lsIsControl(frame) || !containsString(lsOrderTRs, trCd) {
		return nil
	}
	b := frame.Body
	orderID := frameText(b, "ordno")
	if orderID == "" {
		return nil
	}
	original := frameText(b, "orgordno")
	if strings.TrimLeft(original, "0") == "" || original == orderID {
		original = ""
	}
	var side *OrderSide
	switch frameText(b, "bnstp") {
	case "1":
		v := Sell
		side = &v
	case "2":
		v := Buy
		side = &v
	}
	symbolRaw := frameText(b, "shtnIsuno")
	if symbolRaw == "" {
		symbolRaw = frameText(b, "shtcode")
	}
	symbol := LsNormalizeCode(symbolRaw)
	eventType := OrderAccepted
	switch frameText(b, "ordxctptncode") {
	case "11":
		eventType = OrderFilled
	case "12":
		eventType = OrderModified
	case "13":
		eventType = OrderCanceled
	case "14":
		eventType = OrderRejected
	default:
		switch trCd {
		case lsTrOrderFill:
			eventType = OrderFilled
		case lsTrOrderModify:
			eventType = OrderModified
		case lsTrOrderCancel:
			eventType = OrderCanceled
		case lsTrOrderReject:
			eventType = OrderRejected
		}
	}
	var quantity, price *decimal.Decimal
	switch eventType {
	case OrderAccepted:
		quantity, price = frameDecimal(b, "ordqty"), frameDecimal(b, "ordprice")
	case OrderFilled:
		quantity, price = frameDecimal(b, "execqty"), frameDecimal(b, "execprc")
	case OrderModified:
		quantity, price = frameDecimal(b, "mdfycnfqty"), frameDecimal(b, "mdfycnfprc")
	case OrderCanceled:
		quantity, price = frameDecimal(b, "canccnfqty"), frameDecimal(b, "ordprc")
	case OrderRejected:
		quantity, price = frameDecimal(b, "rjtqty"), frameDecimal(b, "ordprc")
	}
	timeText := frameText(b, "exectime")
	if trCd == lsTrOrderAccept {
		timeText = frameText(b, "ordtm")
	}
	timestamp, ok := lsTimeOfDay(today, timeText)
	if !ok {
		return nil
	}
	event := OrderEvent{
		OrderID: orderID, Type: eventType, Timestamp: timestamp, Symbol: symbol, Side: side,
		Quantity: quantity, Price: price, OriginalOrderID: original,
	}
	if trCd != lsTrOrderAccept {
		event.RemainingQuantity = frameDecimal(b, "unercqty")
	}
	if eventType == OrderRejected {
		event.Reason = frameText(b, "msgcode")
	}
	return []OrderEvent{event}
}
