package hermetix

// DB증권 실시간 스트림 — 체결 S00, 호가 S01, 주문 접수 IS0, 주문 체결 IS1. 문서 기반 구현, 실측 전. Kotlin DbMarketStream 과 동일 프로토콜.
//
//   - 접속: 운영 wss://openapi.dbsec.co.kr:7070/websocket, 모의 :17070/websocket. 핸드셰이크 헤더 없음
//   - 인증: 로그인 프레임 없이 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음)
//   - 시세 등록 {"header":{"token","tr_type":"1"},"body":{"tr_cd":"S00","tr_key":"J 005930"}}, 해제는 tr_type:"2".
//     tr_key = 시장구분 2자리(J + 공백) + 종목코드. NXT 는 NJN-005930, 통합은 UJU-005930 (미지원)
//   - 계좌 등록 {"header":{"token","tr_type":"3"},"body":{"tr_cd":"IS0"}} — tr_key 없음, 해제 메시지 없음(세션 종료가 해제)
//   - 접속 후 10초 안에 첫 메시지를 보내야 한다 → 구독이 있으면 OnOpen 에서 즉시 전송. 서버 주기 프레임이 없어 유휴 감시는 끈다
//   - 응답: 구독 ack {"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}, 오류·제어 프레임은 header/body 가 null 이거나
//     rsp_cd/rsp_msg 를 어느 쪽에든 싣는다(""/"0"/"00000" 가 정상). 데이터는 header.tr_cd 로 라우팅하고 header.tr_key 는 항상 null 이라 심볼은 body 에서 읽는다
//   - body 값은 모두 문자열(계좌계는 0 패딩). 필드명 대소문자가 문서와 예시에서 다르다(askp1 vs Askp1) → 대소문자 무시 조회
//
// Sordxctptncode(주문체결유형코드) 값표는 미공개라 상태는 수량 필드로 판정한다. 픽스처: conformance/fixtures/db.json#stream.
// 한도(SDK 문서): 계좌당 세션 2, 종목 50, 접속 6회/분, POST /api/v1/websocket/disconnectSession 으로 세션 정리 가능(미구현).

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	DbWsPaperURL      = "wss://openapi.dbsec.co.kr:17070/websocket"
	DbWsLiveURL       = "wss://openapi.dbsec.co.kr:7070/websocket"
	dbTrTrade         = "S00"
	dbTrOrderBook     = "S01"
	dbTrOrderAccepted = "IS0"
	dbTrOrderExecuted = "IS1"
	// KRX 주식/ETF 시장구분 2자리 — J + 공백
	dbMarketPrefix = "J "
)

var dbSuccessCodes = map[string]bool{"": true, "0": true, "00000": true}

type DbMarketStream struct {
	*reconnectingWebSocket
	wsURL     string
	token     func() (string, error)
	listeners *symbolListeners
}

func newDbMarketStream(wsURL string, token func() (string, error)) *DbMarketStream {
	s := &DbMarketStream{wsURL: wsURL, token: token, listeners: newSymbolListeners(nil)}
	s.reconnectingWebSocket = newReconnectingWebSocket("db", s)
	s.reconnectingWebSocket.idleTimeout = 0
	return s
}

func (s *DbMarketStream) IsConnected() bool { return s.IsSocketOpen() }
func (s *DbMarketStream) URI() string       { return s.wsURL }
func (s *DbMarketStream) OnDisconnected()   {}

// OnOpen - 접속 직후 전부 다시 보낸다 — 10초 규칙 때문에 지체하지 않는다.
func (s *DbMarketStream) OnOpen() {
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix db stream: 토큰 발급 실패 - %v", err)
		return
	}
	for _, code := range s.listeners.tradeKeys() {
		s.Send(s.quoteMessage(t, dbTrTrade, code, "1"))
	}
	for _, code := range s.listeners.bookKeys() {
		s.Send(s.quoteMessage(t, dbTrOrderBook, code, "1"))
	}
	if s.listeners.hasOrders() {
		s.sendAccountRegistrations(t)
	}
}

func (s *DbMarketStream) sendAccountRegistrations(t string) {
	s.Send(s.accountMessage(t, dbTrOrderAccepted))
	s.Send(s.accountMessage(t, dbTrOrderExecuted))
}

func (s *DbMarketStream) sendIfOpen(trCd string, codes []string) {
	if len(codes) == 0 || !s.IsSocketOpen() {
		return
	}
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix db stream: 토큰 발급 실패 - %v", err)
		return
	}
	for _, code := range codes {
		s.Send(s.quoteMessage(t, trCd, code, "1"))
	}
}

func (s *DbMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.sendIfOpen(dbTrTrade, s.listeners.addTrades(symbols, listener))
}

func (s *DbMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.sendIfOpen(dbTrOrderBook, s.listeners.addBooks(symbols, listener))
	return nil
}

func (s *DbMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if s.listeners.addOrders(listener) && s.IsSocketOpen() {
		t, err := s.token()
		if err != nil {
			return err
		}
		s.sendAccountRegistrations(t)
	}
	return nil
}

func (s *DbMarketStream) OnMessage(text string) {
	var frame jsonFrame
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		return
	}
	// 제어 프레임: rsp_cd/rsp_msg 가 header 또는 body 에 실린다. header/body 둘 다 비면 keepalive 류 — 무시
	rspCd := frameText(frame.Header, "rsp_cd")
	if rspCd == "" {
		rspCd = frameText(frame.Body, "rsp_cd")
	}
	rspMsg := frameText(frame.Header, "rsp_msg")
	if rspMsg == "" {
		rspMsg = frameText(frame.Body, "rsp_msg")
	}
	trCd := frameText(frame.Header, "tr_cd")
	if rspCd != "" || rspMsg != "" {
		if dbSuccessCodes[rspCd] {
			log.Printf("INFO hermetix db stream: %s %s", trCd, rspMsg)
		} else {
			log.Printf("WARN hermetix db stream: %s rsp_cd=%s %s", trCd, rspCd, rspMsg)
		}
		return
	}
	if frame.Header == nil || frame.Body == nil {
		return
	}
	if _, isAck := frame.Body["tr_key"].([]any); isAck {
		log.Printf("INFO hermetix db stream: subscribed %s %v", trCd, frame.Body["tr_key"])
		return
	}
	today := time.Now().In(kst)
	switch trCd {
	case dbTrTrade:
		if tick, ok := ParseDbTrade(frame.Body, today); ok {
			s.listeners.deliverTrade("db", tick.Symbol, tick)
		}
	case dbTrOrderBook:
		if tick, ok := ParseDbOrderBook(frame.Body, today); ok {
			s.listeners.deliverBook("db", tick.Symbol, tick)
		}
	case dbTrOrderAccepted, dbTrOrderExecuted:
		if event, ok := ParseDbOrderEvent(trCd, frame.Body, today); ok {
			s.listeners.deliverOrder("db", event)
		}
	default:
		logUnknownFrame("db", text)
	}
}

func (s *DbMarketStream) quoteMessage(t, trCd, code, trType string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"token": t, "tr_type": trType},
		"body":   map[string]string{"tr_cd": trCd, "tr_key": dbMarketPrefix + code},
	})
	return string(raw)
}

func (s *DbMarketStream) accountMessage(t, trCd string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"token": t, "tr_type": "3"},
		"body":   map[string]string{"tr_cd": trCd},
	})
	return string(raw)
}

// dbLower - 문서 표(askp1)와 예시(Askp1)의 대소문자가 달라 소문자 키로 조회한다.
func dbLower(body map[string]any) map[string]any {
	out := make(map[string]any, len(body))
	for k, v := range body {
		out[strings.ToLower(k)] = v
	}
	return out
}

func dbText(f map[string]any, field string) string { return frameText(f, strings.ToLower(field)) }
func dbDec(f map[string]any, field string) *decimal.Decimal {
	return frameDecimal(f, strings.ToLower(field))
}

// DbNormalizeStreamCode - U-005930 / N-005930 / A005930 → 005930.
func DbNormalizeStreamCode(raw string) string {
	return strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(raw), "U-"), "N-"), "A")
}

// dbTimeOfDay - HHmmss 또는 HHmmssSSS → 오늘 KST 시각 (앞 6자리).
func dbTimeOfDay(today time.Time, raw string) (time.Time, bool) {
	text := strings.TrimSpace(raw)
	if len(text) > 6 {
		text = text[:6]
	}
	return kstTimeOfDay(today, text)
}

// ParseDbTrade - S00 body → 체결. 심볼은 단축코드(요청 표기 복원은 스트림이 한다). 필드 부족이면 false.
func ParseDbTrade(body map[string]any, today time.Time) (TradeTick, bool) {
	f := dbLower(body)
	price := dbDec(f, "StckPrpr")
	if price == nil {
		return TradeTick{}, false
	}
	timestamp, ok := dbTimeOfDay(today, dbText(f, "StckCntghour"))
	if !ok {
		return TradeTick{}, false
	}
	tick := TradeTick{
		Symbol: DbNormalizeStreamCode(dbText(f, "ShrnIscd")), Price: *price, Quantity: decimal.Zero, Timestamp: timestamp,
		AskPrice: dbDec(f, "askp1"), BidPrice: dbDec(f, "bidp1"),
	}
	if q := dbDec(f, "CntgVol"); q != nil {
		tick.Quantity = *q
	}
	if v := dbDec(f, "AcmlVol"); v != nil {
		cumulative := v.IntPart()
		tick.CumulativeVolume = &cumulative
	}
	if change := dbDec(f, "PrdyVrss"); change != nil {
		sign := dbText(f, "PrdyVrsssign")
		falling := dbText(f, "PrdyVrssclr") == "-" || sign == "4" || sign == "5"
		if falling && change.IsPositive() {
			neg := change.Neg()
			change = &neg
		}
		tick.Change = change
	}
	if rate := dbDec(f, "PrdyCtrt"); rate != nil {
		r := rate.Div(hundred)
		tick.ChangeRate = &r
	}
	return tick, true
}

// ParseDbOrderBook - S01 body → 호가창(10단계, 0 호가는 제외).
func ParseDbOrderBook(body map[string]any, today time.Time) (OrderBookTick, bool) {
	f := dbLower(body)
	timestamp, ok := dbTimeOfDay(today, dbText(f, "BsopHour"))
	if !ok {
		return OrderBookTick{}, false
	}
	levels := func(priceField, qtyField string) []OrderBookLevel {
		out := make([]OrderBookLevel, 0, 10)
		for i := 1; i <= 10; i++ {
			suffix := string(rune('0' + i))
			if i == 10 {
				suffix = "10"
			}
			price := dbDec(f, priceField+suffix)
			if price == nil || price.IsZero() {
				continue
			}
			quantity := decimal.Zero
			if q := dbDec(f, qtyField+suffix); q != nil {
				quantity = *q
			}
			out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
		}
		return out
	}
	return OrderBookTick{
		Symbol: DbNormalizeStreamCode(dbText(f, "ShrnIscd")), Timestamp: timestamp,
		Asks: levels("askp", "AskpRsqn"), Bids: levels("bidp", "BidpRsqn"),
		TotalAskQuantity: dbDec(f, "TotalAskprsqn"), TotalBidQuantity: dbDec(f, "TotalBidprsqn"),
	}, true
}

// ParseDbOrderEvent - IS0(접수) / IS1(체결·정정·취소·거부) body → 주문 통보. 유형코드 값표가 없어 수량 필드로 판정한다:
// 거부수량 > 0 → REJECTED, 체결수량 > 0 → FILLED, 취소확인수량 > 0 → CANCELED, 정정확인수량 > 0 → MODIFIED, 그 외 ACCEPTED.
func ParseDbOrderEvent(trCd string, body map[string]any, today time.Time) (OrderEvent, bool) {
	f := dbLower(body)
	orderID := dbText(f, "Sordno")
	if orderID == "" {
		return OrderEvent{}, false
	}
	original := dbText(f, "Sorgordno")
	if strings.TrimLeft(original, "0") == "" || strings.TrimLeft(original, "0") == strings.TrimLeft(orderID, "0") {
		original = ""
	}
	var side *OrderSide
	switch dbText(f, "Sbnstp") {
	case "1":
		v := Sell
		side = &v
	case "2":
		v := Buy
		side = &v
	}
	symbol := DbNormalizeStreamCode(dbText(f, "Sshtnisuno"))
	if trCd == dbTrOrderAccepted {
		timestamp, ok := dbTimeOfDay(today, dbText(f, "Sordtm"))
		if !ok {
			return OrderEvent{}, false
		}
		return OrderEvent{
			OrderID: orderID, Type: OrderAccepted, Timestamp: timestamp, Symbol: symbol, Side: side,
			Quantity: dbDec(f, "Sordqty"), Price: dbDec(f, "Sordprc"), OriginalOrderID: original,
		}, true
	}
	positive := func(field string) bool {
		v := dbDec(f, field)
		return v != nil && v.IsPositive()
	}
	eventType := OrderAccepted
	var quantity, price *decimal.Decimal
	switch {
	case positive("Srjtqty"):
		eventType, quantity, price = OrderRejected, dbDec(f, "Srjtqty"), dbDec(f, "Sordprc")
	case positive("Sexecqty"):
		eventType, quantity, price = OrderFilled, dbDec(f, "Sexecqty"), dbDec(f, "Sexecprc")
	case positive("Scanccnfqty"):
		eventType, quantity, price = OrderCanceled, dbDec(f, "Scanccnfqty"), dbDec(f, "Sordprc")
	case positive("Smdfycnfqty"):
		eventType, quantity, price = OrderModified, dbDec(f, "Smdfycnfqty"), dbDec(f, "Smdfycnfprc")
	default:
		quantity, price = dbDec(f, "Sordqty"), dbDec(f, "Sordprc")
	}
	timeText := dbText(f, "Sexectime")
	if timeText == "" {
		timeText = dbText(f, "Sordtm")
	}
	timestamp, ok := dbTimeOfDay(today, timeText)
	if !ok {
		return OrderEvent{}, false
	}
	return OrderEvent{
		OrderID: orderID, Type: eventType, Timestamp: timestamp, Symbol: symbol, Side: side,
		Quantity: quantity, Price: price, RemainingQuantity: dbDec(f, "Sunercqty"), OriginalOrderID: original,
	}, true
}
