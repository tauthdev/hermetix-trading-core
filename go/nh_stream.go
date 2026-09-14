package hermetix

// NH PLUG 실시간 스트림. 문서 기반 구현, 실측 전 — 포털 openapi.json x-realtime-channels·API 가이드 DB·공식 Python SDK(nhplug-sdk) 에서 역추적.
// Kotlin NhMarketStream 과 동일 프로토콜.
//
//   - 접속: 모의 wss://moapi.nhplug.com:17070/websocket, 운영 wss://api.nhplug.com:7070/websocket (경로 /websocket 필수). 핸드셰이크 헤더 없음
//   - 인증: 별도 로그인 프레임 없이 매 구독 메시지의 header.token (REST 접근토큰 그대로)
//   - 구독: {"header":{"token","tr_type":"1"|"2"},"body":{"tr_cd":<채널>,"tr_key":<종목코드>}}. 통보 채널은 tr_key 빈 문자열
//   - 채널: 체결 KRX oc / NXT nc / 통합 mc, 호가 ob / nb / mb — REST 와 달리 시장을 채널코드로 고른다 (marketCd 에 맞춤).
//     통보는 체결 d2(체결·정정·취소·거부 결과) + 접수 d3(신규·정정·취소 접수), 둘 다 국내주식/국내파생 공용(itemgb 로 구분)
//   - 응답(ACK): {"header":{"tr_type","tr_cd","rsp_cd":"00000","rsp_msg"},"body":{"tr_key":[…]}} — 데이터 푸시 header 에는 rsp_cd/tr_type 이 없다
//   - 데이터: {"header":{"tr_cd","tr_key"},"body":{…}} (통보는 header 에 tr_key 없음). 값은 예시상 전부 문자열 — 숫자도 허용해 파싱
//   - heartbeat 없음(문서 명시) — 조용한 게 정상이므로 유휴 감시를 끈다. 암호화 없음
//
// 문서로 확정하지 못한 점(실측 필요): sign/kospigb/janggubun/ordercd/order_type/procnm 코드값(sign 은 REST prdy_vrss_sign 과 같은 4/5/8/9 하락으로 가정),
// movolume(이번 체결량)·new_volume(누적)·value(백만원) 해석, orderno 와 REST mkt_orr_no 의 동일성(선행 0 무시 비교), 거부·정정 통보 순서,
// 모의(17070)에서 시세 채널이 오는지(포털 가이드는 "미제공"), 세션당 등록 한도 10(SDK 실측)/30(공식), 앱키당 세션 2개, 운영 서버 중간 CA 미전송.

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	NhWsPaperURL = "wss://moapi.nhplug.com:17070/websocket"
	NhWsLiveURL  = "wss://api.nhplug.com:7070/websocket"
)

var nhOrderChannels = []string{"d2", "d3"}
var nhBookPrefixes = []string{"", "P_", "S_", "S4_", "S5_", "S6_", "S7_", "S8_", "S9_", "S10_"}

type NhMarketStream struct {
	*reconnectingWebSocket
	wsURL     string
	accountNo string
	token     func() (string, error)
	// 시장 구분에 따른 채널코드 — KRX oc/ob, NXT nc/nb, UNT(통합) mc/mb. tr_cd 는 대소문자를 구분하므로 정규화하지 않는다
	tradeChannel, bookChannel string
	listeners                 *symbolListeners
}

func newNhMarketStream(wsURL, marketCd, accountNo string, token func() (string, error)) *NhMarketStream {
	s := &NhMarketStream{wsURL: wsURL, accountNo: accountNo, token: token, listeners: newSymbolListeners(nil)}
	switch strings.ToUpper(marketCd) {
	case "NXT":
		s.tradeChannel, s.bookChannel = "nc", "nb"
	case "UNT":
		s.tradeChannel, s.bookChannel = "mc", "mb"
	default:
		s.tradeChannel, s.bookChannel = "oc", "ob"
	}
	s.reconnectingWebSocket = newReconnectingWebSocket("nh", s)
	s.reconnectingWebSocket.idleTimeout = 0
	return s
}

func (s *NhMarketStream) IsConnected() bool { return s.IsSocketOpen() }
func (s *NhMarketStream) URI() string       { return s.wsURL }
func (s *NhMarketStream) OnDisconnected()   {}

func (s *NhMarketStream) OnOpen() {
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix nh stream: 토큰 발급 실패 - %v", err)
		return
	}
	for _, code := range s.listeners.tradeKeys() {
		s.Send(s.message(t, "1", s.tradeChannel, code))
	}
	for _, code := range s.listeners.bookKeys() {
		s.Send(s.message(t, "1", s.bookChannel, code))
	}
	if s.listeners.hasOrders() {
		for _, ch := range nhOrderChannels {
			s.Send(s.message(t, "1", ch, ""))
		}
	}
}

func (s *NhMarketStream) sendIfOpen(channel string, codes []string) {
	if len(codes) == 0 || !s.IsSocketOpen() {
		return
	}
	t, err := s.token()
	if err != nil {
		log.Printf("ERROR hermetix nh stream: 토큰 발급 실패 - %v", err)
		return
	}
	for _, code := range codes {
		s.Send(s.message(t, "1", channel, code))
	}
}

func (s *NhMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.sendIfOpen(s.tradeChannel, s.listeners.addTrades(symbols, listener))
}

func (s *NhMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.sendIfOpen(s.bookChannel, s.listeners.addBooks(symbols, listener))
	return nil
}

func (s *NhMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if s.listeners.addOrders(listener) && s.IsSocketOpen() {
		t, err := s.token()
		if err != nil {
			return err
		}
		for _, ch := range nhOrderChannels {
			s.Send(s.message(t, "1", ch, ""))
		}
	}
	return nil
}

func (s *NhMarketStream) OnMessage(text string) {
	var frame jsonFrame
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		return
	}
	trCd := frameText(frame.Header, "tr_cd")
	_, hasRsp := frame.Header["rsp_cd"]
	_, hasType := frame.Header["tr_type"]
	if hasRsp || hasType {
		rspCd := frameText(frame.Header, "rsp_cd")
		msg := frameText(frame.Header, "rsp_msg")
		if rspCd == "00000" {
			verb := "subscribed"
			if frameText(frame.Header, "tr_type") == "2" {
				verb = "unsubscribed"
			}
			log.Printf("INFO hermetix nh stream: %s %s %v (%s)", verb, trCd, frame.Body["tr_key"], msg)
		} else {
			log.Printf("WARN hermetix nh stream: %s rsp_cd=%s %s", trCd, rspCd, msg)
		}
		return
	}
	if frame.Body == nil {
		return
	}
	today := time.Now().In(kst)
	switch trCd {
	case "oc", "nc", "mc":
		for _, tick := range ParseNhTrade(frame, today) {
			s.listeners.deliverTrade("nh", tick.Symbol, tick)
		}
	case "ob", "nb", "mb":
		for _, tick := range ParseNhOrderBook(frame, today) {
			s.listeners.deliverBook("nh", tick.Symbol, tick)
		}
	case "d2", "d3":
		for _, event := range ParseNhOrderEvents(frame, today, s.accountNo) {
			s.listeners.deliverOrder("nh", event)
		}
	default:
		logUnknownFrame("nh", text)
	}
}

func (s *NhMarketStream) message(token, trType, trCd, trKey string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"token": token, "tr_type": trType},
		"body":   map[string]string{"tr_cd": trCd, "tr_key": trKey},
	})
	return string(raw)
}

// nhTimeOfDay - "HH:MM:SS" 또는 "HHMMSS" (KST, 날짜 없음 → today).
func nhTimeOfDay(today time.Time, raw string) (time.Time, bool) {
	digits := strings.Builder{}
	for _, ch := range raw {
		if ch >= '0' && ch <= '9' {
			digits.WriteRune(ch)
		}
	}
	text := digits.String()
	if len(text) > 6 {
		text = text[:6]
	}
	return kstTimeOfDay(today, text)
}

// nhSigned - 부호 코드(4/5/8/9 하락)로 부호를 붙인다. 값이 이미 음수면 그대로.
func nhSigned(value *decimal.Decimal, sign string) *decimal.Decimal {
	if value == nil {
		return nil
	}
	if nhFallingSigns[sign] && value.IsPositive() {
		neg := value.Neg()
		return &neg
	}
	return value
}

// ParseNhTrade - 체결 프레임(oc/nc/mc) → 체결. 심볼은 header.tr_key 또는 body.code 그대로 (요청 표기 복원은 스트림이 한다).
func ParseNhTrade(frame jsonFrame, today time.Time) []TradeTick {
	body := frame.Body
	if body == nil {
		return nil
	}
	price := frameDecimal(body, "price")
	if price == nil {
		return nil
	}
	timestamp, ok := nhTimeOfDay(today, frameText(body, "time"))
	if !ok {
		return nil
	}
	symbol := frameText(frame.Header, "tr_key")
	if symbol == "" {
		symbol = frameText(body, "code")
	}
	sign := frameText(body, "sign")
	tick := TradeTick{
		Symbol: symbol, Price: *price, Quantity: decimal.Zero, Timestamp: timestamp,
		AskPrice: frameDecimal(body, "offer"), BidPrice: frameDecimal(body, "bid"),
		Change: nhSigned(frameDecimal(body, "change"), sign),
	}
	if q := frameDecimal(body, "movolume"); q != nil {
		tick.Quantity = *q
	}
	cumulative := frameDecimal(body, "new_volume")
	if cumulative == nil {
		cumulative = frameDecimal(body, "volume")
	}
	if cumulative != nil {
		v := cumulative.IntPart()
		tick.CumulativeVolume = &v
	}
	if rate := nhSigned(frameDecimal(body, "chrate"), sign); rate != nil {
		r := rate.Div(hundred)
		tick.ChangeRate = &r
	}
	return []TradeTick{tick}
}

// ParseNhOrderBook - 호가 프레임(ob/nb/mb) → 호가창. 1단계 접두 없음, 2단계 P_, 3단계 S_, 4~10단계 S4_~S10_. 0 호가는 뺀다.
func ParseNhOrderBook(frame jsonFrame, today time.Time) []OrderBookTick {
	body := frame.Body
	if body == nil {
		return nil
	}
	timestamp, ok := nhTimeOfDay(today, frameText(body, "hotime"))
	if !ok {
		return nil
	}
	symbol := frameText(frame.Header, "tr_key")
	if symbol == "" {
		symbol = frameText(body, "code")
	}
	levels := func(priceField, qtyField string) []OrderBookLevel {
		out := make([]OrderBookLevel, 0, 10)
		for _, p := range nhBookPrefixes {
			price := frameDecimal(body, p+priceField)
			if price == nil || price.IsZero() {
				continue
			}
			quantity := decimal.Zero
			if q := frameDecimal(body, p+qtyField); q != nil {
				quantity = *q
			}
			out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
		}
		return out
	}
	return []OrderBookTick{{
		Symbol: symbol, Timestamp: timestamp,
		Asks: levels("offer", "offerrem"), Bids: levels("bid", "bidrem"),
		TotalAskQuantity: frameDecimal(body, "T_offerrem"), TotalBidQuantity: frameDecimal(body, "T_bidrem"),
	}}
}

// ParseNhOrderEvents - 통보 프레임 → 이벤트. d3(접수) → ACCEPTED, d2 → rejgb=1 REJECTED / ucgb 0 체결 FILLED, 1 정정 MODIFIED, 2 취소·3 효력해제 CANCELED.
// 국내주식(itemgb=1)만, accountNo 가 주어지면 계좌가 같은 것만.
func ParseNhOrderEvents(frame jsonFrame, today time.Time, accountNo string) []OrderEvent {
	trCd := frameText(frame.Header, "tr_cd")
	b := frame.Body
	if b == nil || (trCd != "d2" && trCd != "d3") {
		return nil
	}
	if frameText(b, "itemgb") != "1" {
		return nil
	}
	if accountNo != "" && frameText(b, "accountno") != "" && frameText(b, "accountno") != accountNo {
		return nil
	}
	var side *OrderSide
	switch frameText(b, "slbygb") {
	case "1":
		v := Sell
		side = &v
	case "2":
		v := Buy
		side = &v
	}
	original := frameText(b, "orgordno")
	if strings.TrimLeft(original, "0") == "" {
		original = ""
	}
	orderID := frameText(b, "orderno")
	symbol := NhNormalizeCode(frameText(b, "issuecd"))
	if trCd == "d3" {
		timestamp, ok := nhTimeOfDay(today, frameText(b, "order_time"))
		if !ok {
			return nil
		}
		return []OrderEvent{{
			OrderID: orderID, Type: OrderAccepted, Timestamp: timestamp, Symbol: symbol, Side: side,
			Quantity: frameDecimal(b, "ordergty"), Price: frameDecimal(b, "orderprc"), OriginalOrderID: original,
		}}
	}
	timestamp, ok := nhTimeOfDay(today, frameText(b, "conctime"))
	if !ok {
		return nil
	}
	eventType := OrderFilled
	switch {
	case frameText(b, "rejgb") == "1":
		eventType = OrderRejected
	case frameText(b, "ucgb") == "1":
		eventType = OrderModified
	case frameText(b, "ucgb") == "2", frameText(b, "ucgb") == "3":
		eventType = OrderCanceled
	}
	return []OrderEvent{{
		OrderID: orderID, Type: eventType, Timestamp: timestamp, Symbol: symbol, Side: side,
		Quantity: frameDecimal(b, "concgty"), Price: frameDecimal(b, "concprc"),
	}}
}
