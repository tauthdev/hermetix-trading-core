package hermetix

// KIS 실시간 스트림. 체결가 H0STCNT0·호가 H0STASP0 는 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과,
// 주문 통보 H0STCNI9(모의)/H0STCNI0(실전)는 문서 기반 (HTS ID 로 구독해야 실측 가능). Kotlin 레퍼런스와 동일 프로토콜.
//
//   - 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
//   - 인증: REST POST /oauth2/Approval 로 받은 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급)
//   - 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id","tr_key"}}}
//     tr_key 는 시세 TR 이면 종목코드, 주문 통보 TR 이면 HTS ID
//   - 데이터 프레임: 0|TR|<건수>|<필드^필드^…> — 첫 세그먼트 0 은 평문, 1 은 AES 암호문(base64).
//     레코드가 여러 건이면 본문에 이어 붙는다 (폭 = 전체 필드 수 / 건수). 실측: 체결가는 한 프레임에 최대 3건
//   - 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd, 암호화 TR 이면 output.iv/key), PINGPONG(그대로 되돌려 보내야 연결 유지)
//
// 필드 순서:
//   - H0STCNT0 (폭 47): 0 코드, 1 시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함), 5 전일대비율(%), … 10 매도호가1, 11 매수호가1, 12 체결량, 13 누적거래량
//   - H0STASP0 (실측 폭 63): 0 코드, 1 시각, 2 시간구분, 3–12 매도호가1–10, 13–22 매수호가1–10, 23–32 매도잔량1–10, 33–42 매수잔량1–10, 43 총매도잔량, 44 총매수잔량
//   - H0STCNI9/0 (AES-256-CBC, 구독 응답의 key/iv): 0 고객ID, 1 계좌번호, 2 주문번호, 3 원주문번호, 4 매도매수구분(01 매도/02 매수), 5 정정취소구분(0/1 정정/2 취소),
//     8 종목코드, 9 체결수량, 10 체결단가, 11 체결시각, 12 거부여부(0/1), 13 체결여부(1 접수·정정·취소·거부 / 2 체결), 16 주문수량, … 25 주문가격
//
// 실측 프레임은 conformance/fixtures/kis.json#stream.

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const (
	KisWsPaperURL         = "ws://ops.koreainvestment.com:31000"
	KisWsLiveURL          = "ws://ops.koreainvestment.com:21000"
	kisTrTrade            = "H0STCNT0"
	kisTrOrderBook        = "H0STASP0"
	kisTrOrderEventsPaper = "H0STCNI9"
	kisTrOrderEventsLive  = "H0STCNI0"
	kisMinFields          = 14
	kisMinBookFields      = 45
	kisMinOrderFields     = 17
)

type KisMarketStream struct {
	*reconnectingWebSocket
	wsURL       string
	custtype    string
	htsID       string
	live        bool
	approvalKey func() (string, error)

	mu               sync.Mutex
	listeners        map[string][]TradeListener     // 종목코드 → 체결 리스너
	bookListeners    map[string][]OrderBookListener // 종목코드 → 호가 리스너
	orderListeners   []OrderEventListener
	requestedSymbols map[string]string    // 종목코드 → 구독 요청 표기
	cipherKeys       map[string][2]string // 암호화 TR → (key, iv) — 구독 응답 output
}

func newKisMarketStream(wsURL, custtype string, approvalKey func() (string, error)) *KisMarketStream {
	s := &KisMarketStream{
		wsURL: wsURL, custtype: custtype, approvalKey: approvalKey,
		listeners: map[string][]TradeListener{}, bookListeners: map[string][]OrderBookListener{},
		requestedSymbols: map[string]string{}, cipherKeys: map[string][2]string{},
	}
	s.reconnectingWebSocket = newReconnectingWebSocket("kis", s)
	return s
}

func (s *KisMarketStream) IsConnected() bool { return s.IsSocketOpen() }

func (s *KisMarketStream) URI() string { return s.wsURL }

func (s *KisMarketStream) trOrderEvents() string {
	if s.live {
		return kisTrOrderEventsLive
	}
	return kisTrOrderEventsPaper
}

func (s *KisMarketStream) OnOpen() {
	key, err := s.approvalKey()
	if err != nil {
		log.Printf("ERROR hermetix kis stream: 접속키 발급 실패 - %v", err)
		return
	}
	s.mu.Lock()
	tradeCodes := keys(s.listeners)
	bookCodes := keys(s.bookListeners)
	hasOrders := len(s.orderListeners) > 0
	s.mu.Unlock()
	for _, code := range tradeCodes {
		s.Send(s.subscribeMessage(key, kisTrTrade, code))
	}
	for _, code := range bookCodes {
		s.Send(s.subscribeMessage(key, kisTrOrderBook, code))
	}
	if hasOrders {
		s.Send(s.subscribeMessage(key, s.trOrderEvents(), s.htsID))
	}
}

func (s *KisMarketStream) OnDisconnected() {}

func keys[L any](m map[string][]L) []string {
	out := make([]string, 0, len(m))
	for code := range m {
		out = append(out, code)
	}
	return out
}

// register - 심볼을 코드로 바꿔 리스너를 붙이고, 새로 구독해야 하는 코드 목록을 돌려준다. 잠금은 호출자가.
func register[L any](s *KisMarketStream, symbols []string, listener L, target map[string][]L) []string {
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

func (s *KisMarketStream) sendSubscriptions(trID string, codes []string) {
	if len(codes) == 0 || !s.IsSocketOpen() {
		return
	}
	key, err := s.approvalKey()
	if err != nil {
		log.Printf("ERROR hermetix kis stream: 접속키 발급 실패 - %v", err)
		return
	}
	for _, code := range codes {
		s.Send(s.subscribeMessage(key, trID, code))
	}
}

func (s *KisMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.mu.Lock()
	newCodes := register(s, symbols, listener, s.listeners)
	s.mu.Unlock()
	s.sendSubscriptions(kisTrTrade, newCodes)
}

func (s *KisMarketStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.mu.Lock()
	newCodes := register(s, symbols, listener, s.bookListeners)
	s.mu.Unlock()
	s.sendSubscriptions(kisTrOrderBook, newCodes)
	return nil
}

func (s *KisMarketStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if strings.TrimSpace(s.htsID) == "" {
		return errors.New("KIS 주문 통보 구독에는 HTS ID 가 필요합니다 (SetHTSID)")
	}
	s.mu.Lock()
	first := len(s.orderListeners) == 0
	s.orderListeners = append(s.orderListeners, listener)
	s.mu.Unlock()
	if first && s.IsSocketOpen() {
		key, err := s.approvalKey()
		if err != nil {
			return err
		}
		s.Send(s.subscribeMessage(key, s.trOrderEvents(), s.htsID))
	}
	return nil
}

func (s *KisMarketStream) OnMessage(text string) {
	if strings.HasPrefix(text, "0|") || strings.HasPrefix(text, "1|") {
		s.onDataFrame(text)
		return
	}
	var node struct {
		Header struct {
			TrID  string `json:"tr_id"`
			TrKey string `json:"tr_key"`
		} `json:"header"`
		Body *struct {
			RtCd   string `json:"rt_cd"`
			MsgCd  string `json:"msg_cd"`
			Msg1   string `json:"msg1"`
			Output *struct {
				Key string `json:"key"`
				IV  string `json:"iv"`
			} `json:"output"`
		} `json:"body"`
	}
	if err := json.Unmarshal([]byte(text), &node); err != nil {
		return
	}
	if node.Header.TrID == "PINGPONG" {
		s.Send(text)
		return
	}
	if node.Body == nil {
		return
	}
	if node.Body.Output != nil && node.Body.Output.Key != "" && node.Body.Output.IV != "" {
		s.mu.Lock()
		s.cipherKeys[node.Header.TrID] = [2]string{node.Body.Output.Key, node.Body.Output.IV}
		s.mu.Unlock()
	}
	trKey := node.Header.TrKey
	if strings.HasPrefix(node.Header.TrID, "H0STCNI") {
		trKey = "(hts)"
	}
	switch {
	case node.Body.RtCd == "0" || node.Body.MsgCd == "OPSP0000":
		log.Printf("INFO hermetix kis stream: subscribed %s %s (%s)", node.Header.TrID, trKey, node.Body.Msg1)
	case node.Body.MsgCd == "OPSP0002":
		log.Printf("INFO hermetix kis stream: already subscribed %s", node.Header.TrID)
	default:
		log.Printf("WARN hermetix kis stream: %s %s rt_cd=%s msg_cd=%s %s", node.Header.TrID, trKey, node.Body.RtCd, node.Body.MsgCd, node.Body.Msg1)
	}
}

func (s *KisMarketStream) onDataFrame(text string) {
	parts := strings.SplitN(text, "|", 4)
	if len(parts) < 4 {
		return
	}
	trID := parts[1]
	body := parts[3]
	if parts[0] == "1" {
		s.mu.Lock()
		pair, ok := s.cipherKeys[trID]
		s.mu.Unlock()
		if !ok {
			log.Printf("WARN hermetix kis stream: 암호화 프레임(%s)인데 복호화 키가 없다 — 구독 응답 전 프레임?", trID)
			return
		}
		plain, err := KisDecrypt(body, pair[0], pair[1])
		if err != nil {
			log.Printf("WARN hermetix kis stream: 복호화 실패(%s) - %v", trID, err)
			return
		}
		body = plain
	}
	frame := "0|" + trID + "|" + parts[2] + "|" + body
	today := time.Now().In(kst)
	switch trID {
	case kisTrTrade:
		for _, tick := range ParseKisFrame(frame, today) {
			s.deliver(tick)
		}
	case kisTrOrderBook:
		for _, tick := range ParseKisOrderBook(frame, today) {
			s.deliverBook(tick)
		}
	case kisTrOrderEventsPaper, kisTrOrderEventsLive:
		for _, event := range ParseKisOrderEvents(frame, today) {
			s.deliverOrderEvent(event)
		}
	}
}

func (s *KisMarketStream) deliver(tick TradeTick) {
	code := tick.Symbol
	s.mu.Lock()
	symbol, ok := s.requestedSymbols[code]
	listeners := append([]TradeListener(nil), s.listeners[code]...)
	s.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	for _, listener := range listeners {
		safeCall("kis", tick.Symbol, func() { listener(tick) })
	}
}

func (s *KisMarketStream) deliverBook(tick OrderBookTick) {
	code := tick.Symbol
	s.mu.Lock()
	symbol, ok := s.requestedSymbols[code]
	listeners := append([]OrderBookListener(nil), s.bookListeners[code]...)
	s.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	for _, listener := range listeners {
		safeCall("kis", tick.Symbol, func() { listener(tick) })
	}
}

func (s *KisMarketStream) deliverOrderEvent(event OrderEvent) {
	s.mu.Lock()
	listeners := append([]OrderEventListener(nil), s.orderListeners...)
	s.mu.Unlock()
	for _, listener := range listeners {
		safeCall("kis", event.OrderID, func() { listener(event) })
	}
}

// safeCall - 리스너 panic 이 스트림을 끊지 않도록.
func safeCall(name, label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("ERROR hermetix %s stream: 리스너 오류 / %s - %v", name, label, r)
		}
	}()
	fn()
}

func (s *KisMarketStream) subscribeMessage(key, trID, trKey string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"approval_key": key, "custtype": s.custtype, "tr_type": "1", "content-type": "utf-8"},
		"body":   map[string]any{"input": map[string]string{"tr_id": trID, "tr_key": trKey}},
	})
	return string(raw)
}

// kisRecords - 0|TR|건수|본문 을 레코드(필드 목록)로 나눈다. TR 불일치·필드 부족이면 nil.
func kisRecords(frame, trID string, minFields int) [][]string {
	parts := strings.SplitN(frame, "|", 4)
	if len(parts) < 4 || parts[1] != trID {
		return nil
	}
	count, err := strconv.Atoi(parts[2])
	if err != nil || count <= 0 {
		count = 1
	}
	fields := strings.Split(parts[3], "^")
	width := len(fields) / count
	if width < minFields {
		return nil
	}
	records := make([][]string, 0, count)
	for i := 0; i < count; i++ {
		records = append(records, fields[i*width:(i+1)*width])
	}
	return records
}

// ParseKisFrame - 체결가 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 deliver 가 한다).
// 알 수 없는 TR·필드 부족은 빈 목록. today 의 날짜(KST)에 HHMMSS 를 붙여 시각을 만든다.
func ParseKisFrame(frame string, today time.Time) []TradeTick {
	records := kisRecords(frame, kisTrTrade, kisMinFields)
	ticks := make([]TradeTick, 0, len(records))
	for _, f := range records {
		if tick, ok := parseKisRecord(f, today); ok {
			ticks = append(ticks, tick)
		}
	}
	return ticks
}

func parseKisRecord(f []string, today time.Time) (TradeTick, bool) {
	price, err := decimal.NewFromString(f[2])
	if err != nil {
		return TradeTick{}, false
	}
	quantity, err := decimal.NewFromString(f[12])
	if err != nil {
		return TradeTick{}, false
	}
	timestamp, ok := kstTimeOfDay(today, f[1])
	if !ok {
		return TradeTick{}, false
	}
	tick := TradeTick{
		Symbol: f[0], Price: price, Quantity: quantity, Timestamp: timestamp,
		AskPrice: dOrNil(f[10]), BidPrice: dOrNil(f[11]),
	}
	if volume, err := strconv.ParseInt(strings.TrimSpace(f[13]), 10, 64); err == nil {
		tick.CumulativeVolume = &volume
	}
	// 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다
	if change := dOrNil(f[4]); change != nil {
		if (f[3] == "4" || f[3] == "5") && change.IsPositive() {
			neg := change.Neg()
			change = &neg
		}
		tick.Change = change
	}
	if rate := dOrNil(f[5]); rate != nil {
		converted := rate.Div(decimal.NewFromInt(100))
		tick.ChangeRate = &converted
	}
	return tick, true
}

// ParseKisOrderBook - 호가 프레임(H0STASP0) → 호가창 목록. 0 호가는 빈 단계로 보고 뺀다.
func ParseKisOrderBook(frame string, today time.Time) []OrderBookTick {
	records := kisRecords(frame, kisTrOrderBook, kisMinBookFields)
	ticks := make([]OrderBookTick, 0, len(records))
	for _, f := range records {
		timestamp, ok := kstTimeOfDay(today, f[1])
		if !ok {
			continue
		}
		levels := func(priceFrom, qtyFrom int) []OrderBookLevel {
			out := make([]OrderBookLevel, 0, 10)
			for i := 0; i < 10; i++ {
				price := dOrNil(f[priceFrom+i])
				if price == nil || price.IsZero() {
					continue
				}
				quantity := decimal.Zero
				if q := dOrNil(f[qtyFrom+i]); q != nil {
					quantity = *q
				}
				out = append(out, OrderBookLevel{Price: *price, Quantity: quantity})
			}
			return out
		}
		ticks = append(ticks, OrderBookTick{
			Symbol: f[0], Timestamp: timestamp,
			Asks: levels(3, 23), Bids: levels(13, 33),
			TotalAskQuantity: dOrNil(f[43]), TotalBidQuantity: dOrNil(f[44]),
		})
	}
	return ticks
}

// ParseKisOrderEvents - 주문 통보 프레임(복호화된 H0STCNI9/H0STCNI0 평문) → 이벤트 목록.
func ParseKisOrderEvents(frame string, today time.Time) []OrderEvent {
	parts := strings.SplitN(frame, "|", 3)
	if len(parts) < 2 || (parts[1] != kisTrOrderEventsPaper && parts[1] != kisTrOrderEventsLive) {
		return nil
	}
	records := kisRecords(frame, parts[1], kisMinOrderFields)
	events := make([]OrderEvent, 0, len(records))
	for _, f := range records {
		timestamp, ok := kstTimeOfDay(today, f[11])
		if !ok {
			continue
		}
		rejected := f[12] == "1"
		filled := f[13] == "2"
		eventType := OrderAccepted
		switch {
		case rejected:
			eventType = OrderRejected
		case filled:
			eventType = OrderFilled
		case f[5] == "2":
			eventType = OrderCanceled
		case f[5] == "1":
			eventType = OrderModified
		}
		event := OrderEvent{OrderID: strings.TrimSpace(f[2]), Type: eventType, Timestamp: timestamp, Symbol: strings.TrimSpace(f[8])}
		if filled {
			event.Quantity, event.Price = dOrNil(f[9]), dOrNil(f[10])
		} else {
			event.Quantity = dOrNil(f[16])
			if len(f) > 25 {
				event.Price = dOrNil(f[25])
			}
		}
		switch f[4] {
		case "01":
			side := Sell
			event.Side = &side
		case "02":
			side := Buy
			event.Side = &side
		}
		if original := strings.TrimSpace(f[3]); original != "" && strings.TrimLeft(original, "0") != "" && original != f[2] {
			event.OriginalOrderID = original
		}
		events = append(events, event)
	}
	return events
}

// KisDecrypt - KIS 암호화 본문: AES-256-CBC, PKCS7, base64. key 32자·iv 16자는 구독 응답 output 에서 온다.
func KisDecrypt(b64, key, iv string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	if len(data) == 0 || len(data)%block.BlockSize() != 0 || len(iv) != block.BlockSize() {
		return "", fmt.Errorf("암호문 길이(%d)/iv 길이(%d)가 블록 크기와 맞지 않는다", len(data), len(iv))
	}
	plain := make([]byte, len(data))
	cipher.NewCBCDecrypter(block, []byte(iv)).CryptBlocks(plain, data)
	pad := int(plain[len(plain)-1])
	if pad <= 0 || pad > block.BlockSize() || pad > len(plain) {
		return "", errors.New("PKCS7 패딩이 잘못됐다")
	}
	return string(plain[:len(plain)-pad]), nil
}

// KisEncrypt - 테스트·픽스처 생성용 (KisDecrypt 의 역).
func KisEncrypt(plain, key, iv string) (string, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}
	if len(iv) != block.BlockSize() {
		return "", fmt.Errorf("iv 길이(%d)가 블록 크기와 다르다", len(iv))
	}
	pad := block.BlockSize() - len(plain)%block.BlockSize()
	data := append([]byte(plain), bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, len(data))
	cipher.NewCBCEncrypter(block, []byte(iv)).CryptBlocks(out, data)
	return base64.StdEncoding.EncodeToString(out), nil
}

// kstTimeOfDay - HHMMSS(KST) 를 today 의 날짜에 붙인다.
func kstTimeOfDay(today time.Time, hhmmss string) (time.Time, bool) {
	text := strings.TrimSpace(hhmmss)
	for len(text) < 6 {
		text = "0" + text
	}
	clock, err := time.ParseInLocation("150405", text, kst)
	if err != nil {
		return time.Time{}, false
	}
	day := today.In(kst)
	return time.Date(day.Year(), day.Month(), day.Day(), clock.Hour(), clock.Minute(), clock.Second(), 0, kst), true
}
