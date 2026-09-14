package hermetix

// KIS 실시간 체결가 스트림 (TR H0STCNT0). 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과 (Kotlin 레퍼런스와 동일 프로토콜).
//
//   - 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
//   - 인증: REST POST /oauth2/Approval 로 받은 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 재발급)
//   - 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"H0STCNT0","tr_key":"005930"}}}
//   - 데이터 프레임: 0|H0STCNT0|<건수>|<필드^필드^…> — 레코드 폭 47, 한 프레임에 최대 3건이 이어 붙는다 (폭 = 전체 필드 수 / 건수)
//   - 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd), PINGPONG(그대로 되돌려 보내야 연결 유지)
//
// 필드 순서(0부터): 0 단축코드, 1 체결시각 HHMMSS, 2 현재가, 3 전일대비부호, 4 전일대비(부호 포함), 5 전일대비율(%),
// 6 가중평균가, 7 시가, 8 고가, 9 저가, 10 매도호가1, 11 매수호가1, 12 체결거래량, 13 누적거래량, …
// 실측 프레임은 conformance/fixtures/kis.json#stream.

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
	KisWsPaperURL = "ws://ops.koreainvestment.com:31000"
	KisWsLiveURL  = "ws://ops.koreainvestment.com:21000"
	kisTrTrade    = "H0STCNT0"
	kisMinFields  = 14
)

type KisMarketStream struct {
	*reconnectingWebSocket
	wsURL       string
	custtype    string
	approvalKey func() (string, error)

	mu               sync.Mutex
	listeners        map[string][]TradeListener // 종목코드 → 리스너
	requestedSymbols map[string]string          // 종목코드 → 구독 요청 표기
}

func newKisMarketStream(wsURL, custtype string, approvalKey func() (string, error)) *KisMarketStream {
	s := &KisMarketStream{
		wsURL: wsURL, custtype: custtype, approvalKey: approvalKey,
		listeners: map[string][]TradeListener{}, requestedSymbols: map[string]string{},
	}
	s.reconnectingWebSocket = newReconnectingWebSocket("kis", s)
	return s
}

func (s *KisMarketStream) IsConnected() bool { return s.IsSocketOpen() }

func (s *KisMarketStream) URI() string { return s.wsURL }

func (s *KisMarketStream) OnOpen() {
	key, err := s.approvalKey()
	if err != nil {
		log.Printf("ERROR hermetix kis stream: 접속키 발급 실패 - %v", err)
		return
	}
	for _, code := range s.codes() {
		s.Send(s.subscribeMessage(key, code))
	}
}

func (s *KisMarketStream) OnDisconnected() {}

func (s *KisMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
	newCodes := make([]string, 0)
	s.mu.Lock()
	for _, symbol := range symbols {
		code := SymbolCode(symbol)
		if _, ok := s.requestedSymbols[code]; !ok {
			s.requestedSymbols[code] = symbol
		}
		if _, ok := s.listeners[code]; !ok {
			newCodes = append(newCodes, code)
		}
		s.listeners[code] = append(s.listeners[code], listener)
	}
	s.mu.Unlock()
	if len(newCodes) > 0 && s.IsSocketOpen() {
		key, err := s.approvalKey()
		if err != nil {
			log.Printf("ERROR hermetix kis stream: 접속키 발급 실패 - %v", err)
			return
		}
		for _, code := range newCodes {
			s.Send(s.subscribeMessage(key, code))
		}
	}
}

func (s *KisMarketStream) OnMessage(text string) {
	if strings.HasPrefix(text, "0|") || strings.HasPrefix(text, "1|") {
		for _, tick := range ParseKisFrame(text, time.Now().In(kst)) {
			s.deliver(tick)
		}
		return
	}
	var node struct {
		Header struct {
			TrID  string `json:"tr_id"`
			TrKey string `json:"tr_key"`
		} `json:"header"`
		Body *struct {
			RtCd  string `json:"rt_cd"`
			MsgCd string `json:"msg_cd"`
			Msg1  string `json:"msg1"`
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
	switch {
	case node.Body.RtCd == "0" || node.Body.MsgCd == "OPSP0000":
		log.Printf("INFO hermetix kis stream: subscribed %s %s (%s)", node.Header.TrID, node.Header.TrKey, node.Body.Msg1)
	case node.Body.MsgCd == "OPSP0002":
		log.Printf("INFO hermetix kis stream: already subscribed %s %s", node.Header.TrID, node.Header.TrKey)
	default:
		log.Printf("WARN hermetix kis stream: %s %s rt_cd=%s msg_cd=%s %s", node.Header.TrID, node.Header.TrKey, node.Body.RtCd, node.Body.MsgCd, node.Body.Msg1)
	}
}

func (s *KisMarketStream) codes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	codes := make([]string, 0, len(s.listeners))
	for code := range s.listeners {
		codes = append(codes, code)
	}
	return codes
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
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("ERROR hermetix kis stream: 리스너 오류 / %s - %v", tick.Symbol, r)
				}
			}()
			listener(tick)
		}()
	}
}

func (s *KisMarketStream) subscribeMessage(key, code string) string {
	raw, _ := json.Marshal(map[string]any{
		"header": map[string]string{"approval_key": key, "custtype": s.custtype, "tr_type": "1", "content-type": "utf-8"},
		"body":   map[string]any{"input": map[string]string{"tr_id": kisTrTrade, "tr_key": code}},
	})
	return string(raw)
}

// ParseKisFrame - 데이터 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 deliver 가 한다).
// 알 수 없는 TR·필드 부족은 빈 목록. today 의 날짜(KST)에 HHMMSS 를 붙여 시각을 만든다.
func ParseKisFrame(frame string, today time.Time) []TradeTick {
	parts := strings.SplitN(frame, "|", 4)
	if len(parts) < 4 || parts[1] != kisTrTrade {
		return nil
	}
	count, err := strconv.Atoi(parts[2])
	if err != nil || count <= 0 {
		count = 1
	}
	fields := strings.Split(parts[3], "^")
	width := len(fields) / count
	if width < kisMinFields {
		return nil
	}
	ticks := make([]TradeTick, 0, count)
	for i := 0; i < count; i++ {
		if tick, ok := parseKisRecord(fields[i*width:(i+1)*width], today); ok {
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
