package hermetix

// 키움 REST API 실시간 체결 스트림 (실시간 타입 0B 주식체결). 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과.
//
//   - 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/…
//   - 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} → {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
//   - 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]} → {"trnm":"REG","return_code":0}
//   - 데이터: {"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"005930","values":{"20":체결시각,"10":현재가,…}}]} (data 키가 trnm 보다 앞에 온다)
//   - {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다
//
// values 의 FID: 20 체결시각 HHMMSS, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가,
// 15 거래량(+매수/-매도 체결), 13 누적거래량. REST 와 같이 가격·호가·체결량에 등락 부호가 붙으므로 절대값으로 파싱한다.
// 실측 프레임은 conformance/fixtures/kiwoom.json#stream.

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

const (
	KiwoomWsPaperURL = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket"
	KiwoomWsLiveURL  = "wss://api.kiwoom.com:10000/api/dostk/websocket"
	kiwoomTypeTrade  = "0B"
)

type KiwoomMarketStream struct {
	*reconnectingWebSocket
	wsURL string
	token func() (string, error)

	mu               sync.Mutex
	loggedIn         bool
	listeners        map[string][]TradeListener
	requestedSymbols map[string]string
}

func newKiwoomMarketStream(wsURL string, token func() (string, error)) *KiwoomMarketStream {
	s := &KiwoomMarketStream{
		wsURL: wsURL, token: token,
		listeners: map[string][]TradeListener{}, requestedSymbols: map[string]string{},
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

func (s *KiwoomMarketStream) SubscribeTrades(symbols []string, listener TradeListener) {
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
	if len(newCodes) > 0 && s.IsConnected() {
		s.Send(s.registerMessage(newCodes))
	}
}

func (s *KiwoomMarketStream) OnMessage(text string) {
	var node struct {
		Trnm       string          `json:"trnm"`
		ReturnCode *int            `json:"return_code"`
		ReturnMsg  string          `json:"return_msg"`
		Data       json.RawMessage `json:"data"`
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
			if codes := s.codes(); len(codes) > 0 {
				s.Send(s.registerMessage(codes))
			}
		} else {
			log.Printf("ERROR hermetix kiwoom stream: 로그인 실패 return_code=%d %s", returnCode, node.ReturnMsg)
		}
	case "REG":
		if returnCode == 0 {
			log.Printf("INFO hermetix kiwoom stream: registered %v", s.codes())
		} else {
			log.Printf("WARN hermetix kiwoom stream: 등록 실패 return_code=%d %s", returnCode, node.ReturnMsg)
		}
	case "REAL":
		for _, tick := range ParseKiwoomReal([]byte(text), time.Now().In(kst)) {
			s.deliver(tick)
		}
	}
}

func (s *KiwoomMarketStream) codes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	codes := make([]string, 0, len(s.listeners))
	for code := range s.listeners {
		codes = append(codes, code)
	}
	return codes
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
	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("ERROR hermetix kiwoom stream: 리스너 오류 / %s - %v", tick.Symbol, r)
				}
			}()
			listener(tick)
		}()
	}
}

func (s *KiwoomMarketStream) registerMessage(codes []string) string {
	raw, _ := json.Marshal(map[string]any{
		"trnm": "REG", "grp_no": "1", "refresh": "1",
		"data": []map[string]any{{"item": codes, "type": []string{kiwoomTypeTrade}}},
	})
	return string(raw)
}

// ParseKiwoomReal - REAL 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (A 프리픽스 제거).
func ParseKiwoomReal(raw []byte, today time.Time) []TradeTick {
	var frame struct {
		Data []struct {
			Type   string            `json:"type"`
			Item   string            `json:"item"`
			Values map[string]string `json:"values"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil
	}
	ticks := make([]TradeTick, 0, len(frame.Data))
	for _, item := range frame.Data {
		if item.Type != kiwoomTypeTrade {
			continue
		}
		signed := func(fid string) *decimal.Decimal { return dOrNil(item.Values[fid]) }
		abs := func(fid string) *decimal.Decimal {
			v := signed(fid)
			if v == nil {
				return nil
			}
			a := v.Abs()
			return &a
		}
		price := abs("10")
		if price == nil {
			continue
		}
		timestamp, ok := kstTimeOfDay(today, item.Values["20"])
		if !ok {
			continue
		}
		tick := TradeTick{
			Symbol:    strings.TrimPrefix(strings.TrimSpace(item.Item), "A"),
			Price:     *price,
			Quantity:  decimal.Zero,
			Timestamp: timestamp,
			AskPrice:  abs("27"),
			BidPrice:  abs("28"),
			Change:    signed("11"),
		}
		if qty := abs("15"); qty != nil {
			tick.Quantity = *qty
		}
		if volume := abs("13"); volume != nil {
			v := volume.IntPart()
			tick.CumulativeVolume = &v
		}
		if rate := signed("12"); rate != nil {
			converted := rate.Div(decimal.NewFromInt(100))
			tick.ChangeRate = &converted
		}
		ticks = append(ticks, tick)
	}
	return ticks
}
