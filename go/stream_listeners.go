package hermetix

// symbolListeners - 종목 단위 리스너 등록부. 새 브로커 스트림(nh·db·ls·toss)이 공유한다.
// 코드(또는 topic 키) → 리스너 목록, 코드 → 구독 요청 표기(KRX:005930 으로 구독하면 그대로 돌려준다).

import (
	"log"
	"strings"
	"sync"

	"github.com/shopspring/decimal"
)

type symbolListeners struct {
	mu        sync.Mutex
	trades    map[string][]TradeListener
	books     map[string][]OrderBookListener
	orders    []OrderEventListener
	requested map[string]string
	keyOf     func(symbol string) string
	usage     *BrokerUsage // 텔레메트리 (nil 이면 세지 않는다)
}

func (sl *symbolListeners) countSubscribed(ch StreamChannel, n int) {
	if sl.usage != nil {
		sl.usage.StreamSubscribed(ch, n)
	}
}

func (sl *symbolListeners) countMessage(ch StreamChannel) {
	if sl.usage != nil {
		sl.usage.StreamMessage(ch, 1)
	}
}

func newSymbolListeners(keyOf func(symbol string) string) *symbolListeners {
	if keyOf == nil {
		keyOf = SymbolCode
	}
	return &symbolListeners{
		trades: map[string][]TradeListener{}, books: map[string][]OrderBookListener{},
		requested: map[string]string{}, keyOf: keyOf,
	}
}

func addListeners[L any](sl *symbolListeners, symbols []string, listener L, target map[string][]L) []string {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	newKeys := make([]string, 0)
	for _, symbol := range symbols {
		key := sl.keyOf(symbol)
		if _, ok := sl.requested[key]; !ok {
			sl.requested[key] = symbol
		}
		if _, ok := target[key]; !ok {
			newKeys = append(newKeys, key)
		}
		target[key] = append(target[key], listener)
	}
	return newKeys
}

// addTrades - 새로 구독해야 하는 키 목록을 돌려준다.
func (sl *symbolListeners) addTrades(symbols []string, l TradeListener) []string {
	newKeys := addListeners(sl, symbols, l, sl.trades)
	sl.countSubscribed(StreamTrades, len(newKeys))
	return newKeys
}

func (sl *symbolListeners) addBooks(symbols []string, l OrderBookListener) []string {
	newKeys := addListeners(sl, symbols, l, sl.books)
	sl.countSubscribed(StreamOrderBook, len(newKeys))
	return newKeys
}

// addOrders - 첫 리스너면 true (구독 메시지를 보내야 한다).
func (sl *symbolListeners) addOrders(l OrderEventListener) bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	first := len(sl.orders) == 0
	sl.orders = append(sl.orders, l)
	if first {
		sl.countSubscribed(StreamOrderEvents, 1)
	}
	return first
}

func (sl *symbolListeners) tradeKeys() []string {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return keys(sl.trades)
}

func (sl *symbolListeners) bookKeys() []string {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return keys(sl.books)
}

func (sl *symbolListeners) hasOrders() bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	return len(sl.orders) > 0
}

func (sl *symbolListeners) removeTrades(keysToDrop []string) []string {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	dropped := make([]string, 0)
	for _, k := range keysToDrop {
		if _, ok := sl.trades[k]; ok {
			delete(sl.trades, k)
			dropped = append(dropped, k)
		}
	}
	return dropped
}

func (sl *symbolListeners) removeBooks(keysToDrop []string) []string {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	dropped := make([]string, 0)
	for _, k := range keysToDrop {
		if _, ok := sl.books[k]; ok {
			delete(sl.books, k)
			dropped = append(dropped, k)
		}
	}
	return dropped
}

func (sl *symbolListeners) clearOrders() bool {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	had := len(sl.orders) > 0
	sl.orders = nil
	return had
}

// deliverTrade - 키로 리스너를 찾아 요청 표기 심볼로 전달한다.
func (sl *symbolListeners) deliverTrade(name, key string, tick TradeTick) {
	sl.mu.Lock()
	symbol, ok := sl.requested[key]
	listeners := append([]TradeListener(nil), sl.trades[key]...)
	sl.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	sl.countMessage(StreamTrades)
	for _, listener := range listeners {
		safeCall(name, tick.Symbol, func() { listener(tick) })
	}
}

func (sl *symbolListeners) deliverBook(name, key string, tick OrderBookTick) {
	sl.mu.Lock()
	symbol, ok := sl.requested[key]
	listeners := append([]OrderBookListener(nil), sl.books[key]...)
	sl.mu.Unlock()
	if ok {
		tick.Symbol = symbol
	}
	sl.countMessage(StreamOrderBook)
	for _, listener := range listeners {
		safeCall(name, tick.Symbol, func() { listener(tick) })
	}
}

func (sl *symbolListeners) deliverOrder(name string, event OrderEvent) {
	sl.mu.Lock()
	listeners := append([]OrderEventListener(nil), sl.orders...)
	sl.mu.Unlock()
	sl.countMessage(StreamOrderEvents)
	for _, listener := range listeners {
		safeCall(name, event.OrderID, func() { listener(event) })
	}
}

// jsonFrame - {"header":{…},"body":{…}} 꼴 프레임. header/body 는 null 일 수 있다.
type jsonFrame struct {
	Header map[string]any `json:"header"`
	Body   map[string]any `json:"body"`
}

func frameText(m map[string]any, field string) string {
	if m == nil {
		return ""
	}
	return strings.TrimSpace(str(m[field]))
}

// frameDecimal - 문자열/숫자 어느 쪽으로 와도 파싱 (콤마 제거). 없으면 nil.
func frameDecimal(m map[string]any, field string) *decimal.Decimal {
	if m == nil {
		return nil
	}
	return nhNum(m[field])
}

func logUnknownFrame(name, text string) {
	if len(text) > 200 {
		text = text[:200]
	}
	log.Printf("DEBUG hermetix %s stream: %s", name, text)
}
