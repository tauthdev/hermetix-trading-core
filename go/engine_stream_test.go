package hermetix

// ON_TRADE 트리거 — 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// fakeStream - 연결 없이 틱을 밀어 넣을 수 있는 가짜 스트림.
type fakeStream struct {
	mu        sync.Mutex
	listeners []struct {
		symbols  []string
		listener TradeListener
	}
	bookListeners []struct {
		symbols  []string
		listener OrderBookListener
	}
	orderListeners []OrderEventListener
	orderEventsErr error // SubscribeOrderEvents 가 돌려줄 오류 (KIS HTS ID 미설정 흉내)
	closed         bool
}

func (s *fakeStream) IsConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed
}
func (s *fakeStream) Connect() {}
func (s *fakeStream) SubscribeTrades(symbols []string, listener TradeListener) {
	s.mu.Lock()
	s.listeners = append(s.listeners, struct {
		symbols  []string
		listener TradeListener
	}{symbols, listener})
	s.mu.Unlock()
}
func (s *fakeStream) SubscribeOrderBook(symbols []string, listener OrderBookListener) error {
	s.mu.Lock()
	s.bookListeners = append(s.bookListeners, struct {
		symbols  []string
		listener OrderBookListener
	}{symbols, listener})
	s.mu.Unlock()
	return nil
}
func (s *fakeStream) SubscribeOrderEvents(listener OrderEventListener) error {
	if s.orderEventsErr != nil {
		return s.orderEventsErr
	}
	s.mu.Lock()
	s.orderListeners = append(s.orderListeners, listener)
	s.mu.Unlock()
	return nil
}
func (s *fakeStream) emitBook(symbol, ask, bid string) {
	a, _ := decimal.NewFromString(ask)
	b, _ := decimal.NewFromString(bid)
	tick := OrderBookTick{Symbol: symbol, Timestamp: time.Now(),
		Asks: []OrderBookLevel{{Price: a, Quantity: decimal.NewFromInt(10)}}, Bids: []OrderBookLevel{{Price: b, Quantity: decimal.NewFromInt(10)}}}
	s.mu.Lock()
	targets := append([]struct {
		symbols  []string
		listener OrderBookListener
	}(nil), s.bookListeners...)
	s.mu.Unlock()
	for _, l := range targets {
		for _, sym := range l.symbols {
			if sym == symbol {
				l.listener(tick)
			}
		}
	}
}
func (s *fakeStream) emitOrderEvent(event OrderEvent) {
	s.mu.Lock()
	targets := append([]OrderEventListener(nil), s.orderListeners...)
	s.mu.Unlock()
	for _, l := range targets {
		l(event)
	}
}
func (s *fakeStream) orderListenerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orderListeners)
}
func (s *fakeStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *fakeStream) emit(symbol, price string) {
	p, _ := decimal.NewFromString(price)
	volume := int64(10)
	tick := TradeTick{Symbol: symbol, Price: p, Quantity: decimal.NewFromInt(1), Timestamp: time.Now(), CumulativeVolume: &volume}
	s.mu.Lock()
	targets := append([]struct {
		symbols  []string
		listener TradeListener
	}(nil), s.listeners...)
	s.mu.Unlock()
	for _, l := range targets {
		for _, sym := range l.symbols {
			if sym == symbol {
				l.listener(tick)
			}
		}
	}
}

// streamingFakeBroker - 스트림을 제공하고 GetQuotes 호출을 센다.
type streamingFakeBroker struct {
	*fakeBroker
	stream     *fakeStream
	quoteCalls atomic.Int32
	streams    []StreamChannel
	appliedMu  sync.Mutex
	applied    []OrderEvent
}

func (b *streamingFakeBroker) ApplyOrderEvent(event OrderEvent) {
	b.appliedMu.Lock()
	b.applied = append(b.applied, event)
	b.appliedMu.Unlock()
}
func (b *streamingFakeBroker) appliedCount() int {
	b.appliedMu.Lock()
	defer b.appliedMu.Unlock()
	return len(b.applied)
}

func (b *streamingFakeBroker) Capabilities() BrokerCapabilities {
	caps := b.fakeBroker.Capabilities()
	caps.Market = "KRX"
	caps.Streams = b.streams
	return caps
}
func (b *streamingFakeBroker) OpenStream() MarketStream { return b.stream }
func (b *streamingFakeBroker) GetQuotes(symbols []string) ([]Quote, error) {
	b.quoteCalls.Add(1)
	quotes := make([]Quote, 0, len(symbols))
	for _, s := range symbols {
		quotes = append(quotes, Quote{Symbol: s, Price: decimal.NewFromInt(1), Timestamp: time.Now()})
	}
	return quotes, nil
}

type recordingStrategy struct {
	spec  StrategySpec
	mu    sync.Mutex
	calls []struct {
		at  time.Time
		ctx *StrategyContext
	}
}

func (s *recordingStrategy) Spec() StrategySpec { return s.spec }
func (s *recordingStrategy) Decide(ctx *StrategyContext) ([]Signal, error) {
	s.mu.Lock()
	s.calls = append(s.calls, struct {
		at  time.Time
		ctx *StrategyContext
	}{time.Now(), ctx})
	s.mu.Unlock()
	return nil, nil
}
func (s *recordingStrategy) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}
func (s *recordingStrategy) call(i int) (time.Time, *StrategyContext) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[i].at, s.calls[i].ctx
}

func onTradeSpec(symbols []string, minInterval time.Duration) StrategySpec {
	off := false
	return StrategySpec{
		Name: "s", Symbols: symbols, PollInterval: time.Hour, RegularHoursOnly: &off,
		Trigger: TriggerOnTrade, MinTickInterval: minInterval,
	}
}

func newStreamingBroker(streams []StreamChannel) *streamingFakeBroker {
	return &streamingFakeBroker{fakeBroker: newFakeBroker(), stream: &fakeStream{}, streams: streams}
}

// startEngine - Run 을 고루틴으로 돌리고 정리를 등록한다.
func startEngine(t *testing.T, engine *StrategyEngine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		engine.Run()
		close(done)
	}()
	t.Cleanup(func() {
		engine.Stop()
		<-done
	})
}

func awaitCalls(t *testing.T, strategy *recordingStrategy, atLeast int, timeout time.Duration) {
	t.Helper()
	waitFor(t, func() bool { return strategy.count() >= atLeast }, timeout, "전략 호출 수 부족")
}

func TestStreamTickTriggersDecideWithStreamQuote(t *testing.T) {
	broker := newStreamingBroker([]StreamChannel{StreamTrades})
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, 300*time.Millisecond)}
	engine := NewStrategyEngine(broker, []Strategy{strategy})
	startEngine(t, engine)
	awaitCalls(t, strategy, 1, 3*time.Second) // 기동 직후 폴링 틱 — 스트림 틱이 없어 REST 현재가

	broker.stream.emit("005930", "71500")
	awaitCalls(t, strategy, 2, 3*time.Second)
	_, ctx := strategy.call(1)
	q, ok := ctx.Quote("005930")
	if !ok || q.Price.String() != "71500" {
		t.Fatalf("quote = %+v", q)
	}
	if n := broker.quoteCalls.Load(); n != 1 {
		t.Fatalf("GetQuotes 호출 = %d (기동 폴링 1회뿐이어야 한다)", n)
	}
	if !engine.StreamConnected() {
		t.Fatal("stream connected")
	}
}

func TestStreamTicksCoalesceAndRespectMinInterval(t *testing.T) {
	interval := 400 * time.Millisecond
	broker := newStreamingBroker([]StreamChannel{StreamTrades})
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, interval)}
	startEngine(t, NewStrategyEngine(broker, []Strategy{strategy}))
	awaitCalls(t, strategy, 1, 3*time.Second) // 기동 폴링 틱
	time.Sleep(450 * time.Millisecond)        // 최소 간격을 넘겨 다음 스트림 틱이 즉시 돌 수 있게

	for i := 0; i < 20; i++ {
		broker.stream.emit("005930", "71500")
	}
	time.Sleep(1500 * time.Millisecond)
	afterBurst := strategy.count() // 20개 → 즉시 1회 + (tick 도중 도착 분) 최소 간격 뒤 1회. 20회가 아니다
	if afterBurst < 2 || afterBurst > 3 {
		t.Fatalf("burst 후 호출 = %d", afterBurst)
	}

	broker.stream.emit("005930", "72000")
	awaitCalls(t, strategy, afterBurst+1, 3*time.Second)
	time.Sleep(300 * time.Millisecond)
	if strategy.count() != afterBurst+1 {
		t.Fatalf("호출 = %d, expected %d", strategy.count(), afterBurst+1)
	}
	// 연속 tick 사이 간격은 항상 MinTickInterval 이상 (여유 10%)
	for i := 1; i < strategy.count(); i++ {
		prev, _ := strategy.call(i - 1)
		cur, _ := strategy.call(i)
		if gap := cur.Sub(prev); gap < interval*9/10 {
			t.Fatalf("gap[%d] = %s < %s", i, gap, interval)
		}
	}
}

func TestStreamPartialCoverageFallsBackToRestQuotes(t *testing.T) {
	broker := newStreamingBroker([]StreamChannel{StreamTrades})
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930", "000660"}, 300*time.Millisecond)}
	startEngine(t, NewStrategyEngine(broker, []Strategy{strategy}))
	awaitCalls(t, strategy, 1, 3*time.Second)

	broker.stream.emit("005930", "71500") // 000660 틱이 없으므로 스트림 틱도 REST
	awaitCalls(t, strategy, 2, 3*time.Second)
	if n := broker.quoteCalls.Load(); n != 2 {
		t.Fatalf("GetQuotes 호출 = %d", n)
	}
}

func TestOnTradeWithoutStreamSupportStillSchedules(t *testing.T) {
	broker := newStreamingBroker(nil) // Streams 미선언
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, 300*time.Millisecond)}
	engine := NewStrategyEngine(broker, []Strategy{strategy})
	if len(engine.Strategies) != 1 {
		t.Fatalf("strategies = %d", len(engine.Strategies))
	}
	startEngine(t, engine)
	awaitCalls(t, strategy, 1, 3*time.Second)
	if engine.StreamConnected() {
		t.Fatal("스트림이 붙으면 안 된다")
	}
	if len(broker.stream.listeners) != 0 {
		t.Fatal("구독이 없어야 한다")
	}
}

func TestStopClosesStream(t *testing.T) {
	broker := newStreamingBroker([]StreamChannel{StreamTrades})
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, 300*time.Millisecond)}
	engine := NewStrategyEngine(broker, []Strategy{strategy})
	done := make(chan struct{})
	go func() {
		engine.Run()
		close(done)
	}()
	waitFor(t, engine.StreamConnected, 3*time.Second, "stream connected")
	engine.Stop()
	<-done
	if !broker.stream.closed {
		t.Fatal("Stop 은 스트림을 닫아야 한다")
	}
}

var allStreams = []StreamChannel{StreamTrades, StreamOrderBook, StreamOrderEvents}

func TestOrderBookSpecSubscribesAndFillsContext(t *testing.T) {
	broker := newStreamingBroker(allStreams)
	spec := onTradeSpec([]string{"005930"}, 300*time.Millisecond)
	spec.OrderBook = true
	strategy := &recordingStrategy{spec: spec}
	startEngine(t, NewStrategyEngine(broker, []Strategy{strategy}))
	awaitCalls(t, strategy, 1, 3*time.Second)
	if len(broker.stream.bookListeners) != 1 || broker.stream.bookListeners[0].symbols[0] != "005930" {
		t.Fatalf("book listeners = %+v", broker.stream.bookListeners)
	}
	_, ctx := strategy.call(0)
	if _, ok := ctx.OrderBook("005930"); ok {
		t.Fatal("아직 호가가 없어야 한다")
	}

	broker.stream.emitBook("005930", "250500", "250000")
	broker.stream.emit("005930", "250500")
	awaitCalls(t, strategy, 2, 3*time.Second)
	_, ctx = strategy.call(1)
	book, ok := ctx.OrderBook("KRX:005930") // 접두 무시 조회
	if !ok {
		t.Fatal("호가창이 컨텍스트에 있어야 한다")
	}
	ask, _ := book.BestAsk()
	bid, _ := book.BestBid()
	if ask.Price.String() != "250500" || bid.Price.String() != "250000" {
		t.Fatalf("book = %+v", book)
	}
}

func TestOrderEventsReachApplierAndBrackets(t *testing.T) {
	broker := newStreamingBroker(allStreams)
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, 300*time.Millisecond)}
	engine := NewStrategyEngine(broker, []Strategy{strategy})
	startEngine(t, engine)
	waitFor(t, func() bool { return broker.stream.orderListenerCount() == 1 }, 3*time.Second, "주문 통보 구독")

	tp := decimal.NewFromInt(310)
	engine.Brackets.Register("12345", "AAPL", decimal.NewFromInt(2), &tp, nil)
	qty := decimal.NewFromInt(2)
	event := OrderEvent{OrderID: "0000012345", Type: OrderFilled, Timestamp: time.Now(), Quantity: &qty}
	broker.stream.emitOrderEvent(event)
	if broker.appliedCount() != 1 || broker.applied[0].OrderID != "0000012345" {
		t.Fatalf("applied = %+v", broker.applied)
	}
	// 브라켓이 통보로 활성화됐다면 GetOrder(Submitted) 조회 없이 익절 시그널이 나온다
	ctx := testCtx("311", "2")
	if signals := engine.Brackets.Check(ctx); len(signals) != 1 {
		t.Fatalf("signals = %+v (통보로 활성화돼야 한다)", signals)
	}
}

func TestOrderEventSubscribeErrorDoesNotStopEngine(t *testing.T) {
	broker := newStreamingBroker(allStreams)
	broker.stream.orderEventsErr = errors.New("HTS ID 필요")
	strategy := &recordingStrategy{spec: onTradeSpec([]string{"005930"}, 300*time.Millisecond)}
	engine := NewStrategyEngine(broker, []Strategy{strategy})
	startEngine(t, engine)
	awaitCalls(t, strategy, 1, 3*time.Second)
	if broker.stream.orderListenerCount() != 0 {
		t.Fatal("구독이 실패했으면 리스너가 없어야 한다")
	}
	broker.stream.emit("005930", "71500")
	awaitCalls(t, strategy, 2, 3*time.Second) // 체결가 트리거는 계속 동작
}

func TestBracketOnOrderEvent(t *testing.T) {
	broker := newFakeBroker()
	monitor := NewBracketMonitor(broker)
	tp := decimal.NewFromInt(310)
	one := decimal.NewFromInt(1)
	event := func(orderID string, kind OrderEventType, quantity *decimal.Decimal) OrderEvent {
		return OrderEvent{OrderID: orderID, Type: kind, Timestamp: time.Now(), Quantity: quantity}
	}

	monitor.Register("ord_1", "AAPL", decimal.NewFromInt(2), &tp, nil)
	monitor.OnOrderEvent(event("0000ord_1", OrderAccepted, nil))
	monitor.OnOrderEvent(event("0000ord_1", OrderFilled, &one)) // 부분 체결 — 아직 비활성 (GetOrder 는 Submitted)
	if signals := monitor.Check(testCtx("311", "2")); len(signals) != 0 {
		t.Fatalf("부분 체결인데 시그널 = %+v", signals)
	}
	monitor.OnOrderEvent(event("0000ord_1", OrderFilled, &one)) // 누적 2 = 주문 수량 → 활성
	if signals := monitor.Check(testCtx("311", "2")); len(signals) != 1 {
		t.Fatalf("signals = %+v", signals)
	}

	monitor.Register("ord_2", "AAPL", decimal.NewFromInt(2), &tp, nil)
	monitor.OnOrderEvent(event("ord_2", OrderCanceled, nil))
	if monitor.ActiveCount() != 0 {
		t.Fatal("취소 통보는 브라켓을 폐기한다")
	}
	monitor.Register("ord_3", "AAPL", decimal.NewFromInt(2), &tp, nil)
	monitor.OnOrderEvent(event("ord_3", OrderRejected, nil))
	if monitor.ActiveCount() != 0 {
		t.Fatal("거부 통보는 브라켓을 폐기한다")
	}
	monitor.Register("ord_4", "AAPL", decimal.NewFromInt(2), &tp, nil)
	monitor.OnOrderEvent(event("other", OrderCanceled, nil)) // 다른 주문 — 무시
	if monitor.ActiveCount() != 1 {
		t.Fatal("다른 주문의 통보는 무시")
	}
}
