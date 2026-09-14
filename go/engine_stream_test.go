package hermetix

// ON_TRADE 트리거 — 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지.

import (
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
	closed bool
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
