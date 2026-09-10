package hermetix

// 엔진 로직 검증 - 다른 언어 구현과 동일한 시나리오.

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type fakeBroker struct {
	created     []CreateOrderRequest
	canceled    []string
	orderStatus OrderStatus
	openOrders  []Order
	environment TradingEnvironment
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{orderStatus: Submitted, environment: Paper}
}

func (f *fakeBroker) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "fake", Market: "US", Currency: "USD",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   true, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Paper: true, Live: true},
	}
}
func (f *fakeBroker) Environment() TradingEnvironment                          { return f.environment }
func (f *fakeBroker) GetQuotes([]string) ([]Quote, error)                      { return nil, nil }
func (f *fakeBroker) GetCandles(string, CandleInterval, int) ([]Candle, error) { return nil, nil }
func (f *fakeBroker) GetCalendar() ([]MarketDay, error)                        { return nil, nil }
func (f *fakeBroker) GetAccount() (Account, error)                             { return Account{}, nil }
func (f *fakeBroker) GetHoldings() ([]Holding, error)                          { return nil, nil }
func (f *fakeBroker) GetBuyingPower() (decimal.Decimal, error)                 { return decimal.Zero, nil }
func (f *fakeBroker) CreateOrder(request CreateOrderRequest) (Order, error) {
	f.created = append(f.created, request)
	return Order{OrderID: "ord_1", Status: Submitted, Symbol: request.Symbol}, nil
}
func (f *fakeBroker) GetOrders() ([]Order, error) { return f.openOrders, nil }
func (f *fakeBroker) GetOrder(orderID string) (Order, error) {
	return Order{OrderID: orderID, Status: f.orderStatus}, nil
}
func (f *fakeBroker) CancelOrder(orderID string) (Order, error) {
	f.canceled = append(f.canceled, orderID)
	return Order{OrderID: orderID, Status: Canceled}, nil
}
func (f *fakeBroker) GetFills() ([]Fill, error) { return nil, nil }

func testCtx(price string, heldQty string) *StrategyContext {
	ctx := &StrategyContext{
		Now: time.Now(), Quotes: map[string]Quote{}, Candles: map[string][]Candle{},
		Holdings: map[string]Holding{}, BuyingPower: decimal.NewFromInt(10000),
	}
	if price != "" {
		p, _ := decimal.NewFromString(price)
		ctx.Quotes["AAPL"] = Quote{Symbol: "AAPL", Price: p, Timestamp: time.Now()}
	}
	if heldQty != "" {
		q, _ := decimal.NewFromString(heldQty)
		ctx.Holdings["AAPL"] = Holding{Symbol: "AAPL", Quantity: q, AvgEntryPrice: decimal.NewFromInt(100)}
	}
	return ctx
}

func TestSellClampedToHoldings(t *testing.T) {
	broker := newFakeBroker()
	executor := NewOrderExecutor(broker, NewBracketMonitor(broker), NewTradingGuard(broker, 5))
	executor.Execute("t", []Signal{SellSignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(10)}}, testCtx("", "3"))
	if broker.created[0].Quantity.String() != "3" {
		t.Fatalf("quantity = %s", broker.created[0].Quantity)
	}
	if broker.created[0].ClientOrderID == "" {
		t.Fatal("clientOrderID should be set")
	}
}

func TestSellSkippedWithoutHoldings(t *testing.T) {
	broker := newFakeBroker()
	executor := NewOrderExecutor(broker, NewBracketMonitor(broker), NewTradingGuard(broker, 5))
	executor.Execute("t", []Signal{SellSignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(1)}}, testCtx("", ""))
	if len(broker.created) != 0 {
		t.Fatal("should not create order")
	}
}

func TestHaltedGuardBlocksSignals(t *testing.T) {
	broker := newFakeBroker()
	guard := NewTradingGuard(broker, 5)
	guard.Halt("test")
	executor := NewOrderExecutor(broker, NewBracketMonitor(broker), guard)
	executor.Execute("t", []Signal{BuySignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(1)}}, testCtx("", ""))
	if len(broker.created) != 0 {
		t.Fatal("should not create order while halted")
	}
}

func TestBracketTakeProfitFlow(t *testing.T) {
	broker := newFakeBroker()
	brackets := NewBracketMonitor(broker)
	executor := NewOrderExecutor(broker, brackets, NewTradingGuard(broker, 5))

	tp, sl := decimal.NewFromInt(310), decimal.NewFromInt(280)
	executor.Execute("t", []Signal{BuySignal{
		Symbol: "AAPL", Quantity: decimal.NewFromInt(2), TakeProfitPrice: &tp, StopLossPrice: &sl,
	}}, testCtx("", ""))
	if brackets.ActiveCount() != 1 {
		t.Fatalf("activeCount = %d", brackets.ActiveCount())
	}

	broker.orderStatus = Filled // 진입 체결
	signals := brackets.Check(testCtx("311", "2"))
	if len(signals) != 1 {
		t.Fatalf("signals = %d", len(signals))
	}
	if signals[0].(SellSignal).Quantity.String() != "2" {
		t.Fatal("wrong quantity")
	}
	if brackets.ActiveCount() != 0 {
		t.Fatal("bracket should be removed")
	}
}

func TestBracketDroppedWhenEntryCanceled(t *testing.T) {
	broker := newFakeBroker()
	brackets := NewBracketMonitor(broker)
	tp := decimal.NewFromInt(310)
	brackets.Register("ord_1", "AAPL", decimal.NewFromInt(2), &tp, nil)
	broker.orderStatus = Canceled
	if signals := brackets.Check(testCtx("999", "")); len(signals) != 0 {
		t.Fatal("should not signal")
	}
	if brackets.ActiveCount() != 0 {
		t.Fatal("bracket should be dropped")
	}
}

func TestGuardHaltsAfterConsecutiveFailures(t *testing.T) {
	broker := newFakeBroker()
	broker.openOrders = []Order{{OrderID: "ord_9", Status: Submitted}}
	guard := NewTradingGuard(broker, 3)
	for i := 0; i < 3; i++ {
		guard.RecordFailure(&BrokerAPIError{500, "", "boom"})
	}
	if !guard.IsHalted() {
		t.Fatal("should be halted")
	}
	if len(broker.canceled) != 1 || broker.canceled[0] != "ord_9" {
		t.Fatalf("canceled = %v", broker.canceled)
	}
}

// ------------------------------------------------------------- 0.6.0: 위험 상한 · 실전 게이트 · 심볼 접두

func dec(s string) *decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return &v
}

func TestRiskGuardLimits(t *testing.T) {
	guard := NewRiskGuard(dec("1000"), nil)
	if guard.TryReserve("AAPL", decimal.NewFromInt(2), dec("600")) == "" {
		t.Fatal("1200 > 1000 이면 거부")
	}
	if guard.TryReserve("AAPL", decimal.NewFromInt(1), dec("600")) != "" {
		t.Fatal("600 은 통과")
	}
	if guard.TryReserve("AAPL", decimal.NewFromInt(1), nil) == "" {
		t.Fatal("가격 미상은 거부")
	}
	if NewRiskGuard(nil, nil).TryReserve("AAPL", decimal.NewFromInt(1), nil) != "" {
		t.Fatal("상한 없으면 통과")
	}

	now := time.Date(2026, 9, 10, 23, 0, 0, 0, time.UTC)
	daily := NewRiskGuard(nil, dec("1000"))
	daily.now = func() time.Time { return now }
	daily.day = daily.today()
	if daily.TryReserve("AAPL", decimal.NewFromInt(3), dec("300")) != "" {
		t.Fatal("900 통과")
	}
	if daily.TryReserve("AAPL", decimal.NewFromInt(1), dec("300")) == "" {
		t.Fatal("1200 거부")
	}
	now = time.Date(2026, 9, 11, 1, 0, 0, 0, time.UTC)
	if daily.TryReserve("AAPL", decimal.NewFromInt(1), dec("300")) != "" {
		t.Fatal("UTC 자정 경과 후 초기화")
	}
	if daily.DailyTotal().String() != "300" {
		t.Fatalf("dailyTotal = %s", daily.DailyTotal())
	}
}

func TestExecutorSkipsOrdersOverRiskLimit(t *testing.T) {
	broker := newFakeBroker()
	executor := NewOrderExecutor(broker, NewBracketMonitor(broker), NewTradingGuard(broker, 5)).WithRisk(NewRiskGuard(dec("1000"), nil))
	executor.Execute("t", []Signal{
		BuySignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(5)},
		BuySignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(3)},
		BuySignal{Symbol: "AAPL", Quantity: decimal.NewFromInt(10), OrderType: Limit, LimitPrice: dec("50")},
		BuySignal{Symbol: "NOPE", Quantity: decimal.NewFromInt(1)},
	}, testCtx("300", ""))
	if len(broker.created) != 2 || broker.created[0].Quantity.String() != "3" || broker.created[1].Quantity.String() != "10" {
		t.Fatalf("created = %+v", broker.created)
	}
}

type noopStrategy struct{}

func (noopStrategy) Spec() StrategySpec                        { return StrategySpec{Name: "t", Symbols: []string{"AAPL"}} }
func (noopStrategy) Decide(*StrategyContext) ([]Signal, error) { return nil, nil }

func TestLiveGateRequiresExplicitConsent(t *testing.T) {
	broker := newFakeBroker()
	broker.environment = Live
	if n := len(NewStrategyEngine(broker, []Strategy{noopStrategy{}}).Strategies); n != 0 {
		t.Fatalf("LIVE 는 동의 없이 스케줄되면 안 된다: %d", n)
	}
	if n := len(NewStrategyEngineWithOptions(broker, []Strategy{noopStrategy{}}, EngineOptions{LiveTradingEnabled: true}).Strategies); n != 1 {
		t.Fatalf("LIVE + 동의 = 스케줄: %d", n)
	}
	broker.environment = Paper
	if n := len(NewStrategyEngine(broker, []Strategy{noopStrategy{}}).Strategies); n != 1 {
		t.Fatalf("PAPER 는 스케줄: %d", n)
	}
}

func TestContextMatchesSymbolsIgnoringMarketPrefix(t *testing.T) {
	ctx := &StrategyContext{
		Quotes:     map[string]Quote{"KRX:005930": {Symbol: "KRX:005930", Price: decimal.NewFromInt(70000)}},
		Holdings:   map[string]Holding{"005930": {Symbol: "005930", Quantity: decimal.NewFromInt(3)}},
		OpenOrders: []Order{{OrderID: "o1", Status: Submitted, Symbol: "005930"}},
	}
	if q, ok := ctx.Quote("005930"); !ok || q.Price.String() != "70000" {
		t.Fatal("접두 없는 조회가 접두 있는 키에 맞아야 한다")
	}
	if h, ok := ctx.Holding("KRX:005930"); !ok || h.Quantity.String() != "3" {
		t.Fatal("접두 있는 조회가 접두 없는 키에 맞아야 한다")
	}
	if !ctx.HasPosition("KRX:005930") || !ctx.HasOpenOrder("KRX:005930") {
		t.Fatal("HasPosition/HasOpenOrder")
	}
	if _, ok := ctx.Quote("AAPL"); ok {
		t.Fatal("없는 심볼")
	}
}
