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
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{orderStatus: Submitted}
}

func (f *fakeBroker) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "fake", Market: "US", Currency: "USD",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   true, ServerOpenOrders: true,
	}
}
func (f *fakeBroker) GetQuotes([]string) ([]Quote, error)                    { return nil, nil }
func (f *fakeBroker) GetCandles(string, CandleInterval, int) ([]Candle, error) { return nil, nil }
func (f *fakeBroker) GetCalendar() ([]MarketDay, error)                      { return nil, nil }
func (f *fakeBroker) GetAccount() (Account, error)                           { return Account{}, nil }
func (f *fakeBroker) GetHoldings() ([]Holding, error)                        { return nil, nil }
func (f *fakeBroker) GetBuyingPower() (decimal.Decimal, error)               { return decimal.Zero, nil }
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
