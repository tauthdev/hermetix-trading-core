package hermetix

// 전략 SPI - 전략 작성자가 구현하는 유일한 표면.
//
// 규약:
//   - Decide 는 엔진이 PollInterval 주기로 호출 (기본: 해당 시장 정규장 중에만)
//   - 반환한 Signal 목록은 엔진이 순서대로 실행. 할 일 없으면 nil
//   - 전략 안에서 브로커 API 직접 호출 금지 - 데이터는 StrategyContext 로 공급된다

import (
	"time"

	"github.com/shopspring/decimal"
)

// TickTrigger - 전략 호출을 무엇이 촉발하는가.
//   - TriggerPoll: PollInterval 주기로만 호출 (기본, 모든 브로커)
//   - TriggerOnTrade: 브로커 체결가 스트림의 틱이 올 때마다 호출. 틱이 몰리면 하나로 합치고 MinTickInterval 보다 촘촘히는
//     부르지 않는다. 폴링은 안전망으로 계속 돈다. 브로커가 StreamTrades 를 선언하지 않으면 경고 후 폴링으로 동작한다
type TickTrigger string

const (
	TriggerPoll    TickTrigger = ""
	TriggerOnTrade TickTrigger = "ON_TRADE"
)

type StrategySpec struct {
	Name             string
	Symbols          []string
	CandleInterval   CandleInterval // 기본 Day1
	CandleLimit      int            // 기본 30
	PollInterval     time.Duration  // 기본 60s (TriggerOnTrade 에서는 스트림이 끊겼을 때의 안전망 주기)
	RegularHoursOnly *bool          // 기본 true
	Trigger          TickTrigger    // 기본 TriggerPoll
	MinTickInterval  time.Duration  // TriggerOnTrade 에서 연속 호출 사이 최소 간격. 기본 1s
}

func (s StrategySpec) minTickInterval() time.Duration {
	if s.MinTickInterval <= 0 {
		return time.Second
	}
	return s.MinTickInterval
}

func (s StrategySpec) candleInterval() CandleInterval {
	if s.CandleInterval == "" {
		return Day1
	}
	return s.CandleInterval
}

func (s StrategySpec) candleLimit() int {
	if s.CandleLimit <= 0 {
		return 30
	}
	return s.CandleLimit
}

func (s StrategySpec) pollInterval() time.Duration {
	if s.PollInterval <= 0 {
		return 60 * time.Second
	}
	return s.PollInterval
}

func (s StrategySpec) regularHoursOnly() bool {
	return s.RegularHoursOnly == nil || *s.RegularHoursOnly
}

// Signal - Buy/Sell/Cancel 의 합 타입.
type Signal interface{ isSignal() }

// BuySignal - 매수 진입. TakeProfit/StopLoss 지정 시 엔진이 자동 청산
// (소프트웨어 브라켓 - 메모리 관리, 재시작 시 소실).
type BuySignal struct {
	Symbol          string
	Quantity        decimal.Decimal
	OrderType       OrderType // 기본 MARKET
	LimitPrice      *decimal.Decimal
	TimeInForce     TimeInForce
	TakeProfitPrice *decimal.Decimal
	StopLossPrice   *decimal.Decimal
}

// SellSignal - 매도 청산. 보유 수량 내로 자동 클램프 (공매도 방지).
type SellSignal struct {
	Symbol      string
	Quantity    decimal.Decimal
	OrderType   OrderType
	LimitPrice  *decimal.Decimal
	TimeInForce TimeInForce
}

type CancelSignal struct{ OrderID string }

func (BuySignal) isSignal()    {}
func (SellSignal) isSignal()   {}
func (CancelSignal) isSignal() {}

// StrategyContext - 전략 호출 시점의 시장/계좌 스냅샷.
type StrategyContext struct {
	Now         time.Time
	Quotes      map[string]Quote
	Candles     map[string][]Candle
	Account     Account
	Holdings    map[string]Holding
	OpenOrders  []Order
	BuyingPower decimal.Decimal
}

// 심볼 조회는 MARKET:CODE 접두 유무를 무시하고 코드로 맞춘다 (SymbolsMatch).

func bySymbol[T any](m map[string]T, symbol string) (T, bool) {
	if v, ok := m[symbol]; ok {
		return v, true
	}
	for k, v := range m {
		if SymbolsMatch(k, symbol) {
			return v, true
		}
	}
	var zero T
	return zero, false
}

func (c *StrategyContext) Quote(symbol string) (Quote, bool) {
	return bySymbol(c.Quotes, symbol)
}

func (c *StrategyContext) CandlesOf(symbol string) []Candle {
	list, _ := bySymbol(c.Candles, symbol)
	return list
}

func (c *StrategyContext) Holding(symbol string) (Holding, bool) {
	return bySymbol(c.Holdings, symbol)
}

func (c *StrategyContext) HasPosition(symbol string) bool {
	h, ok := c.Holding(symbol)
	return ok && h.Quantity.IsPositive()
}

func (c *StrategyContext) OpenOrdersOf(symbol string) []Order {
	result := make([]Order, 0)
	for _, o := range c.OpenOrders {
		if o.Symbol != "" && SymbolsMatch(o.Symbol, symbol) {
			result = append(result, o)
		}
	}
	return result
}

func (c *StrategyContext) HasOpenOrder(symbol string) bool {
	return len(c.OpenOrdersOf(symbol)) > 0
}

type Strategy interface {
	Spec() StrategySpec
	Decide(ctx *StrategyContext) ([]Signal, error)
}
