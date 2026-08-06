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

type StrategySpec struct {
	Name             string
	Symbols          []string
	CandleInterval   CandleInterval // 기본 Day1
	CandleLimit      int            // 기본 30
	PollInterval     time.Duration  // 기본 60s
	RegularHoursOnly *bool          // 기본 true
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

func (c *StrategyContext) Quote(symbol string) (Quote, bool) {
	q, ok := c.Quotes[symbol]
	return q, ok
}

func (c *StrategyContext) CandlesOf(symbol string) []Candle {
	return c.Candles[symbol]
}

func (c *StrategyContext) Holding(symbol string) (Holding, bool) {
	h, ok := c.Holdings[symbol]
	return h, ok
}

func (c *StrategyContext) HasPosition(symbol string) bool {
	h, ok := c.Holdings[symbol]
	return ok && h.Quantity.IsPositive()
}

func (c *StrategyContext) OpenOrdersOf(symbol string) []Order {
	result := make([]Order, 0)
	for _, o := range c.OpenOrders {
		if o.Symbol == symbol {
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
