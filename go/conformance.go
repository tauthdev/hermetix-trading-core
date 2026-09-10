package hermetix

// 어댑터 컨포먼스 검증 - 모든 BrokerClient 구현이 지켜야 하는 공통 모델 규약을 한 시나리오로 확인한다.
// 시세 → 캔들 → 캘린더 → 계좌 → 보유 → 매수가능 → 주문 → 조회 → 취소 → 체결 순으로 호출하고 위반을 모은다.
// 상태 전이(취소 후 재조회)는 검사하지 않는다 - 정적 골든 픽스처로 재생 가능해야 하기 때문이다.
// 새 어댑터 기여 조건은 Violations 가 비어 있는 것이다 (conformance/README.md). Kotlin BrokerConformance 와 같은 검사 항목.

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type ConformanceScenario struct {
	Symbol     string
	Quantity   decimal.Decimal
	LimitPrice decimal.Decimal
}

type ConformanceReport struct {
	Steps      []string
	Violations []string
}

func (r ConformanceReport) Passed() bool { return len(r.Violations) == 0 }

func (r ConformanceReport) String() string {
	if r.Passed() {
		return fmt.Sprintf("conformance OK (%d steps)", len(r.Steps))
	}
	return "conformance FAILED:\n - " + strings.Join(r.Violations, "\n - ")
}

var (
	hhmm    = regexp.MustCompile(`^\d{2}:\d{2}$`)
	isoDate = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	ten     = decimal.NewFromInt(10)
)

func VerifyBrokerConformance(broker BrokerClient, scenario ConformanceScenario) ConformanceReport {
	report := ConformanceReport{}
	check := func(cond bool, msg string) {
		if !cond {
			report.Violations = append(report.Violations, msg)
		}
	}
	step := func(name string, fn func() error) {
		report.Steps = append(report.Steps, name)
		if err := fn(); err != nil {
			report.Violations = append(report.Violations, fmt.Sprintf("%s: 예외 %v", name, err))
		}
	}
	caps := broker.Capabilities()
	quantity := scenario.Quantity
	if quantity.IsZero() {
		quantity = decimal.NewFromInt(1)
	}

	step("capabilities", func() error {
		check(caps.BrokerID != "", "capabilities.BrokerID 가 비어 있다")
		check(caps.Currency != "", "capabilities.Currency 가 비어 있다")
		check(caps.Markets == nil || caps.Markets[caps.Market] || caps.Market != "", "capabilities.Market 이 Markets 에 없다")
		check(caps.SupportsEnvironment(broker.Environment()), fmt.Sprintf("environment(%s) 이 선언된 Environments 에 없다", broker.Environment()))
		check(len(caps.CandleIntervals) > 0, "CandleIntervals 가 비어 있다")
		return nil
	})

	step("quotes", func() error {
		quotes, err := broker.GetQuotes([]string{scenario.Symbol})
		if err != nil {
			return err
		}
		check(len(quotes) == 1, fmt.Sprintf("quotes: 1건을 기대했는데 %d건", len(quotes)))
		if len(quotes) > 0 {
			q := quotes[0]
			check(q.Symbol == scenario.Symbol, fmt.Sprintf("quotes: 심볼은 요청 표기 그대로여야 한다 (요청=%s, 응답=%s)", scenario.Symbol, q.Symbol))
			check(q.Price.IsPositive(), fmt.Sprintf("quotes: price 는 양수여야 한다 (%s)", q.Price))
			check(!q.Timestamp.IsZero(), "quotes: timestamp 가 없다")
			check(q.Volume >= 0, "quotes: volume 음수")
			if q.ChangeRate != nil {
				check(q.ChangeRate.Abs().LessThanOrEqual(ten), fmt.Sprintf("quotes: ChangeRate 는 비율이어야 한다 (%% 로 보임: %s)", q.ChangeRate))
			}
		}
		return nil
	})

	step("candles", func() error {
		intervals := make([]string, 0, len(caps.CandleIntervals))
		for i, ok := range caps.CandleIntervals {
			if ok {
				intervals = append(intervals, string(i))
			}
		}
		sort.Strings(intervals)
		candles, err := broker.GetCandles(scenario.Symbol, CandleInterval(intervals[0]), 3)
		if err != nil {
			return err
		}
		check(len(candles) > 0, "candles: 비어 있다")
		check(len(candles) <= 3, fmt.Sprintf("candles: limit=3 을 넘겼다 (%d)", len(candles)))
		for i, c := range candles {
			if i > 0 {
				check(candles[i-1].Timestamp.Before(c.Timestamp), "candles: 시각이 오름차순이 아니다")
			}
			inRange := c.Low.LessThanOrEqual(c.High) && c.Open.GreaterThanOrEqual(c.Low) && c.Open.LessThanOrEqual(c.High) &&
				c.Close.GreaterThanOrEqual(c.Low) && c.Close.LessThanOrEqual(c.High)
			check(inRange, fmt.Sprintf("candles: OHLC 범위 위반 %s o=%s h=%s l=%s c=%s", c.Timestamp, c.Open, c.High, c.Low, c.Close))
			check(c.Volume >= 0, "candles: volume 음수")
			check(!c.Timestamp.IsZero(), "candles: timestamp 가 없다")
		}
		return nil
	})

	step("calendar", func() error {
		days, err := broker.GetCalendar()
		if err != nil {
			return err
		}
		check(len(days) > 0, "calendar: 비어 있다")
		for _, d := range days {
			check(isoDate.MatchString(d.Date), "calendar: 날짜 형식 위반 "+d.Date)
			if _, err := time.LoadLocation(d.Timezone); err != nil {
				report.Violations = append(report.Violations, "calendar: 타임존 위반 "+d.Timezone)
			}
			if d.Open {
				check(d.Regular != nil, fmt.Sprintf("calendar: 개장일 %s 에 정규장 세션이 없다", d.Date))
				if d.Regular != nil {
					check(hhmm.MatchString(d.Regular.Start) && hhmm.MatchString(d.Regular.End), fmt.Sprintf("calendar: 세션 시각은 HH:MM 이어야 한다 (%s~%s)", d.Regular.Start, d.Regular.End))
				}
			}
		}
		return nil
	})

	step("account", func() error {
		a, err := broker.GetAccount()
		if err != nil {
			return err
		}
		check(a.AccountID != "", "account: AccountID 비어 있음")
		check(a.Currency == caps.Currency, fmt.Sprintf("account: currency(%s) 가 capabilities.Currency(%s) 와 다르다", a.Currency, caps.Currency))
		check(!a.Cash.IsNegative(), "account: cash 음수")
		check(!a.PortfolioValue.IsNegative(), "account: portfolioValue 음수")
		return nil
	})

	step("holdings", func() error {
		holdings, err := broker.GetHoldings()
		if err != nil {
			return err
		}
		for _, h := range holdings {
			check(h.Symbol != "", "holdings: 심볼 비어 있음")
			market, _ := ParseSymbol(h.Symbol)
			check(market == "" || market == caps.Market || caps.Markets[market], "holdings: 미지원 시장 접두 "+h.Symbol)
			check(h.Quantity.IsPositive(), fmt.Sprintf("holdings: quantity 는 양수여야 한다 (%s=%s)", h.Symbol, h.Quantity))
			check(!h.AvgEntryPrice.IsNegative(), "holdings: avgEntryPrice 음수 "+h.Symbol)
			if h.UnrealizedPnlRate != nil {
				check(h.UnrealizedPnlRate.Abs().LessThanOrEqual(ten), fmt.Sprintf("holdings: UnrealizedPnlRate 는 비율이어야 한다 (%% 로 보임: %s=%s)", h.Symbol, h.UnrealizedPnlRate))
			}
		}
		return nil
	})

	step("buyingPower", func() error {
		bp, err := broker.GetBuyingPower()
		if err != nil {
			return err
		}
		check(!bp.IsNegative(), "buyingPower 음수")
		return nil
	})

	orderID := ""
	step("createOrder", func() error {
		limit := scenario.LimitPrice
		order, err := broker.CreateOrder(CreateOrderRequest{
			Symbol: scenario.Symbol, Side: Buy, OrderType: Limit, Quantity: quantity, LimitPrice: &limit, ClientOrderID: "conformance-1",
		})
		if err != nil {
			return err
		}
		check(order.OrderID != "", "createOrder: orderId 비어 있음")
		check(order.Status.IsOpen(), fmt.Sprintf("createOrder: 접수 직후 상태는 미체결(open)이어야 한다 (%s)", order.Status))
		if order.Symbol != "" {
			check(SymbolsMatch(order.Symbol, scenario.Symbol), "createOrder: 심볼 불일치 "+order.Symbol)
		}
		orderID = order.OrderID
		return nil
	})

	if orderID != "" {
		step("getOrder", func() error {
			order, err := broker.GetOrder(orderID)
			if err != nil {
				return err
			}
			check(order.OrderID == orderID, "getOrder: orderId 불일치 "+order.OrderID)
			check(order.Status != Unknown, "getOrder: status UNKNOWN")
			return nil
		})
		step("getOrders", func() error {
			orders, err := broker.GetOrders()
			if err != nil {
				return err
			}
			found := false
			for _, o := range orders {
				if o.OrderID == orderID {
					found = true
				}
				check(o.Status != Unknown, "getOrders: status UNKNOWN "+o.OrderID)
			}
			check(found, fmt.Sprintf("getOrders: 방금 낸 주문 %s 가 목록에 없다", orderID))
			return nil
		})
		step("cancelOrder", func() error {
			canceled, err := broker.CancelOrder(orderID)
			if err != nil {
				return err
			}
			check(canceled.OrderID == orderID, "cancelOrder: orderId 불일치 "+canceled.OrderID)
			check(canceled.Status == PendingCancel || canceled.Status == Canceled, fmt.Sprintf("cancelOrder: status 는 PENDING_CANCEL/CANCELED 이어야 한다 (%s)", canceled.Status))
			return nil
		})
	}

	step("fills", func() error {
		fills, err := broker.GetFills()
		if err != nil {
			return err
		}
		for _, f := range fills {
			if f.Quantity != nil {
				check(f.Quantity.IsPositive(), "fills: quantity 는 양수 "+f.OrderID)
			}
			if f.Price != nil {
				check(f.Price.IsPositive(), "fills: price 는 양수 "+f.OrderID)
			}
		}
		return nil
	})

	return report
}
