package hermetix

// 전략 실행 엔진 - 매 틱: 장시간 확인 -> 스냅샷 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행.

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// MarketCalendar - 개장 판단, 브로커 캘린더를 6시간 캐시.
type MarketCalendar struct {
	broker   BrokerClient
	mu       sync.Mutex
	cache    []MarketDay
	cachedAt time.Time
}

func NewMarketCalendar(broker BrokerClient) *MarketCalendar {
	return &MarketCalendar{broker: broker}
}

func (m *MarketCalendar) IsRegularOpen(now time.Time) (bool, error) {
	days, err := m.calendar()
	if err != nil {
		return false, err
	}
	if len(days) == 0 {
		return false, nil
	}
	loc, err := time.LoadLocation(days[0].Timezone)
	if err != nil {
		return false, err
	}
	local := now.In(loc)
	date := local.Format("2006-01-02")
	for _, day := range days {
		if day.Date != date {
			continue
		}
		if !day.Open || day.Regular == nil {
			return false, nil
		}
		hhmm := local.Format("15:04")
		return day.Regular.Start <= hhmm && hhmm < day.Regular.End, nil
	}
	return false, nil
}

func (m *MarketCalendar) calendar() ([]MarketDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil || time.Since(m.cachedAt) > 6*time.Hour {
		days, err := m.broker.GetCalendar()
		if err != nil {
			return nil, err
		}
		m.cache = days
		m.cachedAt = time.Now()
		log.Printf("INFO hermetix market calendar refreshed / days=%d", len(days))
	}
	return m.cache, nil
}

// TradingGuard - 비상정지: 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단.
type TradingGuard struct {
	broker      BrokerClient
	maxFailures int
	mu          sync.Mutex
	failures    int
	halted      bool
}

func NewTradingGuard(broker BrokerClient, maxFailures int) *TradingGuard {
	if maxFailures <= 0 {
		maxFailures = 5
	}
	return &TradingGuard{broker: broker, maxFailures: maxFailures}
}

func (g *TradingGuard) IsHalted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.halted
}

func (g *TradingGuard) RecordSuccess() {
	g.mu.Lock()
	g.failures = 0
	g.mu.Unlock()
}

func (g *TradingGuard) RecordFailure(cause error) {
	g.mu.Lock()
	g.failures++
	count := g.failures
	g.mu.Unlock()
	log.Printf("WARN hermetix engine failure %d/%d - %v", count, g.maxFailures, cause)
	if count >= g.maxFailures {
		g.Halt(fmt.Sprintf("연속 실패 %d회", count))
	}
}

func (g *TradingGuard) Halt(reason string) {
	g.mu.Lock()
	if g.halted {
		g.mu.Unlock()
		return
	}
	g.halted = true
	g.mu.Unlock()
	log.Printf("ERROR hermetix TRADING HALTED / %s - 미체결 전량 취소", reason)
	orders, err := g.broker.GetOrders()
	if err != nil {
		log.Printf("ERROR hermetix halt: open order lookup failed: %v", err)
		return
	}
	for _, order := range orders {
		if !order.Status.IsOpen() {
			continue
		}
		if _, err := g.broker.CancelOrder(order.OrderID); err != nil {
			log.Printf("ERROR hermetix halt-cancel failed / %s: %v", order.OrderID, err)
		} else {
			log.Printf("INFO hermetix halt-cancel ok / %s", order.OrderID)
		}
	}
}

func (g *TradingGuard) Resume() {
	g.mu.Lock()
	g.failures = 0
	g.halted = false
	g.mu.Unlock()
	log.Printf("INFO hermetix trading resumed")
}

type bracket struct {
	entryOrderID string
	symbol       string
	quantity     decimal.Decimal
	takeProfit   *decimal.Decimal
	stopLoss     *decimal.Decimal
	active       bool
}

// BracketMonitor - 소프트웨어 익절/손절. 상태는 메모리에만 (재시작 시 소실).
type BracketMonitor struct {
	broker   BrokerClient
	mu       sync.Mutex
	brackets map[string]*bracket
}

func NewBracketMonitor(broker BrokerClient) *BracketMonitor {
	return &BracketMonitor{broker: broker, brackets: map[string]*bracket{}}
}

func (b *BracketMonitor) Register(entryOrderID, symbol string, quantity decimal.Decimal,
	takeProfit, stopLoss *decimal.Decimal) {
	if takeProfit == nil && stopLoss == nil {
		return
	}
	b.mu.Lock()
	b.brackets[entryOrderID] = &bracket{entryOrderID, symbol, quantity, takeProfit, stopLoss, false}
	b.mu.Unlock()
	log.Printf("INFO hermetix bracket registered / %s %s qty=%s", entryOrderID, symbol, quantity)
}

func (b *BracketMonitor) Check(ctx *StrategyContext) []Signal {
	b.mu.Lock()
	snapshot := make([]*bracket, 0, len(b.brackets))
	for _, br := range b.brackets {
		snapshot = append(snapshot, br)
	}
	b.mu.Unlock()

	signals := make([]Signal, 0)
	for _, br := range snapshot {
		if !br.active && !b.resolveEntry(br) {
			continue
		}
		quote, ok := ctx.Quote(br.symbol)
		if !ok {
			continue
		}
		tpHit := br.takeProfit != nil && quote.Price.GreaterThanOrEqual(*br.takeProfit)
		slHit := br.stopLoss != nil && quote.Price.LessThanOrEqual(*br.stopLoss)
		if !tpHit && !slHit {
			continue
		}
		b.mu.Lock()
		delete(b.brackets, br.entryOrderID)
		b.mu.Unlock()

		held := decimal.Zero
		if h, ok := ctx.Holding(br.symbol); ok {
			held = h.Quantity
		}
		qty := decimal.Min(br.quantity, held)
		if !qty.IsPositive() {
			log.Printf("WARN hermetix bracket hit but no holdings / %s", br.symbol)
			continue
		}
		kind := "STOP-LOSS"
		if tpHit {
			kind = "TAKE-PROFIT"
		}
		log.Printf("INFO hermetix bracket %s / %s price=%s", kind, br.symbol, quote.Price)
		signals = append(signals, SellSignal{Symbol: br.symbol, Quantity: qty})
	}
	return signals
}

func (b *BracketMonitor) resolveEntry(br *bracket) bool {
	order, err := b.broker.GetOrder(br.entryOrderID)
	if err != nil {
		log.Printf("WARN hermetix bracket entry lookup failed / %s: %v", br.entryOrderID, err)
		return false
	}
	if order.Status == Filled {
		br.active = true
		log.Printf("INFO hermetix bracket activated / entry filled %s", br.entryOrderID)
		return true
	}
	if !order.Status.IsOpen() {
		b.mu.Lock()
		delete(b.brackets, br.entryOrderID)
		b.mu.Unlock()
		log.Printf("INFO hermetix bracket dropped / entry %s %s", order.Status, br.entryOrderID)
	}
	return false
}

func (b *BracketMonitor) ActiveCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.brackets)
}

// OrderExecutor - Signal -> 주문 실행. 매도 클램프(공매도 방지), 멱등키(지원 브로커만).
type OrderExecutor struct {
	broker   BrokerClient
	brackets *BracketMonitor
	guard    *TradingGuard
}

func NewOrderExecutor(broker BrokerClient, brackets *BracketMonitor, guard *TradingGuard) *OrderExecutor {
	return &OrderExecutor{broker, brackets, guard}
}

func (e *OrderExecutor) Execute(strategyName string, signals []Signal, ctx *StrategyContext) {
	for _, signal := range signals {
		if e.guard.IsHalted() {
			log.Printf("WARN hermetix [%s] halted - signal skipped", strategyName)
			continue
		}
		var err error
		switch s := signal.(type) {
		case BuySignal:
			err = e.buy(strategyName, s)
		case SellSignal:
			err = e.sell(strategyName, s, ctx)
		case CancelSignal:
			var order Order
			if order, err = e.broker.CancelOrder(s.OrderID); err == nil {
				log.Printf("INFO hermetix [%s] CANCEL / %s -> %s", strategyName, s.OrderID, order.Status)
			}
		}
		var insufficient *InsufficientFundsError
		if errors.As(err, &insufficient) {
			log.Printf("WARN hermetix [%s] 주문가능금액 부족으로 시그널 스킵", strategyName)
		} else if err != nil {
			log.Printf("ERROR hermetix [%s] signal 실행 실패: %v", strategyName, err)
		}
	}
}

func (e *OrderExecutor) buy(strategyName string, signal BuySignal) error {
	orderType := signal.OrderType
	if orderType == "" {
		orderType = Market
	}
	tif := signal.TimeInForce
	if tif == "" {
		tif = Day
	}
	order, err := e.broker.CreateOrder(CreateOrderRequest{
		Symbol: signal.Symbol, Side: Buy, OrderType: orderType,
		Quantity: signal.Quantity, LimitPrice: signal.LimitPrice, TimeInForce: tif,
		ClientOrderID: e.clientOrderID(strategyName),
	})
	if err != nil {
		return err
	}
	log.Printf("INFO hermetix [%s] BUY 접수 / %s qty=%s orderId=%s",
		strategyName, signal.Symbol, signal.Quantity, order.OrderID)
	e.brackets.Register(order.OrderID, signal.Symbol, signal.Quantity, signal.TakeProfitPrice, signal.StopLossPrice)
	return nil
}

func (e *OrderExecutor) sell(strategyName string, signal SellSignal, ctx *StrategyContext) error {
	held := decimal.Zero
	if h, ok := ctx.Holding(signal.Symbol); ok {
		held = h.Quantity
	}
	qty := decimal.Min(signal.Quantity, held)
	if !qty.IsPositive() {
		log.Printf("WARN hermetix [%s] SELL 스킵 / %s 보유 수량 없음", strategyName, signal.Symbol)
		return nil
	}
	orderType := signal.OrderType
	if orderType == "" {
		orderType = Market
	}
	tif := signal.TimeInForce
	if tif == "" {
		tif = Day
	}
	order, err := e.broker.CreateOrder(CreateOrderRequest{
		Symbol: signal.Symbol, Side: Sell, OrderType: orderType,
		Quantity: qty, LimitPrice: signal.LimitPrice, TimeInForce: tif,
		ClientOrderID: e.clientOrderID(strategyName),
	})
	if err != nil {
		return err
	}
	log.Printf("INFO hermetix [%s] SELL 접수 / %s qty=%s orderId=%s", strategyName, signal.Symbol, qty, order.OrderID)
	return nil
}

func (e *OrderExecutor) clientOrderID(strategyName string) string {
	if !e.broker.Capabilities().ClientOrderID {
		return ""
	}
	return fmt.Sprintf("%s-%08x", strategyName, rand.Uint32())
}

// StrategyEngine - 등록된 전략들을 각자의 PollInterval 로 순차 호출한다.
type StrategyEngine struct {
	Broker     BrokerClient
	Guard      *TradingGuard
	Brackets   *BracketMonitor
	Executor   *OrderExecutor
	Calendar   *MarketCalendar
	Strategies []Strategy
	stop       chan struct{}
}

func NewStrategyEngine(broker BrokerClient, strategies []Strategy) *StrategyEngine {
	guard := NewTradingGuard(broker, 5)
	brackets := NewBracketMonitor(broker)
	caps := broker.Capabilities()

	accepted := make([]Strategy, 0, len(strategies))
	names := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		spec := strategy.Spec()
		// capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
		if !caps.CandleIntervals[spec.candleInterval()] {
			log.Printf("ERROR hermetix [%s] 스케줄 제외: 브로커 '%s' 는 %s 캔들을 지원하지 않습니다",
				spec.Name, caps.BrokerID, spec.candleInterval())
			continue
		}
		accepted = append(accepted, strategy)
		names = append(names, spec.Name)
	}
	log.Printf("INFO hermetix broker=%s market=%s / strategies=%v", caps.BrokerID, caps.Market, names)

	return &StrategyEngine{
		Broker: broker, Guard: guard, Brackets: brackets,
		Executor: NewOrderExecutor(broker, brackets, guard),
		Calendar: NewMarketCalendar(broker),
		Strategies: accepted,
		stop:       make(chan struct{}),
	}
}

// Run - 블로킹 실행 루프. Stop() 으로 종료.
func (e *StrategyEngine) Run() {
	nextRun := map[string]time.Time{}
	for {
		select {
		case <-e.stop:
			return
		default:
		}
		for _, strategy := range e.Strategies {
			spec := strategy.Spec()
			if time.Now().After(nextRun[spec.Name]) {
				e.Tick(strategy)
				nextRun[spec.Name] = time.Now().Add(spec.pollInterval())
			}
		}
		time.Sleep(time.Second)
	}
}

func (e *StrategyEngine) Stop() { close(e.stop) }

func (e *StrategyEngine) Tick(strategy Strategy) {
	spec := strategy.Spec()
	err := e.tick(strategy)
	if err == nil {
		e.Guard.RecordSuccess()
		return
	}
	var marketClosed *MarketClosedError
	var rateLimited *RateLimitError
	switch {
	case errors.As(err, &marketClosed): // 휴장 - 실패 아님
	case errors.As(err, &rateLimited):
		log.Printf("WARN hermetix [%s] rate limited - %v", spec.Name, err)
	default:
		log.Printf("ERROR hermetix [%s] tick failed: %v", spec.Name, err)
		e.Guard.RecordFailure(err)
	}
}

func (e *StrategyEngine) tick(strategy Strategy) error {
	spec := strategy.Spec()
	if e.Guard.IsHalted() {
		return nil
	}
	if spec.regularHoursOnly() {
		open, err := e.Calendar.IsRegularOpen(time.Now())
		if err != nil {
			return err
		}
		if !open {
			return nil
		}
	}

	ctx, err := e.buildContext(strategy)
	if err != nil {
		return err
	}

	if bracketSignals := e.Brackets.Check(ctx); len(bracketSignals) > 0 { // 익절/손절 우선
		e.Executor.Execute(spec.Name, bracketSignals, ctx)
	}

	signals, err := strategy.Decide(ctx)
	if err != nil {
		return err
	}
	if len(signals) > 0 {
		e.Executor.Execute(spec.Name, signals, ctx)
	}
	return nil
}

func (e *StrategyEngine) buildContext(strategy Strategy) (*StrategyContext, error) {
	spec := strategy.Spec()
	quotes, err := e.Broker.GetQuotes(spec.Symbols)
	if err != nil {
		return nil, err
	}
	candles := map[string][]Candle{}
	for _, symbol := range spec.Symbols {
		list, err := e.Broker.GetCandles(symbol, spec.candleInterval(), spec.candleLimit())
		if err != nil {
			return nil, err
		}
		candles[symbol] = list
	}
	account, err := e.Broker.GetAccount()
	if err != nil {
		return nil, err
	}
	holdings, err := e.Broker.GetHoldings()
	if err != nil {
		return nil, err
	}
	orders, err := e.Broker.GetOrders()
	if err != nil {
		return nil, err
	}
	power, err := e.Broker.GetBuyingPower()
	if err != nil {
		return nil, err
	}

	quoteMap := map[string]Quote{}
	for _, q := range quotes {
		quoteMap[q.Symbol] = q
	}
	holdingMap := map[string]Holding{}
	for _, h := range holdings {
		holdingMap[h.Symbol] = h
	}
	openOrders := make([]Order, 0)
	for _, o := range orders {
		if o.Status.IsOpen() {
			openOrders = append(openOrders, o)
		}
	}
	return &StrategyContext{
		Now: time.Now(), Quotes: quoteMap, Candles: candles,
		Account: account, Holdings: holdingMap, OpenOrders: openOrders, BuyingPower: power,
	}, nil
}

// PnlReport - 계좌 수익률 리포트.
type PnlReport struct {
	Timestamp          time.Time
	AccountID          string
	Currency           string
	Cash               decimal.Decimal
	PortfolioValue     decimal.Decimal
	TotalMarketValue   decimal.Decimal
	TotalUnrealizedPnl decimal.Decimal
	TotalReturnRate    *decimal.Decimal
	Holdings           []Holding
}

func Pnl(broker BrokerClient, initialCapital *decimal.Decimal) (PnlReport, error) {
	account, err := broker.GetAccount()
	if err != nil {
		return PnlReport{}, err
	}
	holdings, err := broker.GetHoldings()
	if err != nil {
		return PnlReport{}, err
	}
	totalMv, totalPnl := decimal.Zero, decimal.Zero
	for _, h := range holdings {
		if h.MarketValue != nil {
			totalMv = totalMv.Add(*h.MarketValue)
		}
		if h.UnrealizedPnl != nil {
			totalPnl = totalPnl.Add(*h.UnrealizedPnl)
		}
	}
	report := PnlReport{
		Timestamp: time.Now(), AccountID: account.AccountID, Currency: account.Currency,
		Cash: account.Cash, PortfolioValue: account.PortfolioValue,
		TotalMarketValue: totalMv, TotalUnrealizedPnl: totalPnl, Holdings: holdings,
	}
	if initialCapital != nil && initialCapital.IsPositive() {
		rate := account.PortfolioValue.Sub(*initialCapital).Div(*initialCapital)
		report.TotalReturnRate = &rate
	}
	return report, nil
}
