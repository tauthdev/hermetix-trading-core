"""어댑터 컨포먼스 검증 - 모든 BrokerClient 구현이 지켜야 하는 공통 모델 규약을 한 시나리오로 확인한다.

시세 → 캔들 → 캘린더 → 계좌 → 보유 → 매수가능 → 주문 → 조회 → 취소 → 체결 순으로 호출하고 위반을 모은다.
상태 전이(취소 후 재조회)는 검사하지 않는다 - 정적 골든 픽스처로 재생 가능해야 하기 때문이다.
새 어댑터 기여 조건은 `verify_broker_conformance(...).violations` 가 비어 있는 것이다 (conformance/README.md).
Kotlin BrokerConformance / JS verifyBrokerConformance / Go VerifyBrokerConformance 와 같은 검사 항목.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from .broker import BrokerClient
from .models import CreateOrderRequest, OrderSide, OrderStatus, OrderType, parse_symbol, symbols_match


@dataclass(frozen=True)
class ConformanceScenario:
    symbol: str
    limit_price: Decimal
    quantity: Decimal = Decimal(1)


@dataclass
class ConformanceReport:
    steps: list[str] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations

    def __str__(self) -> str:
        if self.passed:
            return f"conformance OK ({len(self.steps)} steps)"
        return "conformance FAILED:\n - " + "\n - ".join(self.violations)


def verify_broker_conformance(broker: BrokerClient, scenario: ConformanceScenario) -> ConformanceReport:
    report = ConformanceReport()
    caps = broker.capabilities
    ten = Decimal(10)

    def check(condition: bool, message: str) -> None:
        if not condition:
            report.violations.append(message)

    def step(name: str, fn) -> None:
        report.steps.append(name)
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - 위반으로 기록
            report.violations.append(f"{name}: 예외 {type(e).__name__}: {e}")

    def capabilities():
        check(bool(caps.broker_id), "capabilities.broker_id 가 비어 있다")
        check(bool(caps.currency), "capabilities.currency 가 비어 있다")
        check(caps.market in caps.markets, f"capabilities.market({caps.market}) 이 markets 에 없다")
        check(broker.environment in caps.environments, f"environment({broker.environment}) 이 선언된 environments 에 없다")
        check(bool(caps.candle_intervals), "candle_intervals 가 비어 있다")

    def quotes():
        result = broker.get_quotes([scenario.symbol])
        check(len(result) == 1, f"quotes: 1건을 기대했는데 {len(result)}건")
        for q in result[:1]:
            check(q.symbol == scenario.symbol, f"quotes: 심볼은 요청 표기 그대로여야 한다 (요청={scenario.symbol}, 응답={q.symbol})")
            check(q.price > 0, f"quotes: price 는 양수여야 한다 ({q.price})")
            check(isinstance(q.timestamp, datetime), "quotes: timestamp 가 없다")
            check(q.volume >= 0, f"quotes: volume 음수 ({q.volume})")
            if q.change_rate is not None:
                check(abs(q.change_rate) <= ten, f"quotes: change_rate 는 비율이어야 한다 (% 로 보임: {q.change_rate})")

    def candles():
        interval = sorted(caps.candle_intervals, key=lambda i: i.value)[0]
        result = broker.get_candles(scenario.symbol, interval, 3)
        check(bool(result), "candles: 비어 있다")
        check(len(result) <= 3, f"candles: limit=3 을 넘겼다 ({len(result)})")
        check(all(a.timestamp < b.timestamp for a, b in zip(result, result[1:])), "candles: 시각이 오름차순이 아니다")
        for c in result:
            check(c.low <= c.high and c.low <= c.open <= c.high and c.low <= c.close <= c.high,
                  f"candles: OHLC 범위 위반 {c.timestamp} o={c.open} h={c.high} l={c.low} c={c.close}")
            check(c.volume >= 0, "candles: volume 음수")

    def calendar():
        days = broker.get_calendar()
        check(bool(days), "calendar: 비어 있다")
        for d in days:
            try:
                date.fromisoformat(d.date)
            except ValueError:
                report.violations.append(f"calendar: 날짜 형식 위반 {d.date}")
            try:
                ZoneInfo(d.timezone)
            except Exception:  # noqa: BLE001
                report.violations.append(f"calendar: 타임존 위반 {d.timezone}")
            if d.open:
                check(d.regular is not None, f"calendar: 개장일 {d.date} 에 정규장 세션이 없다")
                if d.regular is not None:
                    ok = all(len(t) == 5 and t[2] == ":" and t[:2].isdigit() and t[3:].isdigit() for t in (d.regular.start, d.regular.end))
                    check(ok, f"calendar: 세션 시각은 HH:MM 이어야 한다 ({d.regular.start}~{d.regular.end})")

    def account():
        a = broker.get_account()
        check(bool(a.account_id), "account: account_id 비어 있음")
        check(a.currency == caps.currency, f"account: currency({a.currency}) 가 capabilities.currency({caps.currency}) 와 다르다")
        check(a.cash >= 0, "account: cash 음수")
        check(a.portfolio_value >= 0, "account: portfolio_value 음수")

    def holdings():
        for h in broker.get_holdings():
            check(bool(h.symbol), "holdings: 심볼 비어 있음")
            market, _ = parse_symbol(h.symbol)
            check(market is None or market in caps.markets, f"holdings: 미지원 시장 접두 {h.symbol}")
            check(h.quantity > 0, f"holdings: quantity 는 양수여야 한다 ({h.symbol}={h.quantity})")
            check(h.avg_entry_price >= 0, f"holdings: avg_entry_price 음수 ({h.symbol})")
            if h.unrealized_pnl_rate is not None:
                check(abs(h.unrealized_pnl_rate) <= ten, f"holdings: unrealized_pnl_rate 는 비율이어야 한다 (% 로 보임: {h.symbol}={h.unrealized_pnl_rate})")

    def buying_power():
        check(broker.get_buying_power() >= 0, "buying_power 음수")

    order_id: list[str] = []

    def create_order():
        order = broker.create_order(CreateOrderRequest(
            symbol=scenario.symbol, side=OrderSide.BUY, order_type=OrderType.LIMIT,
            quantity=scenario.quantity, limit_price=scenario.limit_price, client_order_id="conformance-1"))
        check(bool(order.order_id), "create_order: order_id 비어 있음")
        check(order.status.is_open, f"create_order: 접수 직후 상태는 미체결(open)이어야 한다 ({order.status})")
        if order.symbol:
            check(symbols_match(order.symbol, scenario.symbol), f"create_order: 심볼 불일치 ({order.symbol})")
        if order.order_id:
            order_id.append(order.order_id)

    def get_order():
        order = broker.get_order(order_id[0])
        check(order.order_id == order_id[0], f"get_order: order_id 불일치 ({order.order_id})")
        check(order.status != OrderStatus.UNKNOWN, "get_order: status UNKNOWN")

    def get_orders():
        orders = broker.get_orders()
        check(any(o.order_id == order_id[0] for o in orders), f"get_orders: 방금 낸 주문 {order_id[0]} 가 목록에 없다")
        for o in orders:
            check(o.status != OrderStatus.UNKNOWN, f"get_orders: status UNKNOWN ({o.order_id})")

    def cancel_order():
        canceled = broker.cancel_order(order_id[0])
        check(canceled.order_id == order_id[0], f"cancel_order: order_id 불일치 ({canceled.order_id})")
        check(canceled.status in (OrderStatus.PENDING_CANCEL, OrderStatus.CANCELED),
              f"cancel_order: status 는 PENDING_CANCEL/CANCELED 이어야 한다 ({canceled.status})")

    def fills():
        for f in broker.get_fills():
            if f.quantity is not None:
                check(f.quantity > 0, f"fills: quantity 는 양수 ({f.order_id})")
            if f.price is not None:
                check(f.price > 0, f"fills: price 는 양수 ({f.order_id})")

    step("capabilities", capabilities)
    step("quotes", quotes)
    step("candles", candles)
    step("calendar", calendar)
    step("account", account)
    step("holdings", holdings)
    step("buying_power", buying_power)
    step("create_order", create_order)
    if order_id:
        step("get_order", get_order)
        step("get_orders", get_orders)
        step("cancel_order", cancel_order)
    step("fills", fills)
    return report
