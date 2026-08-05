"""전략 실행 엔진 (Kotlin hermetix-engine 과 동일 의미).

매 틱: 장시간 확인 -> 스냅샷 구성 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행.
틱은 단일 스레드에서 순차 실행된다 (같은 계좌 공유 - 동시 주문 경합 차단).
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from .broker import BrokerClient
from .errors import InsufficientFundsError, MarketClosedError, RateLimitError
from .models import CreateOrderRequest, Order, OrderSide, OrderStatus, OrderType
from .strategy import Buy, Cancel, Sell, Signal, Strategy, StrategyContext

logger = logging.getLogger("hermetix")


class MarketCalendar:
    """개장 판단 - 브로커 캘린더를 6시간 캐시."""

    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._cache: list | None = None
        self._cached_at = 0.0

    def is_regular_open(self, now: datetime | None = None) -> bool:
        days = self._calendar()
        if not days:
            return False
        zone = ZoneInfo(days[0].timezone)
        local = (now or datetime.now(timezone.utc)).astimezone(zone)
        today = next((d for d in days if d.date == local.date().isoformat()), None)
        if today is None or not today.open or today.regular is None:
            return False
        hhmm = local.strftime("%H:%M")
        return today.regular.start <= hhmm < today.regular.end

    def _calendar(self):
        if self._cache is None or time.time() - self._cached_at > 6 * 3600:
            self._cache = self._broker.get_calendar()
            self._cached_at = time.time()
            logger.info("market calendar refreshed / days=%d", len(self._cache))
        return self._cache


class TradingGuard:
    """비상정지 - 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단."""

    def __init__(self, broker: BrokerClient, max_consecutive_failures: int = 5):
        self._broker = broker
        self._max = max_consecutive_failures
        self._failures = 0
        self._halted = False
        self._lock = threading.Lock()

    @property
    def is_halted(self) -> bool:
        return self._halted

    def record_success(self) -> None:
        self._failures = 0

    def record_failure(self, cause: Exception) -> None:
        with self._lock:
            self._failures += 1
            logger.warning("engine failure %d/%d - %s", self._failures, self._max, cause)
            if self._failures >= self._max and not self._halted:
                self.halt(f"연속 실패 {self._failures}회")

    def halt(self, reason: str) -> None:
        if self._halted:
            return
        self._halted = True
        logger.error("TRADING HALTED / %s - 미체결 전량 취소", reason)
        try:
            for order in self._broker.get_orders():
                if order.status.is_open:
                    try:
                        self._broker.cancel_order(order.order_id)
                        logger.info("halt-cancel ok / %s", order.order_id)
                    except Exception as e:  # noqa: BLE001 - 취소 실패해도 나머지 진행
                        logger.error("halt-cancel failed / %s: %s", order.order_id, e)
        except Exception as e:  # noqa: BLE001
            logger.error("halt: open order lookup failed: %s", e)

    def resume(self) -> None:
        self._failures = 0
        self._halted = False
        logger.info("trading resumed")


@dataclass(frozen=True)
class _Bracket:
    entry_order_id: str
    symbol: str
    quantity: Decimal
    take_profit: Decimal | None
    stop_loss: Decimal | None
    active: bool = False


class BracketMonitor:
    """소프트웨어 익절/손절. 상태는 메모리에만 있다 (재시작 시 소실)."""

    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._brackets: dict[str, _Bracket] = {}

    def register(self, entry_order_id: str, symbol: str, quantity: Decimal,
                 take_profit: Decimal | None, stop_loss: Decimal | None) -> None:
        if take_profit is None and stop_loss is None:
            return
        self._brackets[entry_order_id] = _Bracket(entry_order_id, symbol, quantity, take_profit, stop_loss)
        logger.info("bracket registered / %s %s qty=%s tp=%s sl=%s",
                    entry_order_id, symbol, quantity, take_profit, stop_loss)

    def check(self, ctx: StrategyContext) -> list[Sell]:
        signals: list[Sell] = []
        for bracket in list(self._brackets.values()):
            if not bracket.active:
                bracket = self._resolve_entry(bracket)
                if bracket is None or not bracket.active:
                    continue

            quote = ctx.quote(bracket.symbol)
            if quote is None:
                continue
            tp_hit = bracket.take_profit is not None and quote.price >= bracket.take_profit
            sl_hit = bracket.stop_loss is not None and quote.price <= bracket.stop_loss
            if not (tp_hit or sl_hit):
                continue

            del self._brackets[bracket.entry_order_id]
            held = ctx.holding(bracket.symbol)
            qty = min(bracket.quantity, held.quantity if held else Decimal(0))
            if qty <= 0:
                logger.warning("bracket hit but no holdings / %s", bracket.symbol)
                continue
            logger.info("bracket %s / %s price=%s", "TAKE-PROFIT" if tp_hit else "STOP-LOSS",
                        bracket.symbol, quote.price)
            signals.append(Sell(symbol=bracket.symbol, quantity=qty))
        return signals

    def _resolve_entry(self, bracket: _Bracket) -> _Bracket | None:
        try:
            order = self._broker.get_order(bracket.entry_order_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("bracket entry lookup failed / %s: %s", bracket.entry_order_id, e)
            return bracket
        if order.status == OrderStatus.FILLED:
            updated = replace(bracket, active=True)
            self._brackets[bracket.entry_order_id] = updated
            logger.info("bracket activated / entry filled %s", bracket.entry_order_id)
            return updated
        if not order.status.is_open:
            del self._brackets[bracket.entry_order_id]
            logger.info("bracket dropped / entry %s %s", order.status.value, bracket.entry_order_id)
            return None
        return bracket

    @property
    def active_count(self) -> int:
        return len(self._brackets)


class OrderExecutor:
    """Signal -> 주문 실행. 매도 클램프(공매도 방지), 멱등키(지원 브로커만)."""

    def __init__(self, broker: BrokerClient, brackets: BracketMonitor, guard: TradingGuard):
        self._broker = broker
        self._brackets = brackets
        self._guard = guard

    def execute(self, strategy_name: str, signals: list[Signal], ctx: StrategyContext) -> None:
        for signal in signals:
            if self._guard.is_halted:
                logger.warning("[%s] halted - signal skipped: %s", strategy_name, signal)
                continue
            try:
                if isinstance(signal, Buy):
                    self._buy(strategy_name, signal)
                elif isinstance(signal, Sell):
                    self._sell(strategy_name, signal, ctx)
                elif isinstance(signal, Cancel):
                    order = self._broker.cancel_order(signal.order_id)
                    logger.info("[%s] CANCEL / %s -> %s", strategy_name, signal.order_id, order.status.value)
            except InsufficientFundsError:
                logger.warning("[%s] 주문가능금액 부족으로 시그널 스킵: %s", strategy_name, signal)
            except Exception as e:  # noqa: BLE001 - 시그널 단위 격리
                logger.error("[%s] signal 실행 실패 %s: %s", strategy_name, signal, e)

    def _buy(self, strategy_name: str, signal: Buy) -> None:
        order = self._broker.create_order(CreateOrderRequest(
            symbol=signal.symbol, side=OrderSide.BUY, order_type=signal.order_type,
            quantity=signal.quantity, limit_price=signal.limit_price,
            time_in_force=signal.time_in_force,
            client_order_id=self._client_order_id(strategy_name),
        ))
        logger.info("[%s] BUY 접수 / %s qty=%s type=%s orderId=%s",
                    strategy_name, signal.symbol, signal.quantity, signal.order_type.value, order.order_id)
        self._brackets.register(order.order_id, signal.symbol, signal.quantity,
                                signal.take_profit_price, signal.stop_loss_price)

    def _sell(self, strategy_name: str, signal: Sell, ctx: StrategyContext) -> None:
        held = ctx.holding(signal.symbol)
        qty = min(signal.quantity, held.quantity if held else Decimal(0))
        if qty <= 0:
            logger.warning("[%s] SELL 스킵 / %s 보유 수량 없음", strategy_name, signal.symbol)
            return
        order = self._broker.create_order(CreateOrderRequest(
            symbol=signal.symbol, side=OrderSide.SELL, order_type=signal.order_type,
            quantity=qty, limit_price=signal.limit_price, time_in_force=signal.time_in_force,
            client_order_id=self._client_order_id(strategy_name),
        ))
        logger.info("[%s] SELL 접수 / %s qty=%s orderId=%s", strategy_name, signal.symbol, qty, order.order_id)

    def _client_order_id(self, strategy_name: str) -> str | None:
        if not self._broker.capabilities.client_order_id:
            return None
        return f"{strategy_name}-{uuid.uuid4().hex[:8]}"


class StrategyEngine:
    """등록된 전략들을 각자의 poll_interval 로 순차 호출한다."""

    def __init__(self, broker: BrokerClient, strategies: list[Strategy],
                 max_consecutive_failures: int = 5):
        self.broker = broker
        self.guard = TradingGuard(broker, max_consecutive_failures)
        self.brackets = BracketMonitor(broker)
        self.executor = OrderExecutor(broker, self.brackets, self.guard)
        self.calendar = MarketCalendar(broker)
        self._stop = threading.Event()

        caps = broker.capabilities
        self.strategies = []
        for strategy in strategies:
            # capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
            if strategy.spec.candle_interval not in caps.candle_intervals:
                logger.error(
                    "[%s] 스케줄 제외: 브로커 '%s' 는 %s 캔들을 지원하지 않습니다 (지원: %s)",
                    strategy.spec.name, caps.broker_id, strategy.spec.candle_interval.value,
                    [i.value for i in caps.candle_intervals])
                continue
            self.strategies.append(strategy)
        logger.info("broker=%s market=%s / strategies=%s",
                    caps.broker_id, caps.market, [s.spec.name for s in self.strategies])

    def run(self) -> None:
        """블로킹 실행 루프. stop() 또는 KeyboardInterrupt 로 종료."""
        next_run = {s.spec.name: 0.0 for s in self.strategies}
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                for strategy in self.strategies:
                    if now >= next_run[strategy.spec.name]:
                        self.tick(strategy)
                        next_run[strategy.spec.name] = time.monotonic() + strategy.spec.poll_interval_seconds
                self._stop.wait(1.0)
        except KeyboardInterrupt:
            logger.info("interrupted - engine stopping")

    def stop(self) -> None:
        self._stop.set()

    def tick(self, strategy: Strategy) -> None:
        spec = strategy.spec
        try:
            if self.guard.is_halted:
                return
            if spec.regular_hours_only and not self.calendar.is_regular_open():
                logger.debug("[%s] market closed - tick skipped", spec.name)
                return

            ctx = self._build_context(strategy)

            bracket_signals = self.brackets.check(ctx)  # 익절/손절이 전략 판단보다 우선
            if bracket_signals:
                self.executor.execute(spec.name, list(bracket_signals), ctx)

            signals = strategy.decide(ctx)
            if signals:
                self.executor.execute(spec.name, signals, ctx)

            self.guard.record_success()
        except MarketClosedError as e:
            logger.debug("[%s] market closed - %s", spec.name, e)  # 휴장 - 실패 아님
        except RateLimitError as e:
            logger.warning("[%s] rate limited - %s", spec.name, e)  # 다음 틱에 회복
        except Exception as e:  # noqa: BLE001
            logger.exception("[%s] tick failed", spec.name)
            self.guard.record_failure(e)

    def _build_context(self, strategy: Strategy) -> StrategyContext:
        spec = strategy.spec
        return StrategyContext(
            now=datetime.now(timezone.utc),
            quotes={q.symbol: q for q in self.broker.get_quotes(spec.symbols)},
            candles={s: self.broker.get_candles(s, spec.candle_interval, spec.candle_limit)
                     for s in spec.symbols},
            account=self.broker.get_account(),
            holdings={h.symbol: h for h in self.broker.get_holdings()},
            open_orders=[o for o in self.broker.get_orders() if o.status.is_open],
            buying_power=self.broker.get_buying_power(),
        )


def pnl_report(broker: BrokerClient, initial_capital: Decimal | None = None) -> dict:
    """계좌 수익률 리포트 (Kotlin PnlService 대응)."""
    account = broker.get_account()
    holdings = broker.get_holdings()
    total_mv = sum((h.market_value or Decimal(0)) for h in holdings)
    total_pnl = sum((h.unrealized_pnl or Decimal(0)) for h in holdings)
    total_return = None
    if initial_capital and initial_capital > 0:
        total_return = (account.portfolio_value - initial_capital) / initial_capital
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": account.account_id,
        "currency": account.currency,
        "cash": account.cash,
        "portfolio_value": account.portfolio_value,
        "total_market_value": total_mv,
        "total_unrealized_pnl": total_pnl,
        "total_return_rate": total_return,
        "holdings": holdings,
    }
