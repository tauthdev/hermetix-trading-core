/** 전략 실행 엔진 - 매 틱: 장시간 확인 -> 스냅샷 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행. */
import { Decimal } from "decimal.js";
import type { BrokerClient } from "./broker.js";
import { sleep } from "./broker.js";
import { InsufficientFundsError, MarketClosedError, RateLimitError } from "./errors.js";
import type { Holding, MarketDay, Order } from "./models.js";
import { isOpenStatus } from "./models.js";
import type { Buy, Sell, Signal, Strategy } from "./strategy.js";
import { StrategyContext } from "./strategy.js";

const log = {
  info: (msg: string) => console.log(`INFO hermetix ${msg}`),
  warn: (msg: string) => console.warn(`WARN hermetix ${msg}`),
  error: (msg: string) => console.error(`ERROR hermetix ${msg}`),
  debug: (msg: string) => { if (process.env.HERMETIX_DEBUG) console.log(`DEBUG hermetix ${msg}`); },
};

/** 개장 판단 - 브로커 캘린더를 6시간 캐시. */
export class MarketCalendar {
  private cache: MarketDay[] | null = null;
  private cachedAt = 0;

  constructor(private readonly broker: BrokerClient) {}

  async isRegularOpen(now = new Date()): Promise<boolean> {
    const days = await this.calendar();
    if (days.length === 0) return false;
    const zone = days[0].timezone;
    const date = new Intl.DateTimeFormat("en-CA", { timeZone: zone }).format(now);
    const today = days.find((d) => d.date === date);
    if (!today?.open || !today.regular) return false;
    const hhmm = new Intl.DateTimeFormat("en-GB", {
      timeZone: zone, hour: "2-digit", minute: "2-digit", hour12: false,
    }).format(now);
    return today.regular.start <= hhmm && hhmm < today.regular.end;
  }

  private async calendar(): Promise<MarketDay[]> {
    if (!this.cache || Date.now() - this.cachedAt > 6 * 3600_000) {
      this.cache = await this.broker.getCalendar();
      this.cachedAt = Date.now();
      log.info(`market calendar refreshed / days=${this.cache.length}`);
    }
    return this.cache;
  }
}

/** 비상정지 - 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단. */
export class TradingGuard {
  private failures = 0;
  private halted = false;

  constructor(private readonly broker: BrokerClient, private readonly maxFailures = 5) {}

  get isHalted(): boolean { return this.halted; }

  recordSuccess(): void { this.failures = 0; }

  async recordFailure(cause: unknown): Promise<void> {
    this.failures++;
    log.warn(`engine failure ${this.failures}/${this.maxFailures} - ${cause}`);
    if (this.failures >= this.maxFailures && !this.halted) await this.halt(`연속 실패 ${this.failures}회`);
  }

  async halt(reason: string): Promise<void> {
    if (this.halted) return;
    this.halted = true;
    log.error(`TRADING HALTED / ${reason} - 미체결 전량 취소`);
    try {
      for (const order of await this.broker.getOrders()) {
        if (!isOpenStatus(order.status)) continue;
        try {
          await this.broker.cancelOrder(order.orderId);
          log.info(`halt-cancel ok / ${order.orderId}`);
        } catch (e) {
          log.error(`halt-cancel failed / ${order.orderId}: ${e}`);
        }
      }
    } catch (e) {
      log.error(`halt: open order lookup failed: ${e}`);
    }
  }

  resume(): void {
    this.failures = 0;
    this.halted = false;
    log.info("trading resumed");
  }
}

interface Bracket {
  entryOrderId: string;
  symbol: string;
  quantity: Decimal;
  takeProfit: Decimal | null;
  stopLoss: Decimal | null;
  active: boolean;
}

/** 소프트웨어 익절/손절. 상태는 메모리에만 (재시작 시 소실). */
export class BracketMonitor {
  private readonly brackets = new Map<string, Bracket>();

  constructor(private readonly broker: BrokerClient) {}

  register(entryOrderId: string, symbol: string, quantity: Decimal,
           takeProfit: Decimal | null, stopLoss: Decimal | null): void {
    if (!takeProfit && !stopLoss) return;
    this.brackets.set(entryOrderId, { entryOrderId, symbol, quantity, takeProfit, stopLoss, active: false });
    log.info(`bracket registered / ${entryOrderId} ${symbol} qty=${quantity} tp=${takeProfit} sl=${stopLoss}`);
  }

  async check(ctx: StrategyContext): Promise<Sell[]> {
    const signals: Sell[] = [];
    for (let bracket of [...this.brackets.values()]) {
      if (!bracket.active) {
        const resolved = await this.resolveEntry(bracket);
        if (!resolved?.active) continue;
        bracket = resolved;
      }
      const quote = ctx.quote(bracket.symbol);
      if (!quote) continue;
      const tpHit = bracket.takeProfit !== null && quote.price.gte(bracket.takeProfit);
      const slHit = bracket.stopLoss !== null && quote.price.lte(bracket.stopLoss);
      if (!tpHit && !slHit) continue;

      this.brackets.delete(bracket.entryOrderId);
      const held = ctx.holding(bracket.symbol)?.quantity ?? new Decimal(0);
      const qty = Decimal.min(bracket.quantity, held);
      if (qty.lte(0)) {
        log.warn(`bracket hit but no holdings / ${bracket.symbol}`);
        continue;
      }
      log.info(`bracket ${tpHit ? "TAKE-PROFIT" : "STOP-LOSS"} / ${bracket.symbol} price=${quote.price}`);
      signals.push({ kind: "sell", symbol: bracket.symbol, quantity: qty });
    }
    return signals;
  }

  private async resolveEntry(bracket: Bracket): Promise<Bracket | null> {
    let order: Order;
    try {
      order = await this.broker.getOrder(bracket.entryOrderId);
    } catch (e) {
      log.warn(`bracket entry lookup failed / ${bracket.entryOrderId}: ${e}`);
      return bracket;
    }
    if (order.status === "FILLED") {
      const updated = { ...bracket, active: true };
      this.brackets.set(bracket.entryOrderId, updated);
      log.info(`bracket activated / entry filled ${bracket.entryOrderId}`);
      return updated;
    }
    if (!isOpenStatus(order.status)) {
      this.brackets.delete(bracket.entryOrderId);
      log.info(`bracket dropped / entry ${order.status} ${bracket.entryOrderId}`);
      return null;
    }
    return bracket;
  }

  get activeCount(): number { return this.brackets.size; }
}

/** Signal -> 주문 실행. 매도 클램프(공매도 방지), 멱등키(지원 브로커만). */
export class OrderExecutor {
  constructor(
    private readonly broker: BrokerClient,
    private readonly brackets: BracketMonitor,
    private readonly guard: TradingGuard,
  ) {}

  async execute(strategyName: string, signals: Signal[], ctx: StrategyContext): Promise<void> {
    for (const signal of signals) {
      if (this.guard.isHalted) {
        log.warn(`[${strategyName}] halted - signal skipped`);
        continue;
      }
      try {
        if (signal.kind === "buy") await this.buy(strategyName, signal);
        else if (signal.kind === "sell") await this.sell(strategyName, signal, ctx);
        else {
          const order = await this.broker.cancelOrder(signal.orderId);
          log.info(`[${strategyName}] CANCEL / ${signal.orderId} -> ${order.status}`);
        }
      } catch (e) {
        if (e instanceof InsufficientFundsError) {
          log.warn(`[${strategyName}] 주문가능금액 부족으로 시그널 스킵`);
        } else {
          log.error(`[${strategyName}] signal 실행 실패: ${e}`);
        }
      }
    }
  }

  private async buy(strategyName: string, signal: Buy): Promise<void> {
    const order = await this.broker.createOrder({
      symbol: signal.symbol, side: "BUY",
      orderType: signal.orderType ?? "MARKET",
      quantity: signal.quantity,
      limitPrice: signal.limitPrice ?? null,
      timeInForce: signal.timeInForce ?? "DAY",
      clientOrderId: this.clientOrderId(strategyName),
    });
    log.info(`[${strategyName}] BUY 접수 / ${signal.symbol} qty=${signal.quantity} orderId=${order.orderId}`);
    this.brackets.register(order.orderId, signal.symbol, signal.quantity,
      signal.takeProfitPrice ?? null, signal.stopLossPrice ?? null);
  }

  private async sell(strategyName: string, signal: Sell, ctx: StrategyContext): Promise<void> {
    const held = ctx.holding(signal.symbol)?.quantity ?? new Decimal(0);
    const qty = Decimal.min(signal.quantity, held);
    if (qty.lte(0)) {
      log.warn(`[${strategyName}] SELL 스킵 / ${signal.symbol} 보유 수량 없음`);
      return;
    }
    const order = await this.broker.createOrder({
      symbol: signal.symbol, side: "SELL",
      orderType: signal.orderType ?? "MARKET",
      quantity: qty,
      limitPrice: signal.limitPrice ?? null,
      timeInForce: signal.timeInForce ?? "DAY",
      clientOrderId: this.clientOrderId(strategyName),
    });
    log.info(`[${strategyName}] SELL 접수 / ${signal.symbol} qty=${qty} orderId=${order.orderId}`);
  }

  private clientOrderId(strategyName: string): string | null {
    if (!this.broker.capabilities.clientOrderId) return null;
    return `${strategyName}-${Math.random().toString(36).slice(2, 10)}`;
  }
}

/** 등록된 전략들을 각자의 pollInterval 로 순차 호출한다. */
export class StrategyEngine {
  readonly guard: TradingGuard;
  readonly brackets: BracketMonitor;
  readonly executor: OrderExecutor;
  readonly calendar: MarketCalendar;
  readonly strategies: Strategy[];
  private stopped = false;

  constructor(readonly broker: BrokerClient, strategies: Strategy[], maxConsecutiveFailures = 5) {
    this.guard = new TradingGuard(broker, maxConsecutiveFailures);
    this.brackets = new BracketMonitor(broker);
    this.executor = new OrderExecutor(broker, this.brackets, this.guard);
    this.calendar = new MarketCalendar(broker);

    const caps = broker.capabilities;
    this.strategies = strategies.filter((s) => {
      const interval = s.spec.candleInterval ?? "1d";
      if (!caps.candleIntervals.has(interval)) {
        // capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
        log.error(`[${s.spec.name}] 스케줄 제외: 브로커 '${caps.brokerId}' 는 ${interval} 캔들을 지원하지 않습니다 ` +
          `(지원: ${[...caps.candleIntervals].join(",")})`);
        return false;
      }
      return true;
    });
    log.info(`broker=${caps.brokerId} market=${caps.market} / strategies=${this.strategies.map((s) => s.spec.name).join(",")}`);
  }

  /** 블로킹 실행 루프. stop() 으로 종료. */
  async run(): Promise<void> {
    const nextRun = new Map(this.strategies.map((s) => [s.spec.name, 0]));
    while (!this.stopped) {
      for (const strategy of this.strategies) {
        if (Date.now() >= (nextRun.get(strategy.spec.name) ?? 0)) {
          await this.tick(strategy);
          nextRun.set(strategy.spec.name, Date.now() + (strategy.spec.pollIntervalSeconds ?? 60) * 1000);
        }
      }
      await sleep(1000);
    }
  }

  stop(): void { this.stopped = true; }

  async tick(strategy: Strategy): Promise<void> {
    const spec = strategy.spec;
    try {
      if (this.guard.isHalted) return;
      if ((spec.regularHoursOnly ?? true) && !(await this.calendar.isRegularOpen())) {
        log.debug(`[${spec.name}] market closed - tick skipped`);
        return;
      }
      const ctx = await this.buildContext(strategy);

      const bracketSignals = await this.brackets.check(ctx); // 익절/손절이 전략보다 우선
      if (bracketSignals.length > 0) await this.executor.execute(spec.name, bracketSignals, ctx);

      const signals = await strategy.decide(ctx);
      if (signals.length > 0) await this.executor.execute(spec.name, signals, ctx);

      this.guard.recordSuccess();
    } catch (e) {
      if (e instanceof MarketClosedError) {
        log.debug(`[${spec.name}] market closed - ${e.message}`); // 휴장 - 실패 아님
      } else if (e instanceof RateLimitError) {
        log.warn(`[${spec.name}] rate limited - ${e.message}`);
      } else {
        log.error(`[${spec.name}] tick failed: ${e}`);
        await this.guard.recordFailure(e);
      }
    }
  }

  private async buildContext(strategy: Strategy): Promise<StrategyContext> {
    const spec = strategy.spec;
    const candles = new Map<string, Awaited<ReturnType<BrokerClient["getCandles"]>>>();
    for (const symbol of spec.symbols) {
      candles.set(symbol, await this.broker.getCandles(symbol, spec.candleInterval ?? "1d", spec.candleLimit ?? 30));
    }
    return new StrategyContext(
      new Date(),
      new Map((await this.broker.getQuotes(spec.symbols)).map((q) => [q.symbol, q])),
      candles,
      await this.broker.getAccount(),
      new Map((await this.broker.getHoldings()).map((h) => [h.symbol, h])),
      (await this.broker.getOrders()).filter((o) => isOpenStatus(o.status)),
      await this.broker.getBuyingPower(),
    );
  }
}

export interface PnlReport {
  timestamp: string;
  accountId: string;
  currency: string;
  cash: Decimal;
  portfolioValue: Decimal;
  totalMarketValue: Decimal;
  totalUnrealizedPnl: Decimal;
  totalReturnRate: Decimal | null;
  holdings: Holding[];
}

/** 계좌 수익률 리포트. */
export async function pnlReport(broker: BrokerClient, initialCapital?: Decimal): Promise<PnlReport> {
  const account = await broker.getAccount();
  const holdings = await broker.getHoldings();
  const zero = new Decimal(0);
  const totalMv = holdings.reduce((acc, h) => acc.plus(h.marketValue ?? zero), zero);
  const totalPnl = holdings.reduce((acc, h) => acc.plus(h.unrealizedPnl ?? zero), zero);
  const totalReturn = initialCapital?.gt(0)
    ? account.portfolioValue.minus(initialCapital).div(initialCapital)
    : null;
  return {
    timestamp: new Date().toISOString(),
    accountId: account.accountId,
    currency: account.currency,
    cash: account.cash,
    portfolioValue: account.portfolioValue,
    totalMarketValue: totalMv,
    totalUnrealizedPnl: totalPnl,
    totalReturnRate: totalReturn,
    holdings,
  };
}
