/**
 * Larry Williams 식 변동성 돌파 전략 (롱 온리, 다중 종목).
 * 직전 완성 캔들 몸통 >= 평균 몸통 x multiplier 인 양봉 -> 시장가 매수 (손절=시가, 브라켓 위임).
 *
 * 실행: HERMETIX_BROKER=next NEXT_CLIENT_ID=... node dist/examples/larry.js
 */
import { Decimal } from "decimal.js";
import type { CandleInterval, Signal, Strategy, StrategyContext, StrategySpec } from "../src/index.js";

export interface LarryOptions {
  symbols?: string[];
  lookback?: number;
  multiplier?: Decimal;
  expireHours?: number;
  budgetRatio?: Decimal;
  candleInterval?: CandleInterval;
}

export class LarryStrategy implements Strategy {
  readonly spec: StrategySpec;
  private readonly lookback: number;
  private readonly multiplier: Decimal;
  private readonly expireHours: number;
  private readonly budgetRatio: Decimal;
  readonly lastEvaluated = new Map<string, number>();
  readonly entryAt = new Map<string, number>();

  constructor(opts: LarryOptions = {}) {
    const symbols = opts.symbols ?? ["AAPL"];
    this.lookback = opts.lookback ?? 24;
    this.multiplier = opts.multiplier ?? new Decimal("1.2");
    this.expireHours = opts.expireHours ?? 48;
    this.budgetRatio = opts.budgetRatio ?? new Decimal("0.5");
    this.spec = {
      name: "larry", symbols,
      candleInterval: opts.candleInterval ?? "1h",
      candleLimit: this.lookback + 2,
    };
  }

  decide(ctx: StrategyContext): Signal[] {
    return this.spec.symbols.flatMap((symbol) => this.decideSymbol(symbol, ctx));
  }

  private decideSymbol(symbol: string, ctx: StrategyContext): Signal[] {
    if (ctx.hasPosition(symbol)) return this.decideExit(symbol, ctx);
    this.entryAt.delete(symbol);
    if (ctx.hasOpenOrder(symbol)) return [];
    return this.decideEntry(symbol, ctx);
  }

  private decideEntry(symbol: string, ctx: StrategyContext): Signal[] {
    const candles = ctx.candlesOf(symbol);
    if (candles.length < this.lookback + 2) return [];
    const completed = candles.slice(0, -1); // 마지막 캔들은 진행 중
    const target = completed[completed.length - 1];
    if (this.lastEvaluated.get(symbol) === target.timestamp.getTime()) return [];
    this.lastEvaluated.set(symbol, target.timestamp.getTime());

    const history = completed.slice(-(this.lookback + 1), -1);
    const avgBody = history
      .reduce((acc, c) => acc.plus(c.open.minus(c.close).abs()), new Decimal(0))
      .div(history.length);
    if (target.open.minus(target.close).abs().lt(avgBody.mul(this.multiplier))) return [];
    if (target.open.gte(target.close)) return []; // 롱 온리 - 음봉 스킵

    const quote = ctx.quote(symbol);
    if (!quote) return [];
    const budget = ctx.buyingPower.mul(this.budgetRatio).div(this.spec.symbols.length);
    const quantity = budget.div(quote.price).floor();
    if (quantity.lt(1)) return [];

    this.entryAt.set(symbol, ctx.now.getTime());
    return [{ kind: "buy", symbol, quantity, stopLossPrice: target.open }];
  }

  private decideExit(symbol: string, ctx: StrategyContext): Signal[] {
    const openedAt = this.entryAt.get(symbol) ?? ctx.now.getTime();
    this.entryAt.set(symbol, openedAt); // 재시작 시 만료 클록 재시작
    if (ctx.now.getTime() - openedAt < this.expireHours * 3600_000) return [];
    const holding = ctx.holding(symbol);
    if (!holding) return [];
    this.entryAt.delete(symbol);
    return [{ kind: "sell", symbol, quantity: holding.quantity }];
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { run } = await import("./_runner.js");
  await run(new LarryStrategy());
}
