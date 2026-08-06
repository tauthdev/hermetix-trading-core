/**
 * WMA 추세선 돌파 전략 (롱 온리, 다중 종목).
 * 예측고점+갭 돌파 & 상승 추세 -> 시장가 매수 (익절 +profitRate, 손절 지지선 - 브라켓 위임).
 *
 * 실행: HERMETIX_BROKER=next NEXT_CLIENT_ID=... node dist/examples/trendBreakout.js
 */
import { Decimal } from "decimal.js";
import type { Candle, CandleInterval, Signal, Strategy, StrategyContext, StrategySpec } from "../src/index.js";

export class TrendLine {
  constructor(
    readonly highPrice: Decimal,
    readonly lowPrice: Decimal,
    readonly highInclination: Decimal,
    readonly lowInclination: Decimal,
    readonly avgVolume: Decimal,
  ) {}

  get gap(): Decimal { return this.highPrice.minus(this.lowPrice); }
  get breakoutLine(): Decimal { return this.highPrice.plus(this.gap); }
  get supportLine(): Decimal { return this.lowPrice.minus(this.gap); }

  static of(candles: Candle[]): TrendLine {
    let weight = new Decimal(0);
    let volumeWeight = new Decimal(1);
    let highInc = new Decimal(0);
    let lowInc = new Decimal(0);
    let avgVolume = new Decimal(candles[0].volume);
    for (let i = 1; i < candles.length; i++) {
      weight = weight.plus(i);
      volumeWeight = volumeWeight.plus(i + 1);
      highInc = highInc.plus(candles[i].high.minus(candles[i - 1].high).mul(i));
      lowInc = lowInc.plus(candles[i].low.minus(candles[i - 1].low).mul(i));
      avgVolume = avgVolume.plus(new Decimal(candles[i].volume).mul(i + 1));
    }
    highInc = highInc.div(weight);
    lowInc = lowInc.div(weight);
    return new TrendLine(
      candles[candles.length - 1].high.plus(highInc),
      candles[candles.length - 1].low.plus(lowInc),
      highInc, lowInc, avgVolume.div(volumeWeight),
    );
  }
}

export interface TrendBreakoutOptions {
  symbols?: string[];
  lookback?: number;
  profitRate?: Decimal;
  volumeRatio?: Decimal;
  expireHours?: number;
  budgetRatio?: Decimal;
  candleInterval?: CandleInterval;
}

export class TrendBreakoutStrategy implements Strategy {
  readonly spec: StrategySpec;
  private readonly lookback: number;
  private readonly profitRate: Decimal;
  private readonly volumeRatio: Decimal;
  private readonly expireHours: number;
  private readonly budgetRatio: Decimal;
  readonly lastEntryCandle = new Map<string, number>();
  readonly entryAt = new Map<string, number>();

  constructor(opts: TrendBreakoutOptions = {}) {
    this.lookback = opts.lookback ?? 120;
    this.profitRate = opts.profitRate ?? new Decimal("0.04");
    this.volumeRatio = opts.volumeRatio ?? new Decimal(0);
    this.expireHours = opts.expireHours ?? 12;
    this.budgetRatio = opts.budgetRatio ?? new Decimal("0.5");
    this.spec = {
      name: "trend-breakout",
      symbols: opts.symbols ?? ["AAPL"],
      candleInterval: opts.candleInterval ?? "1h",
      candleLimit: this.lookback + 1,
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
    if (candles.length < this.lookback + 1) return [];
    const completed = candles.slice(0, -1);
    const inProgress = candles[candles.length - 1];
    if (this.lastEntryCandle.get(symbol) === inProgress.timestamp.getTime()) return [];

    const trend = TrendLine.of(completed.slice(-this.lookback));
    if (trend.highInclination.lte(0)) return []; // 상승 추세만
    const quote = ctx.quote(symbol);
    if (!quote || quote.price.lt(trend.breakoutLine)) return [];
    if (this.volumeRatio.gt(0) && new Decimal(inProgress.volume).lt(trend.avgVolume.mul(this.volumeRatio))) return [];

    const budget = ctx.buyingPower.mul(this.budgetRatio).div(this.spec.symbols.length);
    const quantity = budget.div(quote.price).floor();
    if (quantity.lt(1)) return [];

    this.lastEntryCandle.set(symbol, inProgress.timestamp.getTime());
    this.entryAt.set(symbol, ctx.now.getTime());
    return [{
      kind: "buy", symbol, quantity,
      takeProfitPrice: quote.price.mul(new Decimal(1).plus(this.profitRate)).toDecimalPlaces(2),
      stopLossPrice: trend.supportLine.toDecimalPlaces(2),
    }];
  }

  private decideExit(symbol: string, ctx: StrategyContext): Signal[] {
    const openedAt = this.entryAt.get(symbol) ?? ctx.now.getTime();
    this.entryAt.set(symbol, openedAt);
    if (ctx.now.getTime() - openedAt < this.expireHours * 3600_000) return [];
    const holding = ctx.holding(symbol);
    if (!holding) return [];
    this.entryAt.delete(symbol);
    return [{ kind: "sell", symbol, quantity: holding.quantity }];
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { run } = await import("./_runner.js");
  await run(new TrendBreakoutStrategy());
}
