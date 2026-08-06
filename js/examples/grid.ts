/**
 * 목표가 스캘핑 전략 (다중 종목, 캔들 미사용 - KRX 브로커 호환).
 * 딥 지정가 매수(추격) -> 목표가 매도(GTC) -> 감쇠(count--) -> 수익권 시장가 청산.
 *
 * 실행: HERMETIX_BROKER=kis KIS_APPKEY=... node dist/examples/grid.js
 */
import { Decimal } from "decimal.js";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "../src/index.js";

export interface GridOptions {
  symbols?: string[];
  buyDipRate?: Decimal;
  targetRate?: Decimal;
  maxDecayCount?: number;
  decayMinutes?: number;
  chaseRate?: Decimal;
  budgetRatio?: Decimal;
  pollSeconds?: number;
}

export class GridStrategy implements Strategy {
  readonly spec: StrategySpec;
  private readonly buyDipRate: Decimal;
  private readonly targetRate: Decimal;
  private readonly maxDecayCount: number;
  private readonly decayMinutes: number;
  private readonly chaseRate: Decimal;
  private readonly budgetRatio: Decimal;
  readonly decayCount = new Map<string, number>();
  readonly sellPlacedAt = new Map<string, number>();

  constructor(opts: GridOptions = {}) {
    this.buyDipRate = opts.buyDipRate ?? new Decimal("0.003");
    this.targetRate = opts.targetRate ?? new Decimal("0.01");
    this.maxDecayCount = opts.maxDecayCount ?? 3;
    this.decayMinutes = opts.decayMinutes ?? 30;
    this.chaseRate = opts.chaseRate ?? new Decimal("0.005");
    this.budgetRatio = opts.budgetRatio ?? new Decimal("0.5");
    this.spec = {
      name: "grid",
      symbols: opts.symbols ?? ["AAPL"],
      candleInterval: "1d", candleLimit: 2, // 캔들 미사용
      pollIntervalSeconds: opts.pollSeconds ?? 30,
    };
  }

  decide(ctx: StrategyContext): Signal[] {
    return this.spec.symbols.flatMap((symbol) => this.decideSymbol(symbol, ctx));
  }

  private decideSymbol(symbol: string, ctx: StrategyContext): Signal[] {
    const quote = ctx.quote(symbol);
    if (!quote) return [];
    if (ctx.hasPosition(symbol)) return this.decideSell(symbol, quote.price, ctx);
    this.decayCount.set(symbol, this.maxDecayCount);
    this.sellPlacedAt.delete(symbol);
    return this.decideBuy(symbol, quote.price, ctx);
  }

  private decideBuy(symbol: string, price: Decimal, ctx: StrategyContext): Signal[] {
    const desired = price.mul(new Decimal(1).minus(this.buyDipRate)).toDecimalPlaces(2);
    const openBuy = ctx.openOrdersOf(symbol).find((o) => o.side === "BUY");
    if (openBuy) {
      if (openBuy.limitPrice && desired.gt(openBuy.limitPrice.mul(new Decimal(1).plus(this.chaseRate)))) {
        return [{ kind: "cancel", orderId: openBuy.orderId }]; // 가격 이탈 - 추격
      }
      return [];
    }
    const budget = ctx.buyingPower.mul(this.budgetRatio).div(this.spec.symbols.length);
    const quantity = budget.div(desired).floor();
    if (quantity.lt(1)) return [];
    return [{ kind: "buy", symbol, quantity, orderType: "LIMIT", limitPrice: desired, timeInForce: "DAY" }];
  }

  private decideSell(symbol: string, price: Decimal, ctx: StrategyContext): Signal[] {
    const holding = ctx.holding(symbol);
    if (!holding) return [];
    const entry = holding.avgEntryPrice;
    const count = this.decayCount.get(symbol) ?? this.maxDecayCount;
    this.decayCount.set(symbol, count);

    const openSell = ctx.openOrdersOf(symbol).find((o) => o.side === "SELL");
    if (openSell) {
      const placedAt = this.sellPlacedAt.get(symbol) ?? ctx.now.getTime();
      this.sellPlacedAt.set(symbol, placedAt);
      if (count > 0 && ctx.now.getTime() - placedAt >= this.decayMinutes * 60_000) {
        this.decayCount.set(symbol, count - 1); // 목표 감쇠
        this.sellPlacedAt.delete(symbol);
        return [{ kind: "cancel", orderId: openSell.orderId }];
      }
      return [];
    }

    if (count <= 0 && price.gte(entry)) { // 수익권 - 즉시 시장가 청산
      return [{ kind: "sell", symbol, quantity: holding.quantity }];
    }

    const sellPrice = entry.mul(new Decimal(1).plus(this.targetRate.mul(Math.max(count, 0)))).toDecimalPlaces(2);
    this.sellPlacedAt.set(symbol, ctx.now.getTime());
    return [{ kind: "sell", symbol, quantity: holding.quantity, orderType: "LIMIT", limitPrice: sellPrice, timeInForce: "GTC" }];
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const { run } = await import("./_runner.js");
  await run(new GridStrategy({ symbols: ["005930", "000660"] }));
}
