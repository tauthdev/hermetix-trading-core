/** 예제 전략 3종 핵심 시나리오 - 다른 언어 구현과 동일 정답지. */
import assert from "node:assert/strict";
import { test } from "node:test";
import { GridStrategy } from "../examples/grid.js";
import { LarryStrategy } from "../examples/larry.js";
import { TrendBreakoutStrategy, TrendLine } from "../examples/trendBreakout.js";
import type { Candle, Order, OrderSide } from "../src/index.js";
import { Decimal, StrategyContext } from "../src/index.js";

const candle = (h: number, open: string, close: string, high?: string, low?: string, volume = 1000): Candle => {
  const o = new Decimal(open), c = new Decimal(close);
  return {
    timestamp: new Date(Date.UTC(2026, 7, 6, h)),
    open: o, close: c,
    high: high ? new Decimal(high) : Decimal.max(o, c),
    low: low ? new Decimal(low) : Decimal.min(o, c),
    volume,
  };
};

const ctx = (opts: {
  candles?: Candle[]; price?: string; heldQty?: string; avgEntry?: string;
  openOrders?: Order[]; now?: Date;
} = {}) => new StrategyContext(
  opts.now ?? new Date(),
  new Map(opts.price ? [["AAPL", {
    symbol: "AAPL", price: new Decimal(opts.price), bidPrice: null, askPrice: null,
    volume: 0, change: null, changeRate: null, timestamp: new Date(),
  }]] : []),
  new Map(opts.candles ? [["AAPL", opts.candles]] : []),
  { accountId: "a", currency: "USD", cash: new Decimal(10000), portfolioValue: new Decimal(10000), status: "ACTIVE" },
  new Map(opts.heldQty ? [["AAPL", {
    symbol: "AAPL", quantity: new Decimal(opts.heldQty), avgEntryPrice: new Decimal(opts.avgEntry ?? "100"),
  }]] : []),
  opts.openOrders ?? [],
  new Decimal(10000),
);

const openOrder = (side: OrderSide, limit: string): Order =>
  ({ orderId: "ord_1", status: "SUBMITTED", symbol: "AAPL", side, orderType: "LIMIT", limitPrice: new Decimal(limit) });

// ------------------------------------------------------------------- larry

const larryCandles = (targetOpen: string, targetClose: string, lookback = 5): Candle[] => [
  ...Array.from({ length: lookback }, (_, i) => candle(i, "100", "101")),
  candle(lookback, targetOpen, targetClose),
  candle(lookback + 1, targetClose, targetClose),
];

test("larry: 돌파 양봉이면 시가를 손절로 매수", () => {
  const s = new LarryStrategy({ symbols: ["AAPL"], lookback: 5 });
  const signals = s.decide(ctx({ candles: larryCandles("100", "108"), price: "108" }));
  assert.equal(signals.length, 1);
  const buy = signals[0] as { kind: string; quantity: Decimal; stopLossPrice: Decimal };
  assert.equal(buy.kind, "buy");
  assert.equal(buy.stopLossPrice.toString(), "100");
  assert.equal(buy.quantity.toString(), "46"); // 10000*0.5/108
});

test("larry: 음봉 돌파 스킵 + 중복 진입 방지", () => {
  const s = new LarryStrategy({ symbols: ["AAPL"], lookback: 5 });
  assert.equal(s.decide(ctx({ candles: larryCandles("108", "100"), price: "100" })).length, 0);
  const s2 = new LarryStrategy({ symbols: ["AAPL"], lookback: 5 });
  const data = larryCandles("100", "108");
  assert.equal(s2.decide(ctx({ candles: data, price: "108" })).length, 1);
  assert.equal(s2.decide(ctx({ candles: data, price: "108" })).length, 0);
});

test("larry: 만료 청산", () => {
  const s = new LarryStrategy({ symbols: ["AAPL"], lookback: 5, expireHours: 48 });
  const now = new Date();
  s.entryAt.set("AAPL", now.getTime() - 49 * 3600_000);
  const signals = s.decide(ctx({ candles: larryCandles("100", "101"), heldQty: "46", now }));
  assert.equal(signals.length, 1);
  assert.equal((signals[0] as { kind: string }).kind, "sell");
});

// ---------------------------------------------------------- trend breakout

const trendCandles = (count: number, startHigh: number, step: number): Candle[] =>
  Array.from({ length: count + 1 }, (_, i) => {
    const high = startHigh + step * i;
    return candle(i, String(high - 2), String(high - 1), String(high), String(high - 4));
  });

test("trend: 상승 추세 돌파 시 브라켓 매수", () => {
  const s = new TrendBreakoutStrategy({ symbols: ["AAPL"], lookback: 10 });
  const data = trendCandles(10, 100, 1);
  const trend = TrendLine.of(data.slice(0, -1).slice(-10));
  const signals = s.decide(ctx({ candles: data, price: trend.breakoutLine.plus(1).toString() }));
  assert.equal(signals.length, 1);
  const buy = signals[0] as { kind: string; takeProfitPrice: Decimal; stopLossPrice: Decimal };
  assert.equal(buy.kind, "buy");
  assert.ok(buy.takeProfitPrice && buy.stopLossPrice);
});

test("trend: 하락 추세는 돌파해도 스킵", () => {
  const s = new TrendBreakoutStrategy({ symbols: ["AAPL"], lookback: 10 });
  assert.equal(s.decide(ctx({ candles: trendCandles(10, 110, -1), price: "999" })).length, 0);
});

// -------------------------------------------------------------------- grid

test("grid: 딥 지정가 매수", () => {
  const s = new GridStrategy({ symbols: ["AAPL"] });
  const signals = s.decide(ctx({ price: "100" }));
  const buy = signals[0] as { kind: string; limitPrice: Decimal };
  assert.equal(buy.kind, "buy");
  assert.equal(buy.limitPrice.toString(), "99.7");
});

test("grid: 가격 이탈 시 추격 취소", () => {
  const s = new GridStrategy({ symbols: ["AAPL"] });
  const signals = s.decide(ctx({ price: "102", openOrders: [openOrder("BUY", "99.70")] }));
  assert.equal((signals[0] as { kind: string }).kind, "cancel");
});

test("grid: 목표가 매도 + 감쇠 + 수익권 시장가 청산", () => {
  const s = new GridStrategy({ symbols: ["AAPL"], decayMinutes: 30 });
  // 목표가 = 100 * 1.03
  const sell = s.decide(ctx({ price: "100", heldQty: "50" }))[0] as { limitPrice: Decimal };
  assert.equal(sell.limitPrice.toString(), "103");

  // 감쇠: count 1 -> 0
  const now = new Date();
  s.decayCount.set("AAPL", 1);
  s.sellPlacedAt.set("AAPL", now.getTime() - 31 * 60_000);
  const decay = s.decide(ctx({ price: "100", heldQty: "50", openOrders: [openOrder("SELL", "101")], now }));
  assert.equal((decay[0] as { kind: string }).kind, "cancel");
  assert.equal(s.decayCount.get("AAPL"), 0);

  // count 0 + 수익권 -> 시장가
  const market = s.decide(ctx({ price: "100.5", heldQty: "50", avgEntry: "100", now }))[0] as { orderType?: string };
  assert.equal(market.orderType ?? "MARKET", "MARKET");
});
