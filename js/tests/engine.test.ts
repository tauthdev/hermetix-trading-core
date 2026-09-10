/** 엔진 로직 검증 - Kotlin/Python 레퍼런스와 동일 시나리오. */
import assert from "node:assert/strict";
import { test } from "node:test";
import type { BrokerClient } from "../src/broker.js";
import { BracketMonitor, OrderExecutor, RiskGuard, StrategyEngine, TradingGuard } from "../src/engine.js";
import { Decimal } from "../src/models.js";
import type { CreateOrderRequest, Order, OrderStatus, TradingEnvironment } from "../src/models.js";
import { StrategyContext, buy, sell } from "../src/strategy.js";

class FakeBroker implements BrokerClient {
  capabilities = {
    brokerId: "fake", market: "US" as const, currency: "USD",
    candleIntervals: new Set(["1d" as const]),
    clientOrderId: true, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
  };
  environment: TradingEnvironment = "PAPER";
  created: CreateOrderRequest[] = [];
  canceled: string[] = [];
  orderStatus: OrderStatus = "SUBMITTED";
  openOrders: Order[] = [];

  async getQuotes() { return []; }
  async getCandles() { return []; }
  async getCalendar() { return []; }
  async getAccount() { throw new Error("unused"); return undefined as never; }
  async getHoldings() { return []; }
  async getBuyingPower() { return new Decimal(0); }
  async createOrder(request: CreateOrderRequest): Promise<Order> {
    this.created.push(request);
    return { orderId: `ord_${this.created.length}`, status: "SUBMITTED", symbol: request.symbol };
  }
  async getOrders() { return this.openOrders; }
  async getOrder(orderId: string): Promise<Order> { return { orderId, status: this.orderStatus }; }
  async cancelOrder(orderId: string): Promise<Order> {
    this.canceled.push(orderId);
    return { orderId, status: "CANCELED" };
  }
  async getFills() { return []; }
}

const ctx = (opts: { price?: string; heldQty?: string } = {}) =>
  new StrategyContext(
    new Date(),
    new Map(opts.price ? [["AAPL", {
      symbol: "AAPL", price: new Decimal(opts.price), bidPrice: null, askPrice: null,
      volume: 0, change: null, changeRate: null, timestamp: new Date(),
    }]] : []),
    new Map(),
    { accountId: "a", currency: "USD", cash: new Decimal(1), portfolioValue: new Decimal(1), status: "ACTIVE" },
    new Map(opts.heldQty ? [["AAPL", {
      symbol: "AAPL", quantity: new Decimal(opts.heldQty), avgEntryPrice: new Decimal(100),
    }]] : []),
    [],
    new Decimal(10000),
  );

test("매도는 보유 수량으로 클램프", async () => {
  const broker = new FakeBroker();
  const executor = new OrderExecutor(broker, new BracketMonitor(broker), new TradingGuard(broker));
  await executor.execute("t", [sell("AAPL", new Decimal(10))], ctx({ heldQty: "3" }));
  assert.equal(broker.created[0].quantity.toString(), "3");
  assert.ok(broker.created[0].clientOrderId!.startsWith("t-"));
});

test("보유 없으면 매도 스킵", async () => {
  const broker = new FakeBroker();
  const executor = new OrderExecutor(broker, new BracketMonitor(broker), new TradingGuard(broker));
  await executor.execute("t", [sell("AAPL", new Decimal(1))], ctx());
  assert.equal(broker.created.length, 0);
});

test("비상정지 중 시그널 차단", async () => {
  const broker = new FakeBroker();
  const guard = new TradingGuard(broker);
  await guard.halt("test");
  const executor = new OrderExecutor(broker, new BracketMonitor(broker), guard);
  await executor.execute("t", [buy("AAPL", new Decimal(1))], ctx());
  assert.equal(broker.created.length, 0);
});

test("브라켓 익절 플로우", async () => {
  const broker = new FakeBroker();
  const brackets = new BracketMonitor(broker);
  const executor = new OrderExecutor(broker, brackets, new TradingGuard(broker));

  await executor.execute("t", [buy("AAPL", new Decimal(2), {
    takeProfitPrice: new Decimal(310), stopLossPrice: new Decimal(280),
  })], ctx());
  assert.equal(brackets.activeCount, 1);

  broker.orderStatus = "FILLED"; // 진입 체결
  const signals = await brackets.check(ctx({ price: "311", heldQty: "2" }));
  assert.equal(signals.length, 1);
  assert.equal(signals[0].quantity.toString(), "2");
  assert.equal(brackets.activeCount, 0);
});

test("진입 취소 시 브라켓 폐기", async () => {
  const broker = new FakeBroker();
  const brackets = new BracketMonitor(broker);
  brackets.register("ord_1", "AAPL", new Decimal(2), new Decimal(310), null);
  broker.orderStatus = "CANCELED";
  assert.equal((await brackets.check(ctx({ price: "999" }))).length, 0);
  assert.equal(brackets.activeCount, 0);
});

test("연속 실패 시 비상정지 + 미체결 취소", async () => {
  const broker = new FakeBroker();
  broker.openOrders = [{ orderId: "ord_9", status: "SUBMITTED" }];
  const guard = new TradingGuard(broker, 3);
  for (let i = 0; i < 3; i++) await guard.recordFailure(new Error("boom"));
  assert.ok(guard.isHalted);
  assert.deepEqual(broker.canceled, ["ord_9"]);
});


// ------------------------------------------------------------- 0.6.0: 위험 상한 · 실전 게이트 · 심볼 접두

test("RiskGuard: 1건/일일 상한, 가격 미상 거부, UTC 일자 변경 시 초기화", () => {
  const guard = new RiskGuard(new Decimal(1000));
  assert.ok(guard.tryReserve("AAPL", new Decimal(2), new Decimal(600)));
  assert.equal(guard.tryReserve("AAPL", new Decimal(1), new Decimal(600)), null);
  assert.ok(guard.tryReserve("AAPL", new Decimal(1), null));
  assert.equal(new RiskGuard().tryReserve("AAPL", new Decimal(1), null), null);

  let now = new Date("2026-09-10T23:00:00Z");
  const daily = new RiskGuard(null, new Decimal(1000), () => now);
  assert.equal(daily.tryReserve("AAPL", new Decimal(3), new Decimal(300)), null);
  assert.ok(daily.tryReserve("AAPL", new Decimal(1), new Decimal(300)));
  now = new Date("2026-09-11T01:00:00Z");
  assert.equal(daily.tryReserve("AAPL", new Decimal(1), new Decimal(300)), null);
  assert.equal(daily.getDailyTotal().toString(), "300");
});

test("실행기는 금액 상한을 넘는 시그널을 제출하지 않는다", async () => {
  const broker = new FakeBroker();
  const executor = new OrderExecutor(broker, new BracketMonitor(broker), new TradingGuard(broker), new RiskGuard(new Decimal(1000)));
  await executor.execute("t", [
    buy("AAPL", new Decimal(5)),
    buy("AAPL", new Decimal(3)),
    buy("AAPL", new Decimal(10), { orderType: "LIMIT", limitPrice: new Decimal(50) }),
    buy("NOPE", new Decimal(1)),
  ], ctx({ price: "300" }));
  assert.deepEqual(broker.created.map((r) => r.quantity.toString()), ["3", "10"]);
});

test("실전 게이트: LIVE 는 liveTradingEnabled 없이는 스케줄되지 않는다", () => {
  const noop = { spec: { name: "t", symbols: ["AAPL"] }, decide: () => [] };
  const broker = new FakeBroker();
  broker.environment = "LIVE";
  assert.equal(new StrategyEngine(broker, [noop]).strategies.length, 0);
  assert.equal(new StrategyEngine(broker, [noop], 5, { liveTradingEnabled: true }).strategies.length, 1);
  broker.environment = "PAPER";
  assert.equal(new StrategyEngine(broker, [noop]).strategies.length, 1);
});

test("컨텍스트 조회는 시장 접두 유무를 무시한다", () => {
  const c = new StrategyContext(
    new Date(),
    new Map([["KRX:005930", { symbol: "KRX:005930", price: new Decimal(70000), bidPrice: null, askPrice: null, volume: 0, change: null, changeRate: null, timestamp: new Date() }]]),
    new Map(),
    { accountId: "a", currency: "KRW", cash: new Decimal(1), portfolioValue: new Decimal(1), status: "ACTIVE" },
    new Map([["005930", { symbol: "005930", quantity: new Decimal(3), avgEntryPrice: new Decimal(1) }]]),
    [{ orderId: "o1", status: "SUBMITTED", symbol: "005930" }],
    new Decimal(1),
  );
  assert.equal(c.quote("005930")!.price.toString(), "70000");
  assert.equal(c.holding("KRX:005930")!.quantity.toString(), "3");
  assert.equal(c.hasPosition("KRX:005930"), true);
  assert.equal(c.hasOpenOrder("KRX:005930"), true);
  assert.equal(c.quote("AAPL"), undefined);
});
