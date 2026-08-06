/** 엔진 로직 검증 - Kotlin/Python 레퍼런스와 동일 시나리오. */
import assert from "node:assert/strict";
import { test } from "node:test";
import type { BrokerClient } from "../src/broker.js";
import { BracketMonitor, OrderExecutor, TradingGuard } from "../src/engine.js";
import { Decimal } from "../src/models.js";
import type { CreateOrderRequest, Order, OrderStatus } from "../src/models.js";
import { StrategyContext, buy, sell } from "../src/strategy.js";

class FakeBroker implements BrokerClient {
  capabilities = {
    brokerId: "fake", market: "US" as const, currency: "USD",
    candleIntervals: new Set(["1d" as const]),
    clientOrderId: true, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
  };
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
