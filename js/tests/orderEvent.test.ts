/** 주문 통보가 KIS 메모리 주문 추적과 브라켓에 반영되는지 (Kotlin KisOrderEventTest / BracketMonitorTest 대응). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { KisClient } from "../src/brokers/kis.js";
import { BracketMonitor } from "../src/engine.js";
import { Decimal } from "../src/models.js";
import type { BrokerClient, OrderEvent, OrderEventType } from "../src/index.js";
import { StrategyContext } from "../src/strategy.js";

/** tr_id/path 로 라우팅하는 가짜 fetch — 큐 방식은 순서가 어긋나면 무한 대기하므로 */
function fakeKisFetch(balanceCalls: { n: number }) {
  return async (url: string, init: { method: string; headers?: Record<string, string> }) => {
    const path = new URL(url).pathname;
    const trId = init.headers?.tr_id;
    let body: unknown;
    if (path.startsWith("/oauth2/tokenP")) body = { access_token: "tok", expires_in: 86400 };
    else if (trId === "VTTC0802U") body = { rt_cd: "0", msg_cd: "APBK0013", msg1: "주문 전송 완료", output: { KRX_FWDG_ORD_ORGNO: "00950", ODNO: "0012345", ORD_TMD: "105530" } };
    else if (trId === "VTTC8434R") { balanceCalls.n++; body = { rt_cd: "0", msg_cd: "MCA00000", msg1: "ok", output1: [], output2: [{ dnca_tot_amt: "1000000", tot_evlu_amt: "1000000" }] }; }
    else return { status: 599, text: async () => JSON.stringify({ rt_cd: "1", msg1: `unexpected ${path} ${trId}` }), headers: { forEach: () => {} } };
    return { status: 200, text: async () => JSON.stringify(body), headers: { forEach: () => {} } };
  };
}

const event = (type: OrderEventType, quantity?: string, price?: string): OrderEvent => ({
  orderId: "0000012345", type, timestamp: new Date(), symbol: "005930", side: "BUY",
  quantity: quantity ? new Decimal(quantity) : null, price: price ? new Decimal(price) : null,
});

async function withKis(fn: (client: KisClient, balanceCalls: { n: number }) => Promise<void>) {
  const balanceCalls = { n: 0 };
  const restore = (globalThis as any).fetch;
  (globalThis as any).fetch = fakeKisFetch(balanceCalls);
  try {
    await fn(new KisClient("k", "s", "50199202", "01", "http://kis.test", 1), balanceCalls);
  } finally {
    (globalThis as any).fetch = restore;
  }
}

test("kis: 체결 통보가 누적되어 주문을 FILLED 로 만들고, 그 뒤 getOrder 는 보유 조회를 하지 않는다", () =>
  withKis(async (c, balanceCalls) => {
    const order = await c.createOrder({ symbol: "005930", side: "BUY", orderType: "LIMIT", quantity: new Decimal(2), limitPrice: new Decimal(250000), timeInForce: "DAY" });
    assert.equal(order.orderId, "0012345");
    const baseline = balanceCalls.n; // createOrder 가 기준 보유를 1회 조회

    c.applyOrderEvent(event("ACCEPTED", "2", "250000"));
    c.applyOrderEvent(event("FILLED", "1", "250000"));
    assert.equal((await c.getOrder("0012345")).status, "PARTIALLY_FILLED"); // 아직 open → refresh 1회

    c.applyOrderEvent(event("FILLED", "1", "250500"));
    const filled = await c.getOrder("0012345");
    assert.equal(filled.status, "FILLED");
    assert.ok(filled.filledQuantity!.eq(2));
    assert.ok(filled.avgFillPrice!.eq(250500));
    assert.equal(balanceCalls.n, baseline + 1);
    assert.equal((await c.getFills()).length, 1);
    assert.equal(balanceCalls.n, baseline + 1); // FILLED 뒤에는 조회 없음
  }));

test("kis: 취소 통보는 주문을 CANCELED 로, 모르는 주문번호는 무시", () =>
  withKis(async (c) => {
    await c.createOrder({ symbol: "005930", side: "BUY", orderType: "LIMIT", quantity: new Decimal(2), limitPrice: new Decimal(250000), timeInForce: "DAY" });
    c.applyOrderEvent(event("CANCELED"));
    assert.equal((await c.getOrder("0012345")).status, "CANCELED");
    c.applyOrderEvent({ orderId: "9999999", type: "FILLED", timestamp: new Date(), quantity: new Decimal(1) });
    assert.deepEqual(await c.getOrders(), []);
  }));

// ---- BracketMonitor.onOrderEvent

const brokerNeverQueried: BrokerClient = {
  capabilities: { brokerId: "t", market: "KRX", currency: "KRW", candleIntervals: new Set(["1d"]), clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true },
  async getQuotes() { return []; }, async getCandles() { return []; }, async getCalendar() { return []; },
  async getAccount() { return { accountId: "a", currency: "KRW", cash: new Decimal(0), portfolioValue: new Decimal(0), status: "ACTIVE" }; },
  async getHoldings() { return []; }, async getBuyingPower() { return new Decimal(0); },
  async createOrder(): Promise<never> { throw new Error("unused"); }, async getOrders() { return []; },
  async getOrder(): Promise<never> { throw new Error("getOrder 를 부르면 안 된다"); },
  async cancelOrder(): Promise<never> { throw new Error("unused"); }, async getFills() { return []; },
};

const ctxAt = (price: string) => new StrategyContext(new Date(),
  new Map([["AAPL", { symbol: "AAPL", price: new Decimal(price), bidPrice: null, askPrice: null, volume: 0, change: null, changeRate: null, timestamp: new Date() }]]),
  new Map(), { accountId: "a", currency: "USD", cash: new Decimal(0), portfolioValue: new Decimal(0), status: "ACTIVE" },
  new Map([["AAPL", { symbol: "AAPL", quantity: new Decimal(2), avgEntryPrice: new Decimal(280) }]]), [], new Decimal(0));

const bracketEvent = (type: OrderEventType, orderId = "0000ord_1", quantity?: string): OrderEvent =>
  ({ orderId, type, timestamp: new Date(), quantity: quantity ? new Decimal(quantity) : null });

test("bracket: 주문 통보로 체결이 누적되어 주문 수량을 채우면 서버 조회 없이 활성화된다", async () => {
  const monitor = new BracketMonitor(brokerNeverQueried);
  monitor.register("ord_1", "AAPL", new Decimal(2), new Decimal(310), null);
  monitor.onOrderEvent(bracketEvent("ACCEPTED"));
  monitor.onOrderEvent(bracketEvent("FILLED", "0000ord_1", "1")); // 부분 체결 — 아직 비활성 (getOrder 실패 → 유지)
  assert.equal((await monitor.check(ctxAt("311"))).length, 0);
  monitor.onOrderEvent(bracketEvent("FILLED", "0000ord_1", "1")); // 누적 2 = 주문 수량 → 활성
  const signals = await monitor.check(ctxAt("311"));
  assert.equal(signals.length, 1);
  assert.equal(signals[0].symbol, "AAPL");
});

test("bracket: 주문 통보로 취소·거부되면 브라켓을 폐기한다", () => {
  const monitor = new BracketMonitor(brokerNeverQueried);
  monitor.register("ord_1", "AAPL", new Decimal(2), new Decimal(310), null);
  monitor.onOrderEvent(bracketEvent("CANCELED"));
  assert.equal(monitor.activeCount, 0);
  monitor.register("ord_2", "AAPL", new Decimal(2), new Decimal(310), null);
  monitor.onOrderEvent(bracketEvent("REJECTED", "ord_2"));
  assert.equal(monitor.activeCount, 0);
  monitor.register("ord_3", "AAPL", new Decimal(2), new Decimal(310), null);
  monitor.onOrderEvent(bracketEvent("CANCELED", "other")); // 다른 주문 — 무시
  assert.equal(monitor.activeCount, 1);
});
