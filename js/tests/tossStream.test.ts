/** 토스증권 실시간 스트림 — 로컬 ws 서버로 AsyncAPI 프로토콜을 재생한다 (Kotlin TossMarketStreamTest 대응, 문서 기반). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { Decimal } from "decimal.js";
import { TossMarketStream, parseTossOrderBook, parseTossOrderEvent, parseTossTrade, tossTopicKey } from "../src/brokers/tossStream.js";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";
import { Queue, assertBooks, assertEvents, assertTrades, sleep, streamFixture, withServer } from "./wsHarness.js";

const fx = streamFixture("toss");
const TRADE_US = fx.frames[0];
const ORDERBOOK_KR = fx.orderBook.frames[0];
const ORDER_FILL = fx.orderEvents.frames[0];
const TRADE_KR = '{"type":"message","topic":"trade:kr:005930","data":{"price":"71500","volume":"3","timestamp":"2026-06-18T23:30:00.000+09:00","currency":"KRW"}}';
const opts = (url: string, extra: Partial<{ heartbeatMs: number; declareDelayMs: number }> = {}) =>
  ({ wsUrl: url, token: async () => "TOKEN", accountSeq: async () => "3", declareDelayMs: 50, ...extra });

const orderFrame = (event: string, filled: string, price = "100.5", avg = "100") =>
  `{"type":"message","topic":"personal:order:3","data":{"event":"${event}","accountSeq":"3","order":{"orderId":"ORD-1","symbol":"AAPL","side":"BUY","orderType":"LIMIT","timeInForce":"DAY","status":"x","price":"${price}","quantity":"10","currency":"USD","orderedAt":"2026-06-23T09:30:00.000+09:00","canceledAt":null,"execution":{"filledQuantity":"${filled}","averageFilledPrice":"${avg}"}}}}`;

test("toss: 핸드셰이크에 Bearer 토큰을 싣고, 구독 전체를 배열 하나로 선언한다", () =>
  withServer(async (url, connections) => {
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeTrades(["KRX:005930"], () => {});
      stream.subscribeTrades(["US:AAPL"], () => {});
      stream.subscribeOrderBook(["KRX:005930"], () => {});
      stream.subscribeOrderEvents(() => {});
      stream.connect();
      const conn = await connections.take();
      assert.equal(conn.header("authorization"), "Bearer TOKEN");
      const declaration = JSON.parse(await conn.received.take());
      assert.ok(Array.isArray(declaration));
      assert.equal(declaration[0].id, "req-1");
      const byType = Object.fromEntries(declaration.slice(1).map((d: { type: string; codes: string[] }) => [d.type, d.codes]));
      assert.deepEqual(byType["trade:kr"], ["005930"]);
      assert.deepEqual(byType["trade:us"], ["AAPL"]);
      assert.deepEqual(byType["orderbook:kr"], ["005930"]);
      assert.deepEqual(byType["personal:order"], ["3"]);
      assert.equal(conn.received.size, 0); // 선언은 한 번
    } finally { stream.close(); }
  }));

test("toss: 연결 뒤 추가 구독은 짧게 모아 전체 집합을 다시 선언한다", () =>
  withServer(async (url, connections) => {
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.connect();
      const conn = await connections.take();
      await conn.received.take();
      stream.subscribeTrades(["000660"], () => {});
      stream.subscribeOrderBook(["000660"], () => {});
      const declaration = JSON.parse(await conn.received.take());
      const byType = Object.fromEntries(declaration.slice(1).map((d: { type: string; codes: string[] }) => [d.type, d.codes]));
      assert.deepEqual(byType["trade:kr"], ["000660", "005930"]);
      assert.deepEqual(byType["orderbook:kr"], ["000660"]);
      await sleep(120);
      assert.equal(conn.received.size, 0); // 두 변경이 하나로 합쳐졌다
    } finally { stream.close(); }
  }));

test("toss: 거부된 target 은 다음 선언에서 빠진다", () =>
  withServer(async (url, connections) => {
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930", "999999"], () => {});
      stream.connect();
      const conn = await connections.take();
      await conn.received.take();
      conn.send('{"type":"subscriptions","id":"req-1","subscribed":["trade:kr:005930"],"rejected":[{"target":"trade:kr:999999","code":"stock-not-found","message":"해당 종목을 찾을 수 없습니다."}]}');
      await sleep(20);
      stream.subscribeTrades(["000660"], () => {});
      const declaration = JSON.parse(await conn.received.take());
      assert.deepEqual(declaration[1], { type: "trade:kr", codes: ["000660", "005930"] });
    } finally { stream.close(); }
  }));

test("toss: 체결·호가 프레임을 요청 표기 심볼로 전달한다 (KRX·US, 접두 없이 구독하면 접두 없이)", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const books = new Queue<OrderBookTick>();
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeTrades(["US:AAPL", "005930"], (t) => ticks.put(t));
      stream.subscribeOrderBook(["KRX:005930"], (b) => books.put(b));
      stream.connect();
      const conn = await connections.take();
      await conn.received.take();
      conn.send(TRADE_US);
      const us = await ticks.take();
      assert.equal(us.symbol, "US:AAPL");
      assert.ok(us.price.eq("243.26"));
      assert.ok(us.quantity.eq(8));
      assert.equal(us.cumulativeVolume, null);
      conn.send(TRADE_KR);
      const kr = await ticks.take();
      assert.equal(kr.symbol, "005930"); // 접두 없이 구독 → 접두 없이
      assert.ok(kr.price.eq(71500));
      conn.send(ORDERBOOK_KR);
      const book = await books.take();
      assert.equal(book.symbol, "005930"); // 같은 topic 은 먼저 구독한 표기("005930")로 돌려준다 (Kotlin 과 동일)
      assert.ok(book.asks[0].price.eq(71500));
      assert.ok(book.asks[0].quantity.eq(5));
      assert.ok(book.bids[0].price.eq(71400));
      assert.ok(book.bids[0].quantity.eq(10));
    } finally { stream.close(); }
  }));

test("toss: 주문 이벤트 - FILL 은 체결량·평균가·잔량, 부분 체결은 누적 차이로 계산한다", () =>
  withServer(async (url, connections) => {
    const events = new Queue<OrderEvent>();
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      await conn.received.take();
      conn.send(ORDER_FILL);
      const fill = await events.take();
      assert.equal(fill.type, "FILLED");
      assert.equal(fill.symbol, "US:AAPL");
      assert.equal(fill.side, "BUY");
      assert.ok(fill.quantity!.eq(10));
      assert.ok(fill.price!.eq(100));
      assert.ok(fill.remainingQuantity!.eq(0));

      conn.send(orderFrame("PENDING", "0"));
      assert.equal((await events.take()).type, "ACCEPTED");
      conn.send(orderFrame("PARTIAL_FILL", "4"));
      const partial = await events.take();
      assert.ok(partial.quantity!.eq(4));
      assert.ok(partial.remainingQuantity!.eq(6));
      conn.send(orderFrame("FILL", "10", "100.5", "100.5"));
      const rest = await events.take();
      assert.ok(rest.quantity!.eq(6));
      assert.ok(rest.price!.eq("100.5"));
      assert.ok(rest.remainingQuantity!.eq(0));
      conn.send(orderFrame("CANCELING", "10"));
      conn.send(orderFrame("CANCEL_REJECTED", "10"));
      const rejected = await events.take();
      assert.equal(rejected.type, "REJECTED"); // CANCELING 은 삼켜지고 CANCEL_REJECTED 만 전달
      assert.equal(rejected.reason, "CANCEL_REJECTED");
    } finally { stream.close(); }
  }));

test("toss: 하트비트는 텍스트 PING 을 보내고 pong·error 프레임은 삼킨다", () =>
  withServer(async (url, connections) => {
    const stream = new TossMarketStream(opts(url, { heartbeatMs: 100 }));
    try {
      stream.connect();
      const conn = await connections.take();
      assert.equal(await conn.received.take(), "PING");
      conn.send('{"type":"pong"}');
      conn.send('{"type":"error","error":{"code":"rate-limit-exceeded","message":"too fast"}}');
      assert.equal(await conn.received.take(), "PING");
      assert.equal(stream.isConnected, true);
    } finally { stream.close(); }
  }));

test("toss: 서버가 끊으면 재접속해 전체 집합을 다시 선언한다", () =>
  withServer(async (url, connections) => {
    const stream = new TossMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.connect();
      const first = await connections.take();
      await first.received.take();
      first.socket.close(1000, "server-shutdown");
      const second = await connections.take(10_000);
      assert.equal(second.header("authorization"), "Bearer TOKEN");
      const declaration = JSON.parse(await second.received.take());
      assert.deepEqual(declaration[1], { type: "trade:kr", codes: ["005930"] });
    } finally { stream.close(); }
  }));

test("toss topicKey: 시장 접두 규칙", () => {
  assert.equal(tossTopicKey("KRX:005930"), "kr:005930");
  assert.equal(tossTopicKey("005930"), "kr:005930");
  assert.equal(tossTopicKey("US:aapl"), "us:AAPL");
  assert.throws(() => tossTopicKey("JP:7203"), /지원하지 않는 시장/);
  assert.equal(parseTossTrade("orderbook:kr:005930", {}), null);
  assert.equal(parseTossOrderBook("trade:kr:005930", {}), null);
  assert.equal(parseTossOrderEvent({ event: "REPLACING", order: { orderId: "x" } }, null), null);
  assert.equal(parseTossOrderEvent({ event: "FILL", order: {} }, null), null);
  const modified = parseTossOrderEvent({ event: "REPLACED", order: { orderId: "o", quantity: "5", price: "1", currency: "KRW", symbol: "005930", side: "SELL", execution: { filledQuantity: "0" } } }, null)!;
  assert.equal(modified.type, "MODIFIED");
  assert.equal(modified.symbol, "KRX:005930");
  assert.equal(modified.side, "SELL");
  assert.ok(modified.remainingQuantity!.eq(5));
  // 첫 스냅샷의 누적 체결량이 이전보다 작아도 음수가 되지 않는다
  assert.ok(parseTossOrderEvent({ event: "FILL", order: { orderId: "o", quantity: "5", execution: { filledQuantity: "2" } } }, new Decimal(3))!.quantity!.eq(2));
});

test("toss 픽스처: stream 섹션(AsyncAPI 예시)을 같은 모델로 파싱한다", () => {
  assert.equal(fx.measured, false);
  assertTrades(fx.frames.map((f) => { const n = JSON.parse(f); return parseTossTrade(n.topic, n.data)!; }), fx.expected);
  assert.equal(fx.orderBook.measured, false);
  assertBooks(fx.orderBook.frames.map((f) => { const n = JSON.parse(f); return parseTossOrderBook(n.topic, n.data)!; }), fx.orderBook.expected);
  assert.equal(fx.orderEvents.measured, false);
  assertEvents(fx.orderEvents.frames.map((f) => parseTossOrderEvent(JSON.parse(f).data, null)!), fx.orderEvents.expected);
});
