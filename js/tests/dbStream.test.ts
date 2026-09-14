/** DB증권 실시간 스트림 — 로컬 ws 서버로 프로토콜을 재생한다 (Kotlin DbMarketStreamTest 대응, 문서 기반). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { DbMarketStream, parseDbOrderBook, parseDbOrderEvent, parseDbTrade } from "../src/brokers/dbStream.js";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";
import { Queue, TODAY, assertBooks, assertEvents, assertTrades, streamFixture, withServer } from "./wsHarness.js";

const fx = streamFixture("db");
const TRADE_FRAME = fx.frames[0];
const BOOK_FRAME = fx.orderBook.frames[0];
const [IS0_FRAME, IS1_FILL_FRAME, IS1_CANCEL_FRAME] = fx.orderEvents.frames;
const opts = (url: string) => ({ wsUrl: url, token: async () => "TOKEN" });

test("db: 체결 구독 - 토큰 헤더·tr_type 1·tr_key 'J '+코드, S00 프레임을 요청 표기 심볼의 틱으로 전달한다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new DbMarketStream(opts(url));
    try {
      stream.subscribeTrades(["KRX:005930"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.header.token, "TOKEN");
      assert.equal(subscribe.header.tr_type, "1");
      assert.equal(subscribe.body.tr_cd, "S00");
      assert.equal(subscribe.body.tr_key, "J 005930");
      conn.send('{"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}'); // ack
      conn.send(TRADE_FRAME);
      const tick = await ticks.take();
      assert.equal(tick.symbol, "KRX:005930"); // U-005930 → 005930 → 요청 표기
      assert.ok(tick.price.eq(143300));
      assert.ok(tick.quantity.eq(100));
      assert.equal(tick.cumulativeVolume, 405039);
      assert.ok(tick.change!.eq(33000));
      assert.ok(tick.changeRate!.eq("0.2992"));
      assert.ok(tick.askPrice!.eq(140200));
      assert.ok(tick.bidPrice!.eq(143300));
    } finally { stream.close(); }
  }));

test("db: 호가·주문 통보 - S01 은 tr_type 1, IS0·IS1 은 tr_type 3 으로 tr_key 없이 등록한다", () =>
  withServer(async (url, connections) => {
    const books = new Queue<OrderBookTick>();
    const events = new Queue<OrderEvent>();
    const stream = new DbMarketStream(opts(url));
    try {
      stream.subscribeOrderBook(["KRX:005930"], (b) => books.put(b));
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      const msgs: { header: Record<string, string>; body: Record<string, string> }[] = [];
      for (let i = 0; i < 3; i++) msgs.push(JSON.parse(await conn.received.take()));
      const book = msgs.find((m) => m.body.tr_cd === "S01")!;
      assert.equal(book.header.tr_type, "1");
      assert.equal(book.body.tr_key, "J 005930");
      for (const tr of ["IS0", "IS1"]) {
        const reg = msgs.find((m) => m.body.tr_cd === tr)!;
        assert.equal(reg.header.tr_type, "3");
        assert.equal("tr_key" in reg.body, false);
      }

      conn.send(BOOK_FRAME);
      const b = await books.take();
      assert.equal(b.symbol, "KRX:005930");
      assert.equal(b.asks.length, 10);
      assert.ok(b.asks[0].price.eq(54500));
      assert.ok(b.bids[0].price.eq(54400));
      assert.ok(b.totalAskQuantity!.eq(809630));

      conn.send(IS0_FRAME);
      const accepted = await events.take();
      assert.equal(accepted.type, "ACCEPTED");
      assert.equal(accepted.orderId, "0000034048");
      assert.equal(accepted.side, "BUY");
      assert.equal(accepted.symbol, "005930");
      assert.ok(accepted.quantity!.eq(10));
      assert.ok(accepted.price!.eq(80000));

      conn.send(IS1_FILL_FRAME);
      const filled = await events.take();
      assert.equal(filled.type, "FILLED");
      assert.ok(filled.quantity!.eq(4));
      assert.ok(filled.remainingQuantity!.eq(6));

      conn.send(IS1_CANCEL_FRAME);
      const canceled = await events.take();
      assert.equal(canceled.type, "CANCELED");
      assert.equal(canceled.orderId, "0000241048");
      assert.equal(canceled.originalOrderId, "0000241038");
      assert.equal(canceled.symbol, "004410");
    } finally { stream.close(); }
  }));

test("db: 서버가 끊으면 재접속해 구독과 계좌 등록을 다시 보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new DbMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.subscribeOrderEvents(() => {});
      stream.connect();
      const first = await connections.take();
      for (let i = 0; i < 3; i++) await first.received.take();
      first.socket.close(1000, "bye");
      const second = await connections.take(10_000);
      const trs: string[] = [];
      for (let i = 0; i < 3; i++) trs.push(JSON.parse(await second.received.take()).body.tr_cd);
      assert.deepEqual(trs.sort(), ["IS0", "IS1", "S00"]);
    } finally { stream.close(); }
  }));

test("db: 제어 프레임 - rsp_cd 가 header 또는 body 에 있거나 header/body 가 null 이면 데이터로 다루지 않는다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new DbMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      await conn.received.take();
      conn.send('{"header":null,"body":null}');
      conn.send('{"header":{"tr_cd":"S00","rsp_cd":"10017","rsp_msg":"종목코드 없음"},"body":null}');
      conn.send('{"header":{"tr_cd":"S00"},"body":{"rsp_cd":"00000","rsp_msg":"ok"}}');
      conn.send(TRADE_FRAME);
      const tick = await ticks.take();
      assert.ok(tick.price.eq(143300));
      assert.equal(ticks.size, 0);
    } finally { stream.close(); }
  }));

test("db 파서: 필드명 대소문자 무시, 하락 부호, 0 호가 제외, 거부·정정 판정, 주문번호 없으면 null", () => {
  const lowerCase = { shrniscd: "005930", stckcntghour: "142230", stckprpr: "1000", cntgvol: "3", acmlvol: "10", prdyvrss: "50", prdyvrssclr: "-", prdyctrt: "5.00", askp1: "1010", bidp1: "990" };
  const tick = parseDbTrade(lowerCase, TODAY)!;
  assert.ok(tick.price.eq(1000));
  assert.ok(tick.change!.eq(-50));
  assert.ok(tick.changeRate!.eq("0.05"));
  assert.ok(tick.askPrice!.eq(1010));

  const sparse = { ShrnIscd: "005930", BsopHour: "100647", Askp1: "54500", AskpRsqn1: "10", Askp2: "0", Bidp1: "54400", BidpRsqn1: "20", TotalAskprsqn: "10", TotalBidprsqn: "20" };
  const book = parseDbOrderBook(sparse, TODAY)!;
  assert.equal(book.asks.length, 1);
  assert.equal(book.bids.length, 1);

  const is1 = JSON.parse(IS1_FILL_FRAME).body;
  const rejected = parseDbOrderEvent("IS1", { ...is1, Sexecqty: "0", Srjtqty: "0000000000000010" }, TODAY)!;
  assert.equal(rejected.type, "REJECTED");
  assert.ok(rejected.quantity!.eq(10));
  const modified = parseDbOrderEvent("IS1", { ...is1, Sexecqty: "0", Smdfycnfqty: "3", Smdfycnfprc: "81000" }, TODAY)!;
  assert.equal(modified.type, "MODIFIED");
  assert.ok(modified.price!.eq(81000));
  assert.equal(parseDbOrderEvent("IS1", { ...is1, Sordno: "" }, TODAY), null);
});

test("db 픽스처: stream 섹션(문서 기반)을 기대값대로 파싱한다", () => {
  assert.equal(fx.measured, false);
  assertTrades(fx.frames.map((f) => parseDbTrade(JSON.parse(f).body, TODAY)!), fx.expected);
  assert.equal(fx.orderBook.measured, false);
  assertBooks(fx.orderBook.frames.map((f) => parseDbOrderBook(JSON.parse(f).body, TODAY)!), fx.orderBook.expected);
  assert.equal(fx.orderEvents.measured, false);
  assertEvents(fx.orderEvents.frames.map((f) => { const n = JSON.parse(f); return parseDbOrderEvent(n.header.tr_cd, n.body, TODAY)!; }), fx.orderEvents.expected);
});
