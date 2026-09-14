/** NH PLUG 실시간 스트림 — 로컬 ws 서버로 프로토콜을 재생한다 (Kotlin NhMarketStreamTest 대응, 문서 기반). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { NhMarketStream, parseNhOrderBook, parseNhOrderEvents, parseNhTrade } from "../src/brokers/nhStream.js";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";
import { Queue, TODAY, assertBooks, assertEvents, assertTrades, sleep, streamFixture, withServer } from "./wsHarness.js";

const fx = streamFixture("nh");
const TRADE_FRAME = fx.frames[0];
const BOOK_FRAME = fx.orderBook.frames[0];
const [D3_FRAME, D2_FRAME] = fx.orderEvents.frames;
const opts = (url: string, marketCd = "UNT") => ({ wsUrl: url, token: async () => "TOKEN", marketCd, accountNo: "" });

test("nh: 통합 설정은 mc 로 토큰을 실어 구독하고 ACK 를 처리하며, 체결 프레임을 요청 표기로 전달한다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new NhMarketStream(opts(url));
    try {
      stream.subscribeTrades(["KRX:005940"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.header.token, "TOKEN");
      assert.equal(subscribe.header.tr_type, "1");
      assert.equal(subscribe.body.tr_cd, "mc");
      assert.equal(subscribe.body.tr_key, "005940");
      conn.send('{"header":{"tr_type":"1","tr_cd":"mc","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"},"body":{"tr_key":["005940"]}}');
      conn.send(TRADE_FRAME);
      const tick = await ticks.take();
      assert.equal(tick.symbol, "KRX:005940");
      assert.ok(tick.price.eq(31750));
      assert.ok(tick.quantity.eq(13));
      assert.equal(tick.cumulativeVolume, 837624);
      assert.ok(tick.change!.eq(2500));
      assert.ok(tick.changeRate!.eq("0.0855"));
      assert.equal(stream.isConnected, true);
    } finally { stream.close(); }
  }));

test("nh: KRX 설정은 oc·ob 채널을 쓴다", () =>
  withServer(async (url, connections) => {
    const stream = new NhMarketStream(opts(url, "KRX"));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.subscribeOrderBook(["005930"], () => {});
      stream.connect();
      const conn = await connections.take();
      const codes = [JSON.parse(await conn.received.take()).body.tr_cd, JSON.parse(await conn.received.take()).body.tr_cd].sort();
      assert.deepEqual(codes, ["ob", "oc"]);
    } finally { stream.close(); }
  }));

test("nh: 호가 mb 프레임을 요청 표기 심볼의 10단계 호가창으로 전달한다", () =>
  withServer(async (url, connections) => {
    const books = new Queue<OrderBookTick>();
    const stream = new NhMarketStream(opts(url));
    try {
      stream.subscribeOrderBook(["KRX:005940"], (b) => books.put(b));
      stream.connect();
      const conn = await connections.take();
      assert.equal(JSON.parse(await conn.received.take()).body.tr_cd, "mb");
      conn.send(BOOK_FRAME);
      const book = await books.take();
      assert.equal(book.symbol, "KRX:005940");
      assert.equal(book.asks.length, 10);
      assert.equal(book.bids.length, 10);
      assert.ok(book.asks[0].price.eq(31800));
      assert.ok(book.asks[0].quantity.eq(668));
      assert.ok(book.bids[0].price.eq(31750));
      assert.ok(book.bids[0].quantity.eq(2899));
      assert.ok(book.totalAskQuantity!.eq(33938));
      assert.ok(book.totalBidQuantity!.eq(13132));
    } finally { stream.close(); }
  }));

test("nh: 주문 통보는 d2·d3 를 빈 tr_key 로 등록하고 접수→체결 이벤트로 전달한다", () =>
  withServer(async (url, connections) => {
    const events = new Queue<OrderEvent>();
    const stream = new NhMarketStream(opts(url));
    try {
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      const regs = [JSON.parse(await conn.received.take()), JSON.parse(await conn.received.take())];
      assert.deepEqual(regs.map((r) => r.body.tr_cd).sort(), ["d2", "d3"]);
      assert.ok(regs.every((r) => r.body.tr_key === "" && r.header.tr_type === "1"));
      conn.send(D3_FRAME);
      conn.send(D2_FRAME);
      const accepted = await events.take();
      assert.equal(accepted.type, "ACCEPTED");
      assert.equal(accepted.orderId, "0000000030");
      assert.equal(accepted.symbol, "005940");
      assert.equal(accepted.side, "BUY");
      assert.ok(accepted.quantity!.eq(10));
      assert.ok(accepted.price!.eq(35550));
      const filled = await events.take();
      assert.equal(filled.type, "FILLED");
      assert.ok(filled.quantity!.eq(5));
      assert.ok(filled.price!.eq(35550));
    } finally { stream.close(); }
  }));

test("nh: 서버가 끊으면 재접속해 구독과 통보 등록을 다시 보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new NhMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.subscribeOrderEvents(() => {});
      stream.connect();
      const first = await connections.take();
      for (let i = 0; i < 3; i++) await first.received.take();
      first.socket.close(1000, "bye");
      const second = await connections.take(10_000);
      const codes: string[] = [];
      for (let i = 0; i < 3; i++) codes.push(JSON.parse(await second.received.take()).body.tr_cd);
      assert.deepEqual(codes.sort(), ["d2", "d3", "mc"]);
    } finally { stream.close(); }
  }));

test("nh: 연결된 뒤 추가 구독은 즉시 전송된다", () =>
  withServer(async (url, connections) => {
    const stream = new NhMarketStream(opts(url));
    try {
      stream.connect();
      const conn = await connections.take();
      await sleep(50);
      stream.subscribeTrades(["000660"], () => {});
      assert.equal(JSON.parse(await conn.received.take()).body.tr_key, "000660");
    } finally { stream.close(); }
  }));

test("nh 파서: 숫자 타입·HHMMSS 시각·하락 부호·itemgb 필터·계좌 필터", () => {
  const numeric = { header: { tr_cd: "oc", tr_key: "005930" }, body: { code: "005930", time: "093012", sign: "5", change: 300, price: 71500, chrate: 0.42, offer: 71500, bid: 71400, movolume: 15, new_volume: 1234567 } };
  const tick = parseNhTrade(numeric, TODAY)[0];
  assert.ok(tick.price.eq(71500));
  assert.ok(tick.quantity.eq(15));
  assert.ok(tick.change!.eq(-300));
  assert.ok(tick.changeRate!.eq("-0.0042"));
  assert.equal(tick.cumulativeVolume, 1234567);
  assert.equal(parseNhTrade({ header: { tr_cd: "oc" }, body: { code: "005930", time: "093012" } }, TODAY).length, 0); // 가격 없음

  const d2 = JSON.parse(D2_FRAME);
  d2.body.itemgb = "2"; // 국내파생 → 무시
  assert.equal(parseNhOrderEvents(d2, TODAY).length, 0);
  d2.body.itemgb = "1";
  assert.equal(parseNhOrderEvents(d2, TODAY, "OTHER").length, 0); // 다른 계좌
  assert.equal(parseNhOrderEvents(d2, TODAY, "ACCOUNT NUMBER").length, 1);
  d2.body.ucgb = "2";
  assert.equal(parseNhOrderEvents(d2, TODAY)[0].type, "CANCELED");
  d2.body.ucgb = "1";
  assert.equal(parseNhOrderEvents(d2, TODAY)[0].type, "MODIFIED");
  d2.body.rejgb = "1";
  assert.equal(parseNhOrderEvents(d2, TODAY)[0].type, "REJECTED");

  const d3 = JSON.parse(D3_FRAME);
  d3.body.orgordno = "0000000029";
  assert.equal(parseNhOrderEvents(d3, TODAY)[0].originalOrderId, "0000000029");
  assert.equal(parseNhOrderBook({ header: { tr_cd: "mb" }, body: { code: "005930", hotime: "13:55:11", offer: "0", bid: "31750", bidrem: "1" } }, TODAY)[0].asks.length, 0); // 0 호가 제외
});

test("nh 픽스처: stream 섹션(문서 기반)을 기대값대로 파싱한다", () => {
  assert.equal(fx.measured, false);
  assertTrades(fx.frames.flatMap((f) => parseNhTrade(JSON.parse(f), TODAY)), fx.expected);
  assert.equal(fx.orderBook.measured, false);
  assertBooks(fx.orderBook.frames.flatMap((f) => parseNhOrderBook(JSON.parse(f), TODAY)), fx.orderBook.expected);
  assert.equal(fx.orderEvents.measured, false);
  assertEvents(fx.orderEvents.frames.flatMap((f) => parseNhOrderEvents(JSON.parse(f), TODAY)), fx.orderEvents.expected);
});
