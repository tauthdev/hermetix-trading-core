/** LS증권 실시간 스트림 — 로컬 ws 서버로 프로토콜을 재생한다 (Kotlin LsMarketStreamTest 대응, 문서 기반). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { LsMarketStream, parseLsOrderBook, parseLsOrderEvents, parseLsTrade } from "../src/brokers/lsStream.js";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";
import { Queue, TODAY, assertBooks, assertEvents, assertTrades, sleep, streamFixture, withServer } from "./wsHarness.js";

const fx = streamFixture("ls");
const TRADE_FRAME = fx.frames[0];
const BOOK_FRAME = fx.orderBook.frames[0];
const [SC0_FRAME, SC1_FRAME, SC3_FRAME] = fx.orderEvents.frames;
const opts = (url: string) => ({ wsUrl: url, token: async () => "TOKEN" });

async function takeN(conn: { received: Queue<string> }, n: number): Promise<{ header: Record<string, string>; body: Record<string, string> }[]> {
  const out: { header: Record<string, string>; body: Record<string, string> }[] = [];
  for (let i = 0; i < n; i++) out.push(JSON.parse(await conn.received.take()));
  return out;
}

test("ls: 종목 하나를 구독하면 KOSPI·KOSDAQ 체결 TR 을 tr_type 3 으로 둘 다 등록하고, 체결 프레임을 요청 표기로 전달한다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new LsMarketStream(opts(url));
    try {
      stream.subscribeTrades(["KRX:005930"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      const regs = await takeN(conn, 2);
      assert.deepEqual(regs.map((r) => r.body.tr_cd).sort(), ["K3_", "S3_"]);
      assert.ok(regs.every((r) => r.header.token === "TOKEN" && r.header.tr_type === "3" && r.body.tr_key === "005930"));
      conn.send('{"header":{"tr_cd":"S3_","tr_key":"005930","tr_type":"3","rsp_cd":"00000","rsp_msg":"정상처리되었습니다"}}'); // ACK
      conn.send(TRADE_FRAME);
      const tick = await ticks.take();
      assert.equal(tick.symbol, "KRX:005930");
      assert.ok(tick.price.eq(55550));
      assert.ok(tick.quantity.eq(1));
      assert.equal(tick.cumulativeVolume, 10887);
      assert.ok(tick.change!.eq(1050));
      assert.ok(tick.changeRate!.eq("0.0193"));
      assert.ok(tick.askPrice!.eq(55600));
      assert.ok(tick.bidPrice!.eq(55500));
      // KOSDAQ TR 로 같은 레이아웃이 와도 전달된다
      conn.send(TRADE_FRAME.replace('"tr_cd":"S3_"', '"tr_cd":"K3_"'));
      assert.ok((await ticks.take()).price.eq(55550));
    } finally { stream.close(); }
  }));

test("ls: 주문 통보를 구독하면 SC0~SC4 를 tr_type 1·빈 tr_key 로 등록하고, 접수·체결·취소 프레임을 이벤트로 전달한다", () =>
  withServer(async (url, connections) => {
    const events = new Queue<OrderEvent>();
    const stream = new LsMarketStream(opts(url));
    try {
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      const regs = await takeN(conn, 5);
      assert.deepEqual(regs.map((r) => r.body.tr_cd).sort(), ["SC0", "SC1", "SC2", "SC3", "SC4"]);
      assert.ok(regs.every((r) => r.header.tr_type === "1" && r.body.tr_key === ""));

      conn.send(SC0_FRAME);
      const accepted = await events.take();
      assert.equal(accepted.type, "ACCEPTED");
      assert.equal(accepted.orderId, "86382");
      assert.equal(accepted.side, "BUY");
      assert.ok(accepted.quantity!.eq(2));
      assert.ok(accepted.price!.eq(60000));
      assert.equal(accepted.symbol, "005930");

      conn.send(SC1_FRAME);
      const filled = await events.take();
      assert.equal(filled.type, "FILLED");
      assert.ok(filled.quantity!.eq(1));
      assert.ok(filled.price!.eq(60000));
      assert.ok(filled.remainingQuantity!.eq(1));

      conn.send(SC3_FRAME);
      const canceled = await events.take();
      assert.equal(canceled.type, "CANCELED");
      assert.equal(canceled.orderId, "88343");
      assert.equal(canceled.originalOrderId, "88342");
      assert.equal(canceled.symbol, "000020");
    } finally { stream.close(); }
  }));

test("ls: 호가 구독은 H1_·HA_ 를 등록하고 호가 프레임을 10단계 호가창으로 전달한다", () =>
  withServer(async (url, connections) => {
    const books = new Queue<OrderBookTick>();
    const stream = new LsMarketStream(opts(url));
    try {
      stream.subscribeOrderBook(["KRX:005930"], (b) => books.put(b));
      stream.connect();
      const conn = await connections.take();
      assert.deepEqual((await takeN(conn, 2)).map((r) => r.body.tr_cd).sort(), ["H1_", "HA_"]);
      conn.send(BOOK_FRAME);
      const book = await books.take();
      assert.equal(book.symbol, "KRX:005930");
      assert.equal(book.asks.length, 10);
      assert.ok(book.asks[0].price.eq(72400));
      assert.ok(book.asks[0].quantity.eq(32616));
      assert.ok(book.bids[0].price.eq(72300));
      assert.ok(book.bids[9].price.eq(71400));
      assert.ok(book.totalAskQuantity!.eq(400000));
      assert.ok(book.totalBidQuantity!.eq(500000));
    } finally { stream.close(); }
  }));

test("ls: 서버가 끊으면 재접속해 모든 등록을 다시 보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new LsMarketStream(opts(url));
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.subscribeOrderEvents(() => {});
      stream.connect();
      const first = await connections.take();
      await takeN(first, 7);
      first.socket.close(1000, "bye");
      const second = await connections.take(10_000);
      const trs = (await takeN(second, 7)).map((r) => r.body.tr_cd).sort();
      assert.deepEqual(trs, ["K3_", "S3_", "SC0", "SC1", "SC2", "SC3", "SC4"]);
    } finally { stream.close(); }
  }));

test("ls: 연결된 뒤 추가 구독은 즉시, 해제는 tr_type 4·2 로 전송된다", () =>
  withServer(async (url, connections) => {
    const stream = new LsMarketStream(opts(url));
    try {
      stream.subscribeOrderEvents(() => {});
      stream.connect();
      const conn = await connections.take();
      await takeN(conn, 5);
      await sleep(50);
      stream.subscribeTrades(["000660"], () => {});
      const subs = await takeN(conn, 2);
      assert.ok(subs.every((r) => r.body.tr_key === "000660" && r.header.tr_type === "3"));
      stream.unsubscribeTrades(["000660"]);
      const unsubs = await takeN(conn, 2);
      assert.ok(unsubs.every((r) => r.header.tr_type === "4"));
      stream.unsubscribeOrderEvents();
      const unregs = await takeN(conn, 5);
      assert.ok(unregs.every((r) => r.header.tr_type === "2"));
    } finally { stream.close(); }
  }));

test("ls 파서: ACK·오류 프레임은 데이터로 처리하지 않고, 정정·거부·ordxctptncode 우선을 판정한다", () => {
  const ack = { header: { tr_cd: "S3_", tr_key: "005930", tr_type: "3", rsp_cd: "00000", rsp_msg: "정상처리되었습니다" } };
  assert.equal(parseLsTrade(ack, TODAY), null);
  assert.equal(parseLsOrderBook({ ...ack, header: { ...ack.header, tr_cd: "H1_" } }, TODAY), null);
  assert.equal(parseLsOrderEvents({ header: { tr_cd: "SC1", rsp_msg: "x" } }, TODAY).length, 0);

  const sc1 = JSON.parse(SC1_FRAME);
  const modified = parseLsOrderEvents({ header: { tr_cd: "SC2" }, body: { ...sc1.body, ordxctptncode: "12", ordno: "86383", orgordno: "86382", mdfycnfqty: "1", mdfycnfprc: "70000" } }, TODAY)[0];
  assert.equal(modified.type, "MODIFIED");
  assert.ok(modified.price!.eq(70000));
  assert.equal(modified.originalOrderId, "86382");
  const rejected = parseLsOrderEvents({ header: { tr_cd: "SC4" }, body: { ...sc1.body, ordxctptncode: "", rjtqty: "2", msgcode: "1234" } }, TODAY)[0];
  assert.equal(rejected.type, "REJECTED");
  assert.equal(rejected.reason, "1234");
  assert.ok(rejected.quantity!.eq(2));
  // ordxctptncode 가 tr_cd 보다 우선한다
  assert.equal(parseLsOrderEvents({ header: { tr_cd: "SC1" }, body: { ...sc1.body, ordxctptncode: "13", canccnfqty: "1" } }, TODAY)[0].type, "CANCELED");
  // 하락 부호: change 는 부호 없음 → sign 5 면 음수
  const falling = JSON.parse(TRADE_FRAME);
  falling.body.sign = "5";
  assert.ok(parseLsTrade(falling, TODAY)!.change!.eq(-1050));
});

test("ls 픽스처: stream 섹션(문서 기반)을 기대값대로 파싱한다", () => {
  assert.equal(fx.measured, false);
  assertTrades(fx.frames.map((f) => parseLsTrade(JSON.parse(f), TODAY)!), fx.expected);
  assert.equal(fx.orderBook.measured, false);
  assertBooks(fx.orderBook.frames.map((f) => parseLsOrderBook(JSON.parse(f), TODAY)!), fx.orderBook.expected);
  assert.equal(fx.orderEvents.measured, false);
  assertEvents(fx.orderEvents.frames.flatMap((f) => parseLsOrderEvents(JSON.parse(f), TODAY)), fx.orderEvents.expected);
});
