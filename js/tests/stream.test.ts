/** 웹소켓 흐름 — 로컬 ws 서버로 KIS/키움 프로토콜(구독·에코·재접속)을 재생한다 (Kotlin KisMarketStreamTest/KiwoomMarketStreamTest 대응). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";
import { KisMarketStream, kisDecrypt, kisEncrypt } from "../src/brokers/kisStream.js";
import { KiwoomMarketStream } from "../src/brokers/kiwoomStream.js";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";

const KEY = "zkkljxnqkyodprlmaksyyilhzjmxqjer"; // 32자 (실측 구독 응답과 같은 형식)
const IV = "d82e2f422913e3b2"; // 16자
/** H0STASP0 본문 — 0 코드, 1 시각, 2 시간구분, 3-12 매도호가, 13-22 매수호가, 23-32 매도잔량, 33-42 매수잔량, 43 총매도잔량, 44 총매수잔량 */
const BOOK_FIELDS = [
  "005930", "105530", "0",
  ...Array.from({ length: 10 }, (_, i) => String(250500 + 500 * i)),
  ...Array.from({ length: 10 }, (_, i) => String(250000 - 500 * i)),
  ...Array.from({ length: 10 }, (_, i) => String(1000 * (i + 1))),
  ...Array.from({ length: 10 }, (_, i) => String(2000 * (i + 1))),
  "55000", "65000", "0", "0",
].join("^");
const KIWOOM_BOOK_FRAME = (() => {
  const v: Record<string, string> = { "21": "105530", "121": "55000", "125": "65000" };
  for (let i = 0; i < 10; i++) {
    v[String(41 + i)] = `-${250500 + 500 * i}`; v[String(61 + i)] = String(1000 * (i + 1));
    v[String(51 + i)] = `-${250000 - 500 * i}`; v[String(71 + i)] = String(2000 * (i + 1));
  }
  return JSON.stringify({ data: [{ values: v, type: "0D", name: "주식호가잔량", item: "A005930" }], trnm: "REAL" });
})();
const KIWOOM_ORDER_FRAME = '{"data":[{"values":{"9203":"0000012345","904":"0000000000","9001":"A005930","913":"체결","905":"+매수","907":"2","900":"1","901":"+250000","902":"0","910":"+250000","911":"1","908":"105531","919":""},"type":"00","name":"주문체결","item":""}],"trnm":"REAL"}';

const FRAME_FIELDS = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000";
const REAL_FRAME = '{"trnm":"REAL","data":[{"type":"0B","name":"주식체결","item":"A005930","values":{"20":"093012","10":"-71500","11":"-300","12":"-0.42","27":"+71500","28":"+71400","15":"-15","13":"1234567"}}]}';

/** 도착 순서대로 꺼낼 수 있는 큐 — 타임아웃 시 실패 */
class Queue<T> {
  private items: T[] = [];
  private waiters: ((v: T) => void)[] = [];
  put(v: T) { const w = this.waiters.shift(); if (w) w(v); else this.items.push(v); }
  take(timeoutMs = 5000): Promise<T> {
    const item = this.items.shift();
    if (item !== undefined) return Promise.resolve(item);
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`${timeoutMs}ms 안에 도착하지 않음`)), timeoutMs);
      this.waiters.push((v) => { clearTimeout(t); resolve(v); });
    });
  }
}

class ServerConnection {
  readonly received = new Queue<string>();
  constructor(readonly socket: ServerSocket) {
    socket.on("message", (data) => this.received.put(data.toString()));
  }
  send(text: string) { this.socket.send(text); }
}

async function withServer(fn: (url: string, connections: Queue<ServerConnection>) => Promise<void>) {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  const connections = new Queue<ServerConnection>();
  const all: ServerConnection[] = [];
  server.on("connection", (socket) => { const c = new ServerConnection(socket); all.push(c); connections.put(c); });
  await new Promise<void>((r) => server.once("listening", r));
  const { port } = server.address() as { port: number };
  try {
    await fn(`ws://127.0.0.1:${port}/`, connections);
  } finally {
    for (const c of all) c.socket.terminate();
    await new Promise<void>((r) => server.close(() => r()));
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test("kis: 접속하면 승인키를 실은 구독 메시지를 보내고, 데이터 프레임을 요청 표기 심볼의 틱으로 전달한다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "APPROVAL-KEY" });
    try {
      stream.subscribeTrades(["KRX:005930"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.header.approval_key, "APPROVAL-KEY");
      assert.equal(subscribe.header.tr_type, "1");
      assert.equal(subscribe.body.input.tr_id, "H0STCNT0");
      assert.equal(subscribe.body.input.tr_key, "005930");

      conn.send('{"header":{"tr_id":"H0STCNT0","tr_key":"005930","encrypt":"N"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS"}}');
      conn.send(`0|H0STCNT0|001|${FRAME_FIELDS}`);
      const tick = await ticks.take();
      assert.equal(tick.symbol, "KRX:005930");
      assert.ok(tick.price.eq(71500));
      assert.ok(tick.quantity.eq(15));
      assert.equal(tick.cumulativeVolume, 1234567);
      assert.equal(stream.isConnected, true);
    } finally { stream.close(); }
  }));

test("kis: PINGPONG 은 받은 그대로 돌려보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K" });
    try {
      stream.connect();
      const conn = await connections.take();
      const ping = '{"header":{"tr_id":"PINGPONG","datetime":"20260914093000"}}';
      conn.send(ping);
      assert.equal(await conn.received.take(), ping);
    } finally { stream.close(); }
  }));

test("kis: 서버가 끊으면 재접속하고 구독을 다시 보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K" });
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.connect();
      const first = await connections.take();
      await first.received.take(); // 첫 구독
      first.socket.close(1000, "server going away");
      const second = await connections.take(10_000);
      const resubscribe = JSON.parse(await second.received.take());
      assert.equal(resubscribe.body.input.tr_key, "005930");
    } finally { stream.close(); }
  }));

test("kis: 연결된 뒤 추가 구독은 즉시 전송된다", () =>
  withServer(async (url, connections) => {
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K" });
    try {
      stream.connect();
      const conn = await connections.take();
      await sleep(50);
      stream.subscribeTrades(["000660"], () => {});
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.body.input.tr_key, "000660");
    } finally { stream.close(); }
  }));

test("kiwoom: 접속 → LOGIN → REG 순서로 진행하고 REAL 프레임을 틱으로 전달한다", () =>
  withServer(async (url, connections) => {
    const ticks = new Queue<TradeTick>();
    const stream = new KiwoomMarketStream({ wsUrl: url, token: async () => "ACCESS-TOKEN" });
    try {
      stream.subscribeTrades(["KRX:005930"], (t) => ticks.put(t));
      stream.connect();
      const conn = await connections.take();
      const login = JSON.parse(await conn.received.take());
      assert.equal(login.trnm, "LOGIN");
      assert.equal(login.token, "ACCESS-TOKEN");
      assert.equal(stream.isConnected, false); // 로그인 응답 전

      conn.send('{"trnm":"LOGIN","return_code":0,"return_msg":"","sor_yn":"Y"}');
      const reg = JSON.parse(await conn.received.take());
      assert.equal(reg.trnm, "REG");
      assert.deepEqual(reg.data[0].item, ["005930"]);
      assert.deepEqual(reg.data[0].type, ["0B"]);

      conn.send('{"trnm":"REG","return_code":0,"return_msg":""}');
      conn.send(REAL_FRAME);
      const tick = await ticks.take();
      assert.equal(tick.symbol, "KRX:005930");
      assert.ok(tick.price.eq(71500));
      assert.ok(tick.quantity.eq(15));
      assert.equal(stream.isConnected, true);
    } finally { stream.close(); }
  }));

test("kiwoom: PING 은 받은 그대로 돌려보낸다", () =>
  withServer(async (url, connections) => {
    const stream = new KiwoomMarketStream({ wsUrl: url, token: async () => "T" });
    try {
      stream.connect();
      const conn = await connections.take();
      await conn.received.take(); // LOGIN
      const ping = '{"trnm":"PING"}';
      conn.send(ping);
      assert.equal(await conn.received.take(), ping);
    } finally { stream.close(); }
  }));

test("kiwoom: 서버가 끊으면 재접속해 다시 로그인하고 등록한다", () =>
  withServer(async (url, connections) => {
    const stream = new KiwoomMarketStream({ wsUrl: url, token: async () => "T" });
    try {
      stream.subscribeTrades(["005930"], () => {});
      stream.connect();
      const first = await connections.take();
      await first.received.take(); // LOGIN
      first.socket.close(1000, "bye");
      const second = await connections.take(10_000);
      assert.equal(JSON.parse(await second.received.take()).trnm, "LOGIN");
      second.send('{"trnm":"LOGIN","return_code":0}');
      assert.equal(JSON.parse(await second.received.take()).trnm, "REG");
    } finally { stream.close(); }
  }));

test("kis: 호가 구독 - H0STASP0 프레임을 요청 표기 심볼의 호가창으로 전달한다", () =>
  withServer(async (url, connections) => {
    const books = new Queue<OrderBookTick>();
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K" });
    try {
      stream.subscribeOrderBook(["KRX:005930"], (b) => books.put(b));
      stream.connect();
      const conn = await connections.take();
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.body.input.tr_id, "H0STASP0");
      assert.equal(subscribe.body.input.tr_key, "005930");
      conn.send(`0|H0STASP0|001|${BOOK_FIELDS}`);
      const book = await books.take();
      assert.equal(book.symbol, "KRX:005930");
      assert.equal(book.asks.length, 10);
      assert.ok(book.asks[0].price.eq(250500));
      assert.ok(book.asks[0].quantity.eq(1000));
      assert.ok(book.bids[0].price.eq(250000));
      assert.ok(book.bids[0].quantity.eq(2000));
      assert.ok(book.asks[9].price.eq(255000));
      assert.ok(book.totalAskQuantity!.eq(55000));
      assert.ok(book.totalBidQuantity!.eq(65000));
    } finally { stream.close(); }
  }));

test("kis: 주문 통보 - 구독 응답의 key·iv 로 암호화 프레임을 복호화해 이벤트로 전달한다", () =>
  withServer(async (url, connections) => {
    const events = new Queue<OrderEvent>();
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K", htsId: "HTSUSER" });
    try {
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      const subscribe = JSON.parse(await conn.received.take());
      assert.equal(subscribe.body.input.tr_id, "H0STCNI9"); // 모의
      assert.equal(subscribe.body.input.tr_key, "HTSUSER");

      conn.send(`{"header":{"tr_id":"H0STCNI9","tr_key":"HTSUSER","encrypt":"Y"},"body":{"rt_cd":"0","msg_cd":"OPSP0000","msg1":"SUBSCRIBE SUCCESS","output":{"iv":"${IV}","key":"${KEY}"}}}`);
      const accepted = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^0^0^105530^0^1^1^00950^3^홍길동^0^^N^^^^삼성전자^250000";
      const filled = "HTSUSER^50199202^0000012345^0000000000^02^0^00^0^005930^3^250000^105531^0^2^2^00950^3^홍길동^0^^N^^^^삼성전자^250000";
      conn.send(`1|H0STCNI9|001|${kisEncrypt(accepted, KEY, IV)}`);
      conn.send(`1|H0STCNI9|001|${kisEncrypt(filled, KEY, IV)}`);

      const e1 = await events.take();
      assert.equal(e1.type, "ACCEPTED");
      assert.equal(e1.orderId, "0000012345");
      assert.equal(e1.symbol, "005930");
      assert.equal(e1.side, "BUY");
      assert.ok(e1.quantity!.eq(3));
      assert.ok(e1.price!.eq(250000));
      const e2 = await events.take();
      assert.equal(e2.type, "FILLED");
      assert.ok(e2.quantity!.eq(3));
      assert.ok(e2.price!.eq(250000));
    } finally { stream.close(); }
  }));

test("kis: HTS ID 없이 주문 통보를 구독하면 실패한다", () => {
  const stream = new KisMarketStream({ wsUrl: "ws://127.0.0.1:1/", custtype: "P", approvalKey: async () => "K" });
  assert.throws(() => stream.subscribeOrderEvents(() => {}), /htsId/);
});

test("kis: AES 복호화는 암호화의 역이다", () => {
  const plain = "005930^105530^250000";
  assert.equal(kisDecrypt(kisEncrypt(plain, KEY, IV), KEY, IV), plain);
});

test("kiwoom: 호가·주문체결 등록 - 0D 는 종목으로, 00 은 빈 item 으로 REG 하고 REAL 을 각 리스너에 전달한다", () =>
  withServer(async (url, connections) => {
    const books = new Queue<OrderBookTick>();
    const events = new Queue<OrderEvent>();
    const stream = new KiwoomMarketStream({ wsUrl: url, token: async () => "T" });
    try {
      stream.subscribeOrderBook(["KRX:005930"], (b) => books.put(b));
      stream.subscribeOrderEvents((e) => events.put(e));
      stream.connect();
      const conn = await connections.take();
      await conn.received.take(); // LOGIN
      conn.send('{"trnm":"LOGIN","return_code":0}');
      const regs = [JSON.parse(await conn.received.take()), JSON.parse(await conn.received.take())];
      const bookReg = regs.find((r) => r.data[0].type[0] === "0D");
      assert.deepEqual(bookReg.data[0].item, ["005930"]);
      const orderReg = regs.find((r) => r.data[0].type[0] === "00");
      assert.deepEqual(orderReg.data[0].item, [""]);

      conn.send(KIWOOM_BOOK_FRAME);
      const book = await books.take();
      assert.equal(book.symbol, "KRX:005930");
      assert.ok(book.asks[0].price.eq(250500));
      assert.ok(book.asks[0].quantity.eq(1000));
      assert.ok(book.bids[0].price.eq(250000));
      assert.ok(book.bids[1].price.eq(249500));
      assert.ok(book.totalAskQuantity!.eq(55000));

      conn.send(KIWOOM_ORDER_FRAME);
      const e = await events.take();
      assert.equal(e.type, "FILLED");
      assert.equal(e.orderId, "0000012345");
      assert.equal(e.symbol, "005930");
      assert.equal(e.side, "BUY");
      assert.ok(e.quantity!.eq(1));
      assert.ok(e.price!.eq(250000));
      assert.ok(e.remainingQuantity!.eq(0));
    } finally { stream.close(); }
  }));
