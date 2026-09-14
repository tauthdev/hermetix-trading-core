/** 웹소켓 흐름 — 로컬 ws 서버로 KIS/키움 프로토콜(구독·에코·재접속)을 재생한다 (Kotlin KisMarketStreamTest/KiwoomMarketStreamTest 대응). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";
import { KisMarketStream } from "../src/brokers/kisStream.js";
import { KiwoomMarketStream } from "../src/brokers/kiwoomStream.js";
import type { TradeTick } from "../src/models.js";

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
