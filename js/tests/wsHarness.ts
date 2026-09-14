/** 스트림 테스트 공용 하네스 — 로컬 ws 서버로 브로커 프로토콜을 재생한다 (stream.test.ts 의 것을 nh/db/ls/toss 테스트가 함께 쓴다). */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { WebSocketServer, type WebSocket as ServerSocket } from "ws";
import type { OrderBookTick, OrderEvent, TradeTick } from "../src/models.js";

/** 도착 순서대로 꺼낼 수 있는 큐 — 타임아웃 시 실패 */
export class Queue<T> {
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
  get size(): number { return this.items.length; }
}

export class ServerConnection {
  readonly received = new Queue<string>();
  constructor(readonly socket: ServerSocket, readonly request: IncomingMessage) {
    socket.on("message", (data) => this.received.put(data.toString()));
  }
  send(text: string) { this.socket.send(text); }
  header(name: string): string | undefined { const v = this.request.headers[name.toLowerCase()]; return Array.isArray(v) ? v[0] : v; }
}

export async function withServer(fn: (url: string, connections: Queue<ServerConnection>) => Promise<void>) {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
  const connections = new Queue<ServerConnection>();
  const all: ServerConnection[] = [];
  server.on("connection", (socket, request) => { const c = new ServerConnection(socket, request); all.push(c); connections.put(c); });
  await new Promise<void>((r) => server.once("listening", r));
  const { port } = server.address() as { port: number };
  try {
    await fn(`ws://127.0.0.1:${port}/`, connections);
  } finally {
    for (const c of all) c.socket.terminate();
    await new Promise<void>((r) => server.close(() => r()));
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export const TODAY = "2026-09-14";

export const kstTime = (d: Date) =>
  new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Seoul", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(d);

// ---------------------------------------------------------------- 픽스처 stream 섹션

const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);

export interface ExpectedTrade {
  symbol: string; price: string; quantity: string; time: string; askPrice: string | null; bidPrice: string | null;
  cumulativeVolume: number | null; change: string | null; changeRate: string | null;
}
export interface Level { price: string; quantity: string; }
export interface ExpectedBook { symbol: string; time: string; asks: Level[]; bids: Level[]; totalAskQuantity: string | null; totalBidQuantity: string | null; }
export interface ExpectedEvent {
  orderId: string; type: string; time: string; symbol: string; side: string; quantity: string; price: string;
  remainingQuantity?: string; originalOrderId?: string;
}
export interface Section<E> { channel?: string; frames: string[]; expected: E[]; measured?: boolean; }
export type StreamFixture = Section<ExpectedTrade> & { orderBook: Section<ExpectedBook>; orderEvents: Section<ExpectedEvent> };

export function streamFixture(broker: string): StreamFixture {
  const fx = JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as { stream: StreamFixture };
  assert.ok(fx.stream, `${broker} 픽스처에 stream 섹션이 없다`);
  return fx.stream;
}

/** 기대 시각은 "HH:mm:ss" 또는 초가 0 이면 "HH:mm" 으로 적혀 있을 수 있다 */
function assertTime(actual: Date, expected: string, label: string) {
  const t = kstTime(actual);
  assert.ok(t === expected || t === `${expected}:00`, `${label} time ${t} != ${expected}`);
}

export function assertTrades(ticks: TradeTick[], expected: ExpectedTrade[]) {
  assert.equal(ticks.length, expected.length, "tick count");
  ticks.forEach((tick, i) => {
    const e = expected[i];
    assert.equal(tick.symbol, e.symbol);
    assert.ok(tick.price.eq(e.price), `price ${tick.price} != ${e.price}`);
    assert.ok(tick.quantity.eq(e.quantity), `quantity ${tick.quantity} != ${e.quantity}`);
    if (e.askPrice === null) assert.equal(tick.askPrice, null); else assert.ok(tick.askPrice!.eq(e.askPrice), "askPrice");
    if (e.bidPrice === null) assert.equal(tick.bidPrice, null); else assert.ok(tick.bidPrice!.eq(e.bidPrice), "bidPrice");
    assert.equal(tick.cumulativeVolume, e.cumulativeVolume);
    if (e.change === null) assert.equal(tick.change, null); else assert.ok(tick.change!.eq(e.change), `change ${tick.change} != ${e.change}`);
    if (e.changeRate === null) assert.equal(tick.changeRate, null); else assert.ok(tick.changeRate!.eq(e.changeRate), `changeRate ${tick.changeRate} != ${e.changeRate}`);
    assertTime(tick.timestamp, e.time, "tick");
  });
}

export function assertBooks(books: OrderBookTick[], expected: ExpectedBook[]) {
  assert.equal(books.length, expected.length, "book count");
  books.forEach((book, i) => {
    const e = expected[i];
    assert.equal(book.symbol, e.symbol);
    assertTime(book.timestamp, e.time, "book");
    const levels = (ls: { price: { toFixed(): string }; quantity: { toFixed(): string } }[]) => ls.map((l) => ({ price: l.price.toFixed(), quantity: l.quantity.toFixed() }));
    assert.deepEqual(levels(book.asks), e.asks);
    assert.deepEqual(levels(book.bids), e.bids);
    if (e.totalAskQuantity === null) assert.equal(book.totalAskQuantity, null); else assert.ok(book.totalAskQuantity!.eq(e.totalAskQuantity), "totalAskQuantity");
    if (e.totalBidQuantity === null) assert.equal(book.totalBidQuantity, null); else assert.ok(book.totalBidQuantity!.eq(e.totalBidQuantity), "totalBidQuantity");
  });
}

export function assertEvents(events: OrderEvent[], expected: ExpectedEvent[]) {
  assert.equal(events.length, expected.length, "event count");
  events.forEach((ev, i) => {
    const e = expected[i];
    assert.equal(ev.orderId, e.orderId);
    assert.equal(ev.type, e.type);
    assertTime(ev.timestamp, e.time, "event");
    assert.equal(ev.symbol, e.symbol);
    assert.equal(ev.side, e.side);
    assert.ok(ev.quantity!.eq(e.quantity), `quantity ${ev.quantity} != ${e.quantity}`);
    assert.ok(ev.price!.eq(e.price), `price ${ev.price} != ${e.price}`);
    if (e.remainingQuantity !== undefined) assert.ok(ev.remainingQuantity!.eq(e.remainingQuantity), "remainingQuantity");
    if (e.originalOrderId !== undefined) assert.equal(ev.originalOrderId, e.originalOrderId);
    else assert.equal(ev.originalOrderId ?? null, null);
  });
}
