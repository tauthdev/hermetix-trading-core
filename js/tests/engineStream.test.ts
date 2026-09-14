/** ON_TRADE 트리거 — 스트림 틱이 tick 을 촉발하고, 합쳐지고, 최소 간격을 지키고, 현재가를 REST 대신 틱에서 가져오는지 (Kotlin StrategyEngineStreamTest 대응). */
import assert from "node:assert/strict";
import { test } from "node:test";
import type { BrokerClient, MarketStream, StreamingBrokerClient, TradeListener } from "../src/broker.js";
import { StrategyEngine } from "../src/engine.js";
import { Decimal } from "../src/models.js";
import type { BrokerCapabilities, Quote, StreamChannel, TradeTick } from "../src/models.js";
import type { Signal, Strategy, StrategySpec } from "../src/strategy.js";
import { StrategyContext } from "../src/strategy.js";

/** 연결 없이 틱을 밀어 넣을 수 있는 가짜 스트림 */
class FakeStream implements MarketStream {
  readonly listeners: [string[], TradeListener][] = [];
  closed = false;
  get isConnected() { return !this.closed; }
  connect() {}
  subscribeTrades(symbols: string[], listener: TradeListener) { this.listeners.push([symbols, listener]); }
  close() { this.closed = true; }
  emit(symbol: string, price: string) {
    const tick: TradeTick = { symbol, price: new Decimal(price), quantity: new Decimal(1), timestamp: new Date(), cumulativeVolume: 10 };
    for (const [symbols, listener] of this.listeners) if (symbols.includes(symbol)) listener(tick);
  }
}

class FakeBroker implements BrokerClient {
  readonly capabilities: BrokerCapabilities;
  environment = "PAPER" as const;
  quoteCalls: string[][] = [];
  constructor(streams: StreamChannel[]) {
    this.capabilities = {
      brokerId: "test", market: "KRX", currency: "KRW", candleIntervals: new Set(["1d"]),
      clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
      streams: new Set(streams),
    };
  }
  async getQuotes(symbols: string[]): Promise<Quote[]> {
    this.quoteCalls.push(symbols);
    return symbols.map((s) => ({ symbol: s, price: new Decimal(1), bidPrice: null, askPrice: null, volume: 0, change: null, changeRate: null, timestamp: new Date() }));
  }
  async getCandles() { return []; }
  async getCalendar() { return []; }
  async getAccount() { return { accountId: "acc", currency: "KRW", cash: new Decimal(1), portfolioValue: new Decimal(1), status: "ACTIVE" }; }
  async getHoldings() { return []; }
  async getBuyingPower() { return new Decimal(1_000_000); }
  async createOrder(): Promise<never> { throw new Error("unused"); }
  async getOrders() { return []; }
  async getOrder(): Promise<never> { throw new Error("unused"); }
  async cancelOrder(): Promise<never> { throw new Error("unused"); }
  async getFills() { return []; }
}

class StreamingFakeBroker extends FakeBroker implements StreamingBrokerClient {
  constructor(readonly stream: FakeStream) { super(["TRADES"]); }
  openStream() { return this.stream; }
}

class RecordingStrategy implements Strategy {
  readonly calls: [number, StrategyContext][] = [];
  constructor(readonly spec: StrategySpec) {}
  decide(ctx: StrategyContext): Signal[] { this.calls.push([Date.now(), ctx]); return []; }
}

const spec = (symbols: string[], minTickIntervalMs = 300): StrategySpec =>
  ({ name: "s", symbols, pollIntervalSeconds: 3600, regularHoursOnly: false, trigger: "ON_TRADE", minTickIntervalMs });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function awaitCalls(strategy: RecordingStrategy, atLeast: number, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (strategy.calls.length < atLeast && Date.now() < deadline) await sleep(20);
  assert.ok(strategy.calls.length >= atLeast, `calls=${strategy.calls.length} < ${atLeast}`);
}

/** run() 을 백그라운드로 돌리고 정리 함수를 돌려준다 */
function start(engine: StrategyEngine) {
  const running = engine.run();
  return async () => { engine.stop(); await running; };
}

test("틱이 오면 전략이 호출되고 현재가는 REST 대신 스트림 틱에서 온다", async () => {
  const stream = new FakeStream();
  const broker = new StreamingFakeBroker(stream);
  const strategy = new RecordingStrategy(spec(["005930"]));
  const stop = start(new StrategyEngine(broker, [strategy]));
  try {
    assert.deepEqual(stream.listeners[0][0], ["005930"]);
    await awaitCalls(strategy, 1); // 기동 폴링 틱 — 스트림 틱이 없어 REST 현재가
    stream.emit("005930", "71500");
    await awaitCalls(strategy, 2);
    assert.ok(strategy.calls[1][1].quote("005930")!.price.eq(71500));
    assert.equal(broker.quoteCalls.length, 1); // 기동 폴링 틱 1회뿐
  } finally { await stop(); }
});

test("몰려온 틱은 하나로 합쳐지고 최소 간격을 지킨다", async () => {
  const stream = new FakeStream();
  const broker = new StreamingFakeBroker(stream);
  const strategy = new RecordingStrategy(spec(["005930"], 400));
  const stop = start(new StrategyEngine(broker, [strategy]));
  try {
    await awaitCalls(strategy, 1);
    await sleep(450);
    for (let i = 0; i < 20; i++) stream.emit("005930", `7150${i}`);
    await sleep(1500);
    const afterBurst = strategy.calls.length; // 즉시 1회 + (tick 도중 도착한 틱) 최소 간격 뒤 1회
    assert.ok(afterBurst >= 2 && afterBurst <= 3, `afterBurst=${afterBurst}`);

    stream.emit("005930", "72000");
    await awaitCalls(strategy, afterBurst + 1);
    await sleep(300);
    assert.equal(strategy.calls.length, afterBurst + 1);

    const times = strategy.calls.map(([t]) => t);
    for (let i = 1; i < times.length; i++) assert.ok(times[i] - times[i - 1] >= 360, `gap ${times[i] - times[i - 1]}`);
  } finally { await stop(); }
});

test("일부 심볼만 틱이 있으면 현재가는 REST 로 간다", async () => {
  const stream = new FakeStream();
  const broker = new StreamingFakeBroker(stream);
  const strategy = new RecordingStrategy(spec(["005930", "000660"]));
  const stop = start(new StrategyEngine(broker, [strategy]));
  try {
    await awaitCalls(strategy, 1);
    stream.emit("005930", "71500");
    await awaitCalls(strategy, 2);
    assert.equal(broker.quoteCalls.length, 2); // 000660 틱이 없으므로 스트림 틱도 REST
  } finally { await stop(); }
});

test("스트림 미지원 브로커에서 ON_TRADE 는 폴링으로 스케줄된다", async () => {
  const broker = new FakeBroker([]);
  const strategy = new RecordingStrategy(spec(["005930"]));
  const engine = new StrategyEngine(broker, [strategy]);
  assert.equal(engine.strategies.length, 1);
  const stop = start(engine);
  try {
    await awaitCalls(strategy, 1);
    assert.equal(engine.streamConnected, false);
  } finally { await stop(); }
});

test("stop 은 스트림을 닫는다", async () => {
  const stream = new FakeStream();
  const engine = new StrategyEngine(new StreamingFakeBroker(stream), [new RecordingStrategy(spec(["005930"]))]);
  const running = engine.run();
  await sleep(50);
  assert.equal(engine.streamConnected, true);
  engine.stop();
  await running;
  assert.equal(stream.closed, true);
});
