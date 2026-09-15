/**
 * 토스증권 실시간 스트림. 공식 AsyncAPI 1.2.2 문서 기반, ⚠️ 실측 전 (모의투자 서버가 없어 실계좌로만 검증 가능). Kotlin TossMarketStream 과 동일 프로토콜.
 *
 * - wss://openapi-ws.tossinvest.com/ws/v1, 핸드셰이크에 Authorization: Bearer {access_token} — REST 와 같은 토큰. 접속 때만 검사된다
 *   (Node 22 내장 WebSocket 은 undici 의 init.headers 로 업그레이드 헤더를 싣는다)
 * - 구독은 선언형: 클라이언트가 보내는 JSON 배열 하나가 현재 구독 집합 전체다. 새 배열이 이전 집합을 통째로 대체한다.
 *   요소 {"type":"trade:kr","codes":["005930"]}, 첫 요소 {"id":"req-N"} 은 응답에 echo. type: trade:us/kr, orderbook:us/kr, personal:order(codes = accountSeq)
 * - 서버 프레임 type: subscriptions(subscribed[]·rejected[]{target,code,message}), message(topic = trade:kr:005930), error(server-shutdown 등), pong
 * - 180초 무송신 시 서버가 끊는다 → 60초마다 텍스트 프레임 PING (JSON 아님) → {"type":"pong"}
 * - 한도: 계정당 연결 2개, 연결당 구독 100개, 선언 5회/초 → 구독 변경은 declareDelayMs 동안 모아 한 번에 보낸다
 * - trade/orderbook 은 유실 가능, personal:order 는 세션 내 무손실. 재접속 뒤 놓친 이벤트는 재전송되지 않는다
 *
 * 프레임: trade data price/volume/timestamp(ISO +09:00)/currency — 누적거래량·등락·매수매도 구분 없음; orderbook data asks[]/bids[] {price, volume} 최우선부터;
 * personal:order data event(PENDING/PARTIAL_FILL/FILL/CANCELING/CANCELED/REPLACING/REPLACED/REJECTED/CANCEL_REJECTED/REPLACE_REJECTED), order 스냅샷
 * (execution.filledQuantity 는 누적이라 이번 체결량은 직전 스냅샷과의 차이).
 * 실측 필요: 호가 단계 수, 취소·정정 시 어떤 orderId 로 이벤트가 오는지, 표준 ping 프레임이 유휴 타이머를 리셋하는지.
 */
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { parseSymbol } from "../models.js";
import type { BrokerUsage } from "../telemetry.js";
import { ReconnectingWebSocket } from "../stream.js";

export interface TossStreamOptions {
  /** 사용량 텔레메트리 핸들 (어댑터가 넘긴다) */
  usage?: BrokerUsage;
  wsUrl: string;
  /** 접속마다 호출 — REST 접근토큰 (재발급하면 이전 토큰이 무효가 되므로 REST 캐시를 그대로 쓴다) */
  token: () => Promise<string>;
  /** personal:order 구독 키 — accountSeq */
  accountSeq: () => Promise<string>;
  heartbeatMs?: number;
  /** 구독 변경을 모아 한 번에 선언하는 지연 (5회/초 한도) */
  declareDelayMs?: number;
}

type Json = Record<string, unknown>;

const num = (v: unknown): Decimal | null => {
  if (v === null || v === undefined || v === "") return null;
  try { return new Decimal(String(v).trim()); } catch { return null; }
};
const ts = (v: unknown): Date | null => { if (!v) return null; const d = new Date(String(v)); return Number.isNaN(d.getTime()) ? null : d; };

/** Hermetix 심볼 → topic 키 (KRX:005930/005930 → kr:005930, US:AAPL → us:AAPL) */
export function tossTopicKey(symbol: string): string {
  const { market, code } = parseSymbol(symbol);
  let m: string;
  if (market === null || market === "KRX") m = "kr";
  else if (market === "US") m = "us";
  else throw new Error(`토스 어댑터가 지원하지 않는 시장: ${market} (${symbol})`);
  return `${m}:${m === "us" ? code.toUpperCase() : code}`;
}

/** topic 의 시장·코드 → 정규 표기 (KRX:005930, US:AAPL) */
export const tossCanonicalSymbol = (market: string, code: string): string => (market === "us" ? `US:${code}` : `KRX:${code}`);

export class TossMarketStream extends ReconnectingWebSocket implements MarketStream {
  /** topic 키(kr:005930) → 리스너 */
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  /** topic 키 → 구독 요청 표기 */
  private readonly requestedSymbols = new Map<string, string>();
  /** 서버가 거부한 target(trade:kr:999999) — 다시 선언하면 또 거부되므로 뺀다 */
  private readonly rejectedTargets = new Set<string>();
  /** orderId → 직전 스냅샷의 누적 체결량 */
  private readonly filledSoFar = new Map<string, Decimal>();
  private pendingDeclare: NodeJS.Timeout | null = null;
  private requestCounter = 0;
  private readonly declareDelayMs: number;

  constructor(private readonly options: TossStreamOptions) {
    super("toss", 30_000, 0, options.heartbeatMs ?? 60_000, options.usage);
    this.declareDelayMs = options.declareDelayMs ?? 200;
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  protected uri(): string { return this.options.wsUrl; }

  protected async headers(): Promise<Record<string, string>> { return { Authorization: `Bearer ${await this.options.token()}` }; }

  protected async onOpen(): Promise<void> { await this.declareNow(); }

  protected onHeartbeat(): void { this.send("PING"); }

  close(): void {
    if (this.pendingDeclare) { clearTimeout(this.pendingDeclare); this.pendingDeclare = null; }
    super.close();
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    if (this.register(symbols, listener, this.tradeListeners)) this.scheduleDeclare();
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    if (this.register(symbols, listener, this.bookListeners)) this.scheduleDeclare();
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    const first = this.orderListeners.length === 0;
    this.orderListeners.push(listener);
    if (first) { this.options.usage?.streamSubscribed("ORDER_EVENTS"); this.scheduleDeclare(); }
  }

  /** true 면 새 topic 이 생겨 선언을 다시 보내야 한다 */
  private register<L>(symbols: string[], listener: L, target: Map<string, L[]>): boolean {
    let changed = false;
    for (const symbol of symbols) {
      const key = tossTopicKey(symbol);
      if (!this.requestedSymbols.has(key)) this.requestedSymbols.set(key, symbol);
      if (!target.has(key)) this.options.usage?.streamSubscribed((target as unknown) === (this.tradeListeners as unknown) ? "TRADES" : "ORDER_BOOK");
      let list = target.get(key);
      if (!list) { list = []; target.set(key, list); changed = true; }
      list.push(listener);
    }
    return changed;
  }

  private scheduleDeclare(): void {
    if (!this.isSocketOpen) return; // 접속되면 onOpen 이 전체를 선언한다
    if (this.pendingDeclare) clearTimeout(this.pendingDeclare);
    this.pendingDeclare = setTimeout(() => { this.pendingDeclare = null; void this.declareNow(); }, this.declareDelayMs);
    this.pendingDeclare.unref?.();
  }

  /** 현재 구독 집합 전체를 한 배열로 보낸다 */
  private async declareNow(): Promise<void> {
    const declaration = await this.buildDeclaration();
    if (declaration.length <= 1) return; // id 만 있으면 보낼 게 없다
    this.send(JSON.stringify(declaration));
  }

  async buildDeclaration(): Promise<Json[]> {
    const items: Json[] = [{ id: `req-${++this.requestCounter}` }];
    const codesFor = (prefix: string, keys: Iterable<string>, market: string): string[] =>
      [...keys].filter((k) => k.startsWith(`${market}:`)).map((k) => k.slice(market.length + 1))
        .filter((c) => !this.rejectedTargets.has(`${prefix}:${market}:${c}`)).sort();
    for (const market of ["kr", "us"]) {
      const codes = codesFor("trade", this.tradeListeners.keys(), market);
      if (codes.length > 0) items.push({ type: `trade:${market}`, codes });
    }
    for (const market of ["kr", "us"]) {
      const codes = codesFor("orderbook", this.bookListeners.keys(), market);
      if (codes.length > 0) items.push({ type: `orderbook:${market}`, codes });
    }
    if (this.orderListeners.length > 0) {
      let seq: string | null = null;
      try { seq = await this.options.accountSeq(); } catch (e) { console.warn(`WARN hermetix toss stream: accountSeq 조회 실패 - 주문 이벤트 구독 보류: ${e}`); }
      if (seq && !this.rejectedTargets.has(`personal:order:${seq}`)) items.push({ type: "personal:order", codes: [seq] });
    }
    return items;
  }

  protected onMessage(raw: string): void {
    let node: Json;
    try { node = JSON.parse(raw); } catch { return; }
    switch (String(node.type ?? "")) {
      case "pong": break;
      case "subscriptions": this.onSubscriptions(node); break;
      case "error": {
        const error = (node.error ?? {}) as Json;
        console.warn(`WARN hermetix toss stream: error ${error.code ?? ""} ${error.message ?? ""} (id=${node.id ?? ""})`);
        break;
      }
      case "message": this.onData(String(node.topic ?? ""), (node.data ?? {}) as Json); break;
      default: break;
    }
  }

  private onSubscriptions(node: Json): void {
    const subscribed = (node.subscribed as unknown[] | undefined) ?? [];
    console.log(`INFO hermetix toss stream: subscribed ${subscribed.length} (id=${node.id ?? ""})`);
    for (const r of ((node.rejected as Json[] | undefined) ?? [])) {
      const target = String(r.target ?? "");
      console.warn(`WARN hermetix toss stream: 구독 거부 ${target} ${r.code ?? ""} ${r.message ?? ""} — 선언에서 제외한다`);
      if (target) this.rejectedTargets.add(target);
    }
  }

  private onData(topic: string, data: Json): void {
    const parts = topic.split(":");
    if (parts.length < 3) return;
    const key = `${parts[1]}:${parts.slice(2).join(":")}`;
    switch (parts[0]) {
      case "trade": {
        const tick = parseTossTrade(topic, data);
        if (!tick) return;
        const requested = this.requestedSymbols.get(key);
        const out = requested ? { ...tick, symbol: requested } : tick;
        this.options.usage?.streamMessage("TRADES");
        for (const l of this.tradeListeners.get(key) ?? []) { try { l(out); } catch (e) { console.error(`ERROR hermetix toss stream: 리스너 오류 / ${out.symbol}: ${e}`); } }
        break;
      }
      case "orderbook": {
        const book = parseTossOrderBook(topic, data);
        if (!book) return;
        const requested = this.requestedSymbols.get(key);
        const out = requested ? { ...book, symbol: requested } : book;
        this.options.usage?.streamMessage("ORDER_BOOK");
        for (const l of this.bookListeners.get(key) ?? []) { try { l(out); } catch (e) { console.error(`ERROR hermetix toss stream: 호가 리스너 오류 / ${out.symbol}: ${e}`); } }
        break;
      }
      case "personal": {
        const order = (data.order ?? {}) as Json;
        const orderId = String(order.orderId ?? "");
        const event = parseTossOrderEvent(data, this.filledSoFar.get(orderId) ?? null);
        if (!event) return;
        const filled = num(((order.execution ?? {}) as Json).filledQuantity);
        if (filled) this.filledSoFar.set(orderId, filled);
        if (event.type === "CANCELED" || event.type === "REJECTED") this.filledSoFar.delete(orderId);
        this.options.usage?.streamMessage("ORDER_EVENTS");
        for (const l of this.orderListeners) { try { l(event); } catch (e) { console.error(`ERROR hermetix toss stream: 주문 이벤트 리스너 오류 / ${event.orderId}: ${e}`); } }
        break;
      }
      default: break;
    }
  }
}

/** trade:{kr|us}:{code} 데이터 → 체결. 누적거래량·등락·호가는 프레임에 없어 null */
export function parseTossTrade(topic: string, data: Json): TradeTick | null {
  const parts = topic.split(":");
  if (parts.length < 3 || parts[0] !== "trade") return null;
  const price = num(data.price);
  if (!price) return null;
  return {
    symbol: tossCanonicalSymbol(parts[1], parts.slice(2).join(":")),
    price,
    quantity: num(data.volume) ?? new Decimal(0),
    timestamp: ts(data.timestamp) ?? new Date(),
    askPrice: null, bidPrice: null, cumulativeVolume: null, change: null, changeRate: null,
  };
}

/** orderbook:{kr|us}:{code} 데이터 → 호가창. asks 오름차순·bids 내림차순으로 오므로 순서 그대로가 최우선부터 */
export function parseTossOrderBook(topic: string, data: Json): OrderBookTick | null {
  const parts = topic.split(":");
  if (parts.length < 3 || parts[0] !== "orderbook") return null;
  const levels = (raw: unknown): OrderBookLevel[] => {
    const out: OrderBookLevel[] = [];
    for (const l of ((raw as Json[] | undefined) ?? [])) {
      const price = num(l.price);
      if (!price) continue;
      out.push({ price, quantity: num(l.volume) ?? new Decimal(0) });
    }
    return out;
  };
  return {
    symbol: tossCanonicalSymbol(parts[1], parts.slice(2).join(":")),
    timestamp: ts(data.timestamp) ?? new Date(),
    asks: levels(data.asks),
    bids: levels(data.bids),
    totalAskQuantity: null,
    totalBidQuantity: null,
  };
}

/**
 * personal:order 데이터 → 주문 이벤트. previousFilled 는 같은 orderId 의 직전 누적 체결량 (없으면 이번이 첫 스냅샷).
 * CANCELING/REPLACING 은 중간 상태라 null.
 */
export function parseTossOrderEvent(data: Json, previousFilled: Decimal | null): OrderEvent | null {
  const order = (data.order ?? {}) as Json;
  const orderId = String(order.orderId ?? "");
  if (!orderId) return null;
  const execution = (order.execution ?? {}) as Json;
  const filled = num(execution.filledQuantity) ?? new Decimal(0);
  const quantity = num(order.quantity);
  const price = num(order.price);
  const eventName = String(data.event ?? "");
  const types: Record<string, OrderEventType> = {
    PENDING: "ACCEPTED", PARTIAL_FILL: "FILLED", FILL: "FILLED", CANCELED: "CANCELED", REPLACED: "MODIFIED",
    REJECTED: "REJECTED", CANCEL_REJECTED: "REJECTED", REPLACE_REJECTED: "REJECTED",
  };
  const type = types[eventName];
  if (!type) return null; // CANCELING, REPLACING, 미지 값
  const market = order.currency === "USD" ? "US" : "KRX";
  const symbol = order.symbol ? `${market}:${order.symbol}` : null;
  const delta = filled.minus(previousFilled ?? 0);
  const fillQuantity = delta.lt(0) ? filled : delta;
  const side: OrderSide | null = order.side === "BUY" ? "BUY" : order.side === "SELL" ? "SELL" : null;
  return {
    orderId, type, timestamp: ts(order.orderedAt) ?? new Date(), symbol, side,
    quantity: type === "FILLED" ? fillQuantity : quantity,
    price: type === "FILLED" ? (num(execution.averageFilledPrice) ?? price) : price,
    remainingQuantity: quantity ? quantity.minus(filled) : null,
    originalOrderId: null,
    reason: type === "REJECTED" ? eventName : null,
  };
}
