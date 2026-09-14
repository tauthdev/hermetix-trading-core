/**
 * NH PLUG 실시간 스트림. ⚠️ 문서 기반 구현, 실측 전 — 포털 openapi.json x-realtime-channels·API 가이드·공식 Python SDK 에서 역추적 (Kotlin NhMarketStream 과 동일 프로토콜).
 *
 * - 접속: 모의 wss://moapi.nhplug.com:17070/websocket, 운영 wss://api.nhplug.com:7070/websocket (경로 /websocket 필수). 핸드셰이크 헤더 없음
 * - 인증: 로그인 프레임 없이 매 구독 메시지의 header.token (REST 접근토큰 그대로)
 * - 구독: {"header":{"token","tr_type":"1"|"2"},"body":{"tr_cd":<채널>,"tr_key":<종목코드>}}. 통보 채널은 tr_key 빈 문자열
 * - 채널: 체결 KRX oc / NXT nc / 통합 mc, 호가 ob / nb / mb — 시장을 채널코드로 고른다(marketCd). 통보는 체결 d2 + 접수 d3 (국내주식/파생 공용, itemgb 로 구분)
 * - ACK: {"header":{"tr_type","tr_cd","rsp_cd":"00000","rsp_msg"},"body":{"tr_key":[…]}} — 데이터 푸시 header 에는 rsp_cd/tr_type 이 없다
 * - 데이터: {"header":{"tr_cd","tr_key"},"body":{…}} (통보는 header 에 tr_key 없음). 값은 문자열이지만 숫자도 허용해 파싱
 * - heartbeat 없음(문서 명시) — 조용한 게 정상이므로 유휴 감시를 끈다. 암호화 없음
 *
 * 문서로 확정하지 못한 점(실측 필요): sign/kospigb/janggubun/ordercd/order_type 코드값(sign 은 REST 와 같은 4/5/8/9 하락으로 가정),
 * movolume=이번 체결량·new_volume=누적, orderno 와 REST mkt_orr_no 의 동일성(선행 0 무시 비교), 거부가 d3 없이 d2(rejgb=1)로만 오는지,
 * 시간 필드에 날짜·타임존 없음(오늘 KST), 모의(17070)에서 시세 채널이 오는지(포털은 "미제공").
 */
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import { kstToday } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

/** 통보의 issuecd 는 12자리(선행 0)·A 접두일 수 있다 → 6자리 코드 (nh.ts 의 nhNormalizeCode 와 같은 규칙, 순환 import 회피용 복제) */
const nhNormalizeCode = (raw: unknown): string => {
  const t = String(raw ?? "").trim().replace(/^A/, "");
  return t.length > 6 && /^\d+$/.test(t) ? t.slice(-6) : t;
};

export const NH_ORDER_CHANNELS = ["d2", "d3"] as const;
const FALLING_SIGNS = new Set(["4", "5", "8", "9"]);
const BOOK_PREFIXES = ["", "P_", "S_", "S4_", "S5_", "S6_", "S7_", "S8_", "S9_", "S10_"];

export interface NhStreamOptions {
  wsUrl: string;
  /** 접속·구독마다 호출 — REST 접근토큰 */
  token: () => Promise<string>;
  /** KRX | NXT | UNT — 체결·호가 채널코드를 고른다 (REST 의 market_cd 와 같은 값) */
  marketCd?: string;
  /** 비우지 않으면 통보의 accountno 가 같은 것만 전달한다 */
  accountNo?: string;
}

type Json = Record<string, unknown>;

const num = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/,/g, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};
const text = (b: Json, field: string): string => String(b[field] ?? "").trim();

/** "HH:MM:SS" 또는 "HHMMSS" (KST, 날짜 없음 → today) */
const kstTime = (raw: string, today: string): Date => kstDateTime(today, raw.replace(/\D/g, "").padStart(6, "0").slice(0, 6));

/** 시장 구분 → 채널코드 (tr_cd 는 대소문자를 구분하므로 정규화하지 않는다) */
export function nhChannels(marketCd = "KRX"): { trade: string; book: string } {
  switch (marketCd.toUpperCase()) {
    case "NXT": return { trade: "nc", book: "nb" };
    case "UNT": return { trade: "mc", book: "mb" };
    default: return { trade: "oc", book: "ob" };
  }
}

export class NhMarketStream extends ReconnectingWebSocket implements MarketStream {
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  private readonly requestedSymbols = new Map<string, string>();
  readonly tradeChannel: string;
  readonly bookChannel: string;

  constructor(private readonly options: NhStreamOptions) {
    super("nh", 30_000, 0);
    const ch = nhChannels(options.marketCd);
    this.tradeChannel = ch.trade;
    this.bookChannel = ch.book;
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  protected uri(): string { return this.options.wsUrl; }

  protected async onOpen(): Promise<void> {
    const t = await this.options.token();
    for (const code of this.tradeListeners.keys()) this.send(this.message(t, "1", this.tradeChannel, code));
    for (const code of this.bookListeners.keys()) this.send(this.message(t, "1", this.bookChannel, code));
    if (this.orderListeners.length > 0) for (const ch of NH_ORDER_CHANNELS) this.send(this.message(t, "1", ch, ""));
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes = this.register(symbols, listener, this.tradeListeners);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.map((c) => this.message(t, "1", this.tradeChannel, c)));
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    const newCodes = this.register(symbols, listener, this.bookListeners);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.map((c) => this.message(t, "1", this.bookChannel, c)));
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    const first = this.orderListeners.length === 0;
    this.orderListeners.push(listener);
    if (first && this.isSocketOpen) this.sendLater((t) => NH_ORDER_CHANNELS.map((ch) => this.message(t, "1", ch, "")));
  }

  private register<L>(symbols: string[], listener: L, target: Map<string, L[]>): string[] {
    const newCodes: string[] = [];
    for (const symbol of symbols) {
      const code = symbolCode(symbol);
      if (!this.requestedSymbols.has(code)) this.requestedSymbols.set(code, symbol);
      let list = target.get(code);
      if (!list) { list = []; target.set(code, list); newCodes.push(code); }
      list.push(listener);
    }
    return newCodes;
  }

  private sendLater(build: (token: string) => string[]): void {
    this.options.token()
      .then((t) => { for (const m of build(t)) this.send(m); })
      .catch((e) => console.error(`ERROR hermetix nh stream: 토큰 조회 실패 - ${e}`));
  }

  protected onMessage(raw: string): void {
    let node: Json;
    try { node = JSON.parse(raw); } catch { return; }
    const header = (node.header ?? {}) as Json;
    const trCd = String(header.tr_cd ?? "");
    if ("rsp_cd" in header || "tr_type" in header) {
      const rspCd = String(header.rsp_cd ?? "");
      const msg = String(header.rsp_msg ?? "");
      if (rspCd === "00000") console.log(`INFO hermetix nh stream: ${String(header.tr_type) === "2" ? "unsubscribed" : "subscribed"} ${trCd} ${JSON.stringify((node.body as Json | undefined)?.tr_key ?? "")} (${msg})`);
      else console.warn(`WARN hermetix nh stream: ${trCd} rsp_cd=${rspCd} ${msg}`);
      return;
    }
    if (!node.body || typeof node.body !== "object") return;
    switch (trCd) {
      case "oc": case "nc": case "mc":
        for (const tick of parseNhTrade(node)) this.deliver(tick, this.tradeListeners, "리스너");
        break;
      case "ob": case "nb": case "mb":
        for (const tick of parseNhOrderBook(node)) this.deliver(tick, this.bookListeners, "호가 리스너");
        break;
      case "d2": case "d3":
        for (const event of parseNhOrderEvents(node, kstToday(), this.options.accountNo ?? "")) {
          for (const l of this.orderListeners) {
            try { l(event); } catch (e) { console.error(`ERROR hermetix nh stream: 주문 통보 리스너 오류 / ${event.orderId}: ${e}`); }
          }
        }
        break;
      default:
        break;
    }
  }

  private deliver<T extends { symbol: string }>(tick: T, target: Map<string, ((t: T) => void)[]>, label: string): void {
    const code = tick.symbol;
    const symbol = this.requestedSymbols.get(code) ?? code;
    const normalized = symbol === code ? tick : { ...tick, symbol };
    for (const listener of target.get(code) ?? []) {
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix nh stream: ${label} 오류 / ${symbol}: ${e}`); }
    }
  }

  private message(token: string, trType: string, trCd: string, trKey: string): string {
    return JSON.stringify({ header: { token, tr_type: trType }, body: { tr_cd: trCd, tr_key: trKey } });
  }
}

/** 체결 프레임(oc/nc/mc) → 체결. 심볼은 header.tr_key 또는 body.code 그대로 (요청 표기 복원은 스트림이 한다) */
export function parseNhTrade(node: Json, today: string = kstToday()): TradeTick[] {
  const body = node.body as Json | undefined;
  if (!body || typeof body !== "object") return [];
  try {
    const sign = text(body, "sign");
    const falling = FALLING_SIGNS.has(sign);
    let change = num(body.change);
    if (change && falling && change.gt(0)) change = change.neg();
    let rate = num(body.chrate);
    if (rate && falling && rate.gt(0)) rate = rate.neg();
    const price = num(body.price);
    if (!price) return [];
    const cumulative = num(body.new_volume) ?? num(body.volume);
    return [{
      symbol: text((node.header ?? {}) as Json, "tr_key") || text(body, "code"),
      price,
      quantity: num(body.movolume) ?? new Decimal(0),
      timestamp: kstTime(text(body, "time"), today),
      askPrice: num(body.offer),
      bidPrice: num(body.bid),
      cumulativeVolume: cumulative ? cumulative.toNumber() : null,
      change,
      changeRate: rate ? rate.div(100) : null,
    }];
  } catch { return []; }
}

/** 호가 프레임(ob/nb/mb) → 호가창. 1단계 접두 없음, 2단계 P_, 3단계 S_, 4~10단계 S4_~S10_. 0 호가는 뺀다 */
export function parseNhOrderBook(node: Json, today: string = kstToday()): OrderBookTick[] {
  const body = node.body as Json | undefined;
  if (!body || typeof body !== "object") return [];
  try {
    const levels = (priceField: string, qtyField: string): OrderBookLevel[] => {
      const out: OrderBookLevel[] = [];
      for (const p of BOOK_PREFIXES) {
        const price = num(body[`${p}${priceField}`]);
        if (!price || price.isZero()) continue;
        out.push({ price, quantity: num(body[`${p}${qtyField}`]) ?? new Decimal(0) });
      }
      return out;
    };
    return [{
      symbol: text((node.header ?? {}) as Json, "tr_key") || text(body, "code"),
      timestamp: kstTime(text(body, "hotime"), today),
      asks: levels("offer", "offerrem"),
      bids: levels("bid", "bidrem"),
      totalAskQuantity: num(body.T_offerrem),
      totalBidQuantity: num(body.T_bidrem),
    }];
  } catch { return []; }
}

/**
 * 통보 프레임 → 이벤트. d3(접수) → ACCEPTED, d2 → rejgb=1 REJECTED / ucgb 0 체결 FILLED, 1 정정 MODIFIED, 2 취소·3 효력해제 CANCELED.
 * 국내주식(itemgb=1)만, accountNo 가 주어지면 계좌가 같은 것만.
 */
export function parseNhOrderEvents(node: Json, today: string = kstToday(), accountNo = ""): OrderEvent[] {
  const trCd = text((node.header ?? {}) as Json, "tr_cd");
  const b = node.body as Json | undefined;
  if (!b || typeof b !== "object" || !(NH_ORDER_CHANNELS as readonly string[]).includes(trCd)) return [];
  if (text(b, "itemgb") !== "1") return [];
  if (accountNo && text(b, "accountno") && text(b, "accountno") !== accountNo) return [];
  try {
    const side: OrderSide | null = text(b, "slbygb") === "1" ? "SELL" : text(b, "slbygb") === "2" ? "BUY" : null;
    const originalRaw = text(b, "orgordno");
    const originalOrderId = originalRaw && originalRaw.replace(/^0+/, "") !== "" ? originalRaw : null;
    if (trCd === "d3") {
      return [{
        orderId: text(b, "orderno"), type: "ACCEPTED", timestamp: kstTime(text(b, "order_time"), today),
        symbol: nhNormalizeCode(text(b, "issuecd")), side,
        quantity: num(b.ordergty), price: num(b.orderprc), remainingQuantity: null, originalOrderId, reason: null,
      }];
    }
    let type: OrderEventType;
    const ucgb = text(b, "ucgb");
    if (text(b, "rejgb") === "1") type = "REJECTED";
    else if (ucgb === "1") type = "MODIFIED";
    else if (ucgb === "2" || ucgb === "3") type = "CANCELED";
    else type = "FILLED";
    return [{
      orderId: text(b, "orderno"), type, timestamp: kstTime(text(b, "conctime"), today),
      symbol: nhNormalizeCode(text(b, "issuecd")), side,
      quantity: num(b.concgty), price: num(b.concprc), remainingQuantity: null, originalOrderId: null, reason: null,
    }];
  } catch { return []; }
}
