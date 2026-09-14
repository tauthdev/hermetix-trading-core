/**
 * LS증권 OPEN API 실시간 스트림. ⚠️ 문서 기반 구현, 모의 실측 전 — 포털 실시간 TR 문서와 커뮤니티 클라이언트에서 역추적 (Kotlin LsMarketStream 과 동일 프로토콜).
 *
 * - 접속: 모의 wss://openapi.ls-sec.co.kr:29443/websocket, 실전 :9443/websocket. 핸드셰이크 헤더·로그인 프레임 없음
 * - 인증: 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음). 토큰은 익일 07:00 만료 → 재접속 시 캐시 토큰을 다시 싣는다
 * - 시세 등록/해제: {"header":{"token","tr_type":"3"|"4"},"body":{"tr_cd":"S3_","tr_key":"005930"}}
 * - 계좌 등록/해제: tr_type "1"/"2", tr_cd SC0(접수)·SC1(체결)·SC2(정정)·SC3(취소)·SC4(거부), tr_key 빈 문자열. TR 마다 따로 보낸다
 * - 응답: 등록 ACK 는 header 에 rsp_cd/rsp_msg 가 있고 body 가 없다. 데이터는 {"header":{"tr_cd","tr_key"},"body":{…}}, 값은 전부 문자열
 * - 하트비트 없음(문서 미기재) — 유휴 감시를 끈다
 *
 * KOSPI/KOSDAQ 선택: 서버는 종목으로 시장을 고르지 않는다 — 종목마다 KOSPI TR(S3_/H1_)과 KOSDAQ TR(K3_/HA_)을 둘 다 등록한다.
 * 맞지 않는 쪽은 조용히 비고, 등록 수만 2배가 된다 (한도 미문서).
 *
 * 필드: S3_/K3_ chetime HHMMSS·price·cvolume·volume(누적)·change(부호 없음)+sign(4·5 하락)·drate(부호 있는 %)·offerho/bidho·shcode;
 * H1_/HA_ hotime·offerho1~10/bidho1~10·offerrem1~10/bidrem1~10·totofferrem/totbidrem;
 * SC0 ordno·orgordno(신규 0)·bnstp(1 매도/2 매수)·ordqty·ordprice·ordtm HHMMSSmmm·shtcode(A접두);
 * SC1~SC4 ordno·orgordno·bnstp·execqty/execprc·unercqty·mdfycnfqty/mdfycnfprc·canccnfqty·rjtqty·exectime·shtnIsuno·ordxctptncode(11 체결/12 정정/13 취소/14 거부 추정)·msgcode
 * 실측 시 확인할 것: ACK JSON 키·rsp_cd 값, SC4 본문·거부 사유, 세션당 등록 한도, 07:00 토큰 만료 시 소켓 동작.
 */
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import { kstToday } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const LS_TR_TRADE_KOSPI = "S3_";
export const LS_TR_TRADE_KOSDAQ = "K3_";
export const LS_TR_BOOK_KOSPI = "H1_";
export const LS_TR_BOOK_KOSDAQ = "HA_";
export const LS_TR_ORDER_ACCEPTED = "SC0";
export const LS_TR_ORDER_FILLED = "SC1";
export const LS_TR_ORDER_MODIFIED = "SC2";
export const LS_TR_ORDER_CANCELED = "SC3";
export const LS_TR_ORDER_REJECTED = "SC4";
export const LS_TRADE_TRS = [LS_TR_TRADE_KOSPI, LS_TR_TRADE_KOSDAQ];
export const LS_BOOK_TRS = [LS_TR_BOOK_KOSPI, LS_TR_BOOK_KOSDAQ];
export const LS_ORDER_TRS = [LS_TR_ORDER_ACCEPTED, LS_TR_ORDER_FILLED, LS_TR_ORDER_MODIFIED, LS_TR_ORDER_CANCELED, LS_TR_ORDER_REJECTED];
export const LS_TR_TYPE_ACCOUNT_REGISTER = "1";
export const LS_TR_TYPE_ACCOUNT_UNREGISTER = "2";
export const LS_TR_TYPE_SUBSCRIBE = "3";
export const LS_TR_TYPE_UNSUBSCRIBE = "4";
export const LS_RSP_OK = "00000";
/** xingAPI 관례 sign: 1 상한 2 상승 3 보합 4 하한 5 하락 */
const FALLING_SIGNS = new Set(["4", "5"]);

export interface LsStreamOptions {
  wsUrl: string;
  /** 접속·구독마다 호출 — REST 접근토큰 */
  token: () => Promise<string>;
}

type Json = Record<string, unknown>;

const text = (b: Json, field: string): string => String(b[field] ?? "").trim();
const num = (b: Json, field: string): Decimal | null => {
  const t = text(b, field).replace(/,/g, "");
  if (!t) return null;
  try { return new Decimal(t); } catch { return null; }
};
/** HHMMSS 또는 HHMMSSmmm → 오늘 KST */
const kstTime = (raw: string, today: string): Date => kstDateTime(today, raw.trim().padStart(6, "0").slice(0, 6));
/** A005930 → 005930 */
const normalizeCode = (raw: string): string => { const t = raw.trim(); return t.length === 7 && t[0] === "A" ? t.slice(1) : t; };

/** ACK/오류 프레임(header 에 rsp_*·body 없음)이면 true — 데이터 파서는 건너뛴다 */
function isControl(node: Json): boolean {
  const header = (node.header ?? {}) as Json;
  const body = node.body as Json | null | undefined;
  return "rsp_msg" in header || "rsp_cd" in header || !body || typeof body !== "object" || Object.keys(body).length === 0;
}

/** 시세 프레임의 종목코드 — header.tr_key 6자리, 없으면 body.shcode */
function marketCode(node: Json): string {
  const key = text((node.header ?? {}) as Json, "tr_key");
  return key.length === 6 ? key : text((node.body ?? {}) as Json, "shcode");
}

export class LsMarketStream extends ReconnectingWebSocket implements MarketStream {
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  private readonly requestedSymbols = new Map<string, string>();

  constructor(private readonly options: LsStreamOptions) {
    super("ls", 30_000, 0);
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  protected uri(): string { return this.options.wsUrl; }

  protected async onOpen(): Promise<void> {
    const t = await this.options.token();
    for (const code of this.tradeListeners.keys()) for (const tr of LS_TRADE_TRS) this.send(this.message(t, LS_TR_TYPE_SUBSCRIBE, tr, code));
    for (const code of this.bookListeners.keys()) for (const tr of LS_BOOK_TRS) this.send(this.message(t, LS_TR_TYPE_SUBSCRIBE, tr, code));
    if (this.orderListeners.length > 0) for (const tr of LS_ORDER_TRS) this.send(this.message(t, LS_TR_TYPE_ACCOUNT_REGISTER, tr, ""));
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes = this.register(symbols, listener, this.tradeListeners);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.flatMap((c) => LS_TRADE_TRS.map((tr) => this.message(t, LS_TR_TYPE_SUBSCRIBE, tr, c))));
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    const newCodes = this.register(symbols, listener, this.bookListeners);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.flatMap((c) => LS_BOOK_TRS.map((tr) => this.message(t, LS_TR_TYPE_SUBSCRIBE, tr, c))));
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    const first = this.orderListeners.length === 0;
    this.orderListeners.push(listener);
    if (first && this.isSocketOpen) this.sendLater((t) => LS_ORDER_TRS.map((tr) => this.message(t, LS_TR_TYPE_ACCOUNT_REGISTER, tr, "")));
  }

  /** 체결 구독 해제 (tr_type 4). 리스너도 지운다 */
  unsubscribeTrades(symbols: string[]): void {
    const codes = symbols.map(symbolCode).filter((c) => this.tradeListeners.delete(c));
    if (codes.length > 0 && this.isSocketOpen) this.sendLater((t) => codes.flatMap((c) => LS_TRADE_TRS.map((tr) => this.message(t, LS_TR_TYPE_UNSUBSCRIBE, tr, c))));
  }

  /** 호가 구독 해제 (tr_type 4). 리스너도 지운다 */
  unsubscribeOrderBook(symbols: string[]): void {
    const codes = symbols.map(symbolCode).filter((c) => this.bookListeners.delete(c));
    if (codes.length > 0 && this.isSocketOpen) this.sendLater((t) => codes.flatMap((c) => LS_BOOK_TRS.map((tr) => this.message(t, LS_TR_TYPE_UNSUBSCRIBE, tr, c))));
  }

  /** 계좌 통보 해제 (tr_type 2). 리스너도 지운다 */
  unsubscribeOrderEvents(): void {
    if (this.orderListeners.length === 0) return;
    this.orderListeners.length = 0;
    if (this.isSocketOpen) this.sendLater((t) => LS_ORDER_TRS.map((tr) => this.message(t, LS_TR_TYPE_ACCOUNT_UNREGISTER, tr, "")));
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
      .catch((e) => console.error(`ERROR hermetix ls stream: 토큰 조회 실패 - ${e}`));
  }

  protected onMessage(raw: string): void {
    let node: Json;
    try { node = JSON.parse(raw); } catch { return; }
    const header = (node.header ?? {}) as Json;
    const trCd = text(header, "tr_cd");
    if (isControl(node)) {
      const rspCd = text(header, "rsp_cd");
      const msg = text(header, "rsp_msg");
      if (!rspCd || rspCd === LS_RSP_OK) console.log(`INFO hermetix ls stream: ack ${trCd} ${text(header, "tr_key")} tr_type=${text(header, "tr_type")} (${msg})`);
      else console.warn(`WARN hermetix ls stream: ${trCd} ${text(header, "tr_key")} rsp_cd=${rspCd} ${msg}`);
      return;
    }
    if (LS_TRADE_TRS.includes(trCd)) {
      const tick = parseLsTrade(node);
      if (tick) this.deliver(tick, this.tradeListeners, "리스너");
    } else if (LS_BOOK_TRS.includes(trCd)) {
      const book = parseLsOrderBook(node);
      if (book) this.deliver(book, this.bookListeners, "호가 리스너");
    } else if (LS_ORDER_TRS.includes(trCd)) {
      for (const event of parseLsOrderEvents(node)) {
        for (const l of this.orderListeners) {
          try { l(event); } catch (e) { console.error(`ERROR hermetix ls stream: 주문 통보 리스너 오류 / ${event.orderId}: ${e}`); }
        }
      }
    }
  }

  private deliver<T extends { symbol: string }>(tick: T, target: Map<string, ((t: T) => void)[]>, label: string): void {
    const code = tick.symbol;
    const symbol = this.requestedSymbols.get(code) ?? code;
    const normalized = symbol === code ? tick : { ...tick, symbol };
    for (const listener of target.get(code) ?? []) {
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix ls stream: ${label} 오류 / ${symbol}: ${e}`); }
    }
  }

  private message(token: string, trType: string, trCd: string, trKey: string): string {
    return JSON.stringify({ header: { token, tr_type: trType }, body: { tr_cd: trCd, tr_key: trKey } });
  }
}

/** 체결 프레임(S3_/K3_) → 체결. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다). TR 불일치·필수 필드 부족이면 null */
export function parseLsTrade(node: Json, today: string = kstToday()): TradeTick | null {
  if (isControl(node) || !LS_TRADE_TRS.includes(text((node.header ?? {}) as Json, "tr_cd"))) return null;
  const b = node.body as Json;
  try {
    const price = num(b, "price");
    if (!price) return null;
    const sign = text(b, "sign");
    let change = num(b, "change");
    if (change && FALLING_SIGNS.has(sign) && change.gt(0)) change = change.neg();
    const rate = num(b, "drate");
    const cumulative = num(b, "volume");
    return {
      symbol: marketCode(node),
      price,
      quantity: num(b, "cvolume") ?? new Decimal(0),
      timestamp: kstTime(text(b, "chetime"), today),
      askPrice: num(b, "offerho"),
      bidPrice: num(b, "bidho"),
      cumulativeVolume: cumulative ? cumulative.toNumber() : null,
      change,
      changeRate: rate ? rate.div(100) : null,
    };
  } catch { return null; }
}

/** 호가 프레임(H1_/HA_) → 호가창. 0 호가는 빈 단계로 보고 뺀다 */
export function parseLsOrderBook(node: Json, today: string = kstToday()): OrderBookTick | null {
  if (isControl(node) || !LS_BOOK_TRS.includes(text((node.header ?? {}) as Json, "tr_cd"))) return null;
  const b = node.body as Json;
  try {
    const levels = (pricePrefix: string, qtyPrefix: string): OrderBookLevel[] => {
      const out: OrderBookLevel[] = [];
      for (let i = 1; i <= 10; i++) {
        const price = num(b, `${pricePrefix}${i}`);
        if (!price || price.isZero()) continue;
        out.push({ price, quantity: num(b, `${qtyPrefix}${i}`) ?? new Decimal(0) });
      }
      return out;
    };
    return {
      symbol: marketCode(node),
      timestamp: kstTime(text(b, "hotime"), today),
      asks: levels("offerho", "offerrem"),
      bids: levels("bidho", "bidrem"),
      totalAskQuantity: num(b, "totofferrem"),
      totalBidQuantity: num(b, "totbidrem"),
    };
  } catch { return null; }
}

/** 주문 통보 프레임(SC0~SC4) → 이벤트 목록 (프레임당 1건, 파싱 실패면 빈 목록) */
export function parseLsOrderEvents(node: Json, today: string = kstToday()): OrderEvent[] {
  const trCd = text((node.header ?? {}) as Json, "tr_cd");
  if (isControl(node) || !LS_ORDER_TRS.includes(trCd)) return [];
  const b = node.body as Json;
  try {
    const orderId = text(b, "ordno");
    if (!orderId) return [];
    const originalRaw = text(b, "orgordno");
    const originalOrderId = originalRaw && originalRaw.replace(/^0+/, "") !== "" && originalRaw !== orderId ? originalRaw : null;
    const side: OrderSide | null = text(b, "bnstp") === "1" ? "SELL" : text(b, "bnstp") === "2" ? "BUY" : null;
    const symbol = normalizeCode(text(b, "shtnIsuno") || text(b, "shtcode")) || null;
    const byCode: Record<string, OrderEventType> = { "11": "FILLED", "12": "MODIFIED", "13": "CANCELED", "14": "REJECTED" };
    const byTr: Record<string, OrderEventType> = {
      [LS_TR_ORDER_FILLED]: "FILLED", [LS_TR_ORDER_MODIFIED]: "MODIFIED", [LS_TR_ORDER_CANCELED]: "CANCELED", [LS_TR_ORDER_REJECTED]: "REJECTED",
    };
    const type: OrderEventType = byCode[text(b, "ordxctptncode")] ?? byTr[trCd] ?? "ACCEPTED";
    const [quantity, price] = type === "ACCEPTED" ? [num(b, "ordqty"), num(b, "ordprice")]
      : type === "FILLED" ? [num(b, "execqty"), num(b, "execprc")]
      : type === "MODIFIED" ? [num(b, "mdfycnfqty"), num(b, "mdfycnfprc")]
      : type === "CANCELED" ? [num(b, "canccnfqty"), num(b, "ordprc")]
      : [num(b, "rjtqty"), num(b, "ordprc")];
    const time = trCd === LS_TR_ORDER_ACCEPTED ? text(b, "ordtm") : text(b, "exectime");
    return [{
      orderId, type, timestamp: kstTime(time, today), symbol, side, quantity, price,
      remainingQuantity: trCd === LS_TR_ORDER_ACCEPTED ? null : num(b, "unercqty"),
      originalOrderId,
      reason: type === "REJECTED" ? (text(b, "msgcode") || null) : null,
    }];
  } catch { return []; }
}
