/**
 * DB증권 실시간 스트림 — 체결 S00, 호가 S01, 주문 접수 IS0, 주문 체결 IS1. ⚠️ 문서 기반 구현, 실측 전 (Kotlin DbMarketStream 과 동일 프로토콜).
 *
 * - 접속: 운영 wss://openapi.dbsec.co.kr:7070/websocket, 모의 :17070/websocket. 핸드셰이크 헤더 없음
 * - 인증: 로그인 프레임 없이 매 메시지 header.token 에 REST 접근토큰(Bearer 접두 없음)
 * - 시세 등록 {"header":{"token","tr_type":"1"},"body":{"tr_cd":"S00","tr_key":"J 005930"}}, 해제는 tr_type "2". tr_key = 시장구분 2자리("J"+공백) + 종목코드
 * - 계좌 등록 {"header":{"token","tr_type":"3"},"body":{"tr_cd":"IS0"}} — tr_key 없음, 해제 메시지 없음(세션 종료가 해제)
 * - 접속 후 10초 안에 첫 메시지를 보내야 한다 → onOpen 에서 즉시 전송. 서버 주기 프레임이 없어 유휴 감시는 끈다
 * - 응답: 구독 ack {"header":{"tr_cd":"S00","tr_key":null},"body":{"tr_key":["J 005930"]}}, 제어 프레임은 header/body 가 null 이거나
 *   rsp_cd/rsp_msg 를 어느 쪽에든 싣는다(""/"0"/"00000" 정상). 데이터는 header.tr_cd 로 라우팅, header.tr_key 는 null 이라 심볼은 body(ShrnIscd/Sshtnisuno)
 * - body 값은 모두 문자열(계좌계는 0 패딩). 필드명 대소문자가 문서와 예시에서 다르다(askp1 vs Askp1) → 대소문자 무시 조회
 *
 * Sordxctptncode(주문체결유형코드) 값표는 미공개라 상태는 수량 필드로 판정한다. 픽스처: conformance/fixtures/db.json#stream
 */
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import { kstToday } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import type { BrokerUsage } from "../telemetry.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const DB_TR_TRADE = "S00";
export const DB_TR_ORDER_BOOK = "S01";
export const DB_TR_ORDER_ACCEPTED = "IS0";
export const DB_TR_ORDER_EXECUTED = "IS1";
/** KRX 주식/ETF 시장구분 2자리 — J + 공백. NXT 는 NJN-, 통합은 UJU- (미지원) */
export const DB_MARKET_PREFIX = "J ";
const SUCCESS_CODES = new Set(["", "0", "00000"]);

export interface DbStreamOptions {
  /** 사용량 텔레메트리 핸들 (어댑터가 넘긴다) */
  usage?: BrokerUsage;
  wsUrl: string;
  /** 접속·구독마다 호출 — REST 접근토큰 */
  token: () => Promise<string>;
}

type Json = Record<string, unknown>;
type Lower = Map<string, unknown>;

/** 문서 표(askp1)와 예시(Askp1)의 대소문자가 달라 소문자 키로 조회한다 */
const lower = (body: Json): Lower => new Map(Object.entries(body).map(([k, v]) => [k.toLowerCase(), v]));
const text = (f: Lower, field: string): string => String(f.get(field.toLowerCase()) ?? "").trim();
const num = (f: Lower, field: string): Decimal | null => {
  const t = text(f, field).replace(/,/g, "");
  if (!t) return null;
  try { return new Decimal(t); } catch { return null; }
};
/** HHmmss 또는 HHmmssSSS → 오늘 KST */
const kstTime = (raw: string, today: string): Date => kstDateTime(today, raw.slice(0, 6).padStart(6, "0"));

/** U-005930 / N-005930 / A005930 → 005930 */
export const dbStreamNormalizeCode = (raw: string): string => raw.trim().replace(/^U-/, "").replace(/^N-/, "").replace(/^A/, "");

export class DbMarketStream extends ReconnectingWebSocket implements MarketStream {
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  private readonly requestedSymbols = new Map<string, string>();

  constructor(private readonly options: DbStreamOptions) {
    super("db", 30_000, 0, 0, options.usage);
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  protected uri(): string { return this.options.wsUrl; }

  /** 접속 직후 전부 다시 보낸다 — 10초 규칙 때문에 지체하지 않는다 */
  protected async onOpen(): Promise<void> {
    const t = await this.options.token();
    for (const code of this.tradeListeners.keys()) this.send(this.quoteMessage(t, DB_TR_TRADE, code, "1"));
    for (const code of this.bookListeners.keys()) this.send(this.quoteMessage(t, DB_TR_ORDER_BOOK, code, "1"));
    if (this.orderListeners.length > 0) this.sendAccountRegistrations(t);
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes = this.register(symbols, listener, this.tradeListeners);
    this.options.usage?.streamSubscribed("TRADES", newCodes.length);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.map((c) => this.quoteMessage(t, DB_TR_TRADE, c, "1")));
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    const newCodes = this.register(symbols, listener, this.bookListeners);
    this.options.usage?.streamSubscribed("ORDER_BOOK", newCodes.length);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendLater((t) => newCodes.map((c) => this.quoteMessage(t, DB_TR_ORDER_BOOK, c, "1")));
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    const first = this.orderListeners.length === 0;
    if (first) this.options.usage?.streamSubscribed("ORDER_EVENTS");
    this.orderListeners.push(listener);
    if (first && this.isSocketOpen) this.sendLater((t) => [this.accountMessage(t, DB_TR_ORDER_ACCEPTED), this.accountMessage(t, DB_TR_ORDER_EXECUTED)]);
  }

  private sendAccountRegistrations(t: string): void {
    this.send(this.accountMessage(t, DB_TR_ORDER_ACCEPTED));
    this.send(this.accountMessage(t, DB_TR_ORDER_EXECUTED));
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
      .catch((e) => console.error(`ERROR hermetix db stream: 토큰 조회 실패 - ${e}`));
  }

  protected onMessage(raw: string): void {
    let node: Json;
    try { node = JSON.parse(raw); } catch { return; }
    const header = (node.header ?? null) as Json | null;
    const body = (node.body ?? null) as Json | null;
    // 제어 프레임: rsp_cd/rsp_msg 가 header 또는 body 에 실린다. header/body 둘 다 비면 keepalive 류 — 무시
    const rspCd = String(header?.rsp_cd ?? body?.rsp_cd ?? "");
    const rspMsg = String(header?.rsp_msg ?? body?.rsp_msg ?? "");
    if (rspCd || rspMsg) {
      if (SUCCESS_CODES.has(rspCd)) console.log(`INFO hermetix db stream: ${String(header?.tr_cd ?? "")} ${rspMsg}`);
      else console.warn(`WARN hermetix db stream: ${String(header?.tr_cd ?? "")} rsp_cd=${rspCd} ${rspMsg}`);
      return;
    }
    if (!header || !body || typeof header !== "object" || typeof body !== "object") return;
    const trCd = String(header.tr_cd ?? "");
    if (Array.isArray(body.tr_key)) { // 구독 ack
      console.log(`INFO hermetix db stream: subscribed ${trCd} ${JSON.stringify(body.tr_key)}`);
      return;
    }
    switch (trCd) {
      case DB_TR_TRADE: {
        const tick = parseDbTrade(body);
        if (tick) this.deliver(tick, this.tradeListeners, "리스너");
        break;
      }
      case DB_TR_ORDER_BOOK: {
        const book = parseDbOrderBook(body);
        if (book) this.deliver(book, this.bookListeners, "호가 리스너");
        break;
      }
      case DB_TR_ORDER_ACCEPTED: case DB_TR_ORDER_EXECUTED: {
        const event = parseDbOrderEvent(trCd, body);
        if (!event) break;
        this.options.usage?.streamMessage("ORDER_EVENTS");
        for (const l of this.orderListeners) {
          try { l(event); } catch (e) { console.error(`ERROR hermetix db stream: 주문 통보 리스너 오류 / ${event.orderId}: ${e}`); }
        }
        break;
      }
      default:
        break;
    }
  }

  private deliver<T extends { symbol: string }>(tick: T, target: Map<string, ((t: T) => void)[]>, label: string): void {
    this.options.usage?.streamMessage((target as unknown) === (this.tradeListeners as unknown) ? "TRADES" : "ORDER_BOOK");
    const code = tick.symbol;
    const symbol = this.requestedSymbols.get(code) ?? code;
    const normalized = symbol === code ? tick : { ...tick, symbol };
    for (const listener of target.get(code) ?? []) {
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix db stream: ${label} 오류 / ${symbol}: ${e}`); }
    }
  }

  private quoteMessage(t: string, trCd: string, code: string, trType: string): string {
    return JSON.stringify({ header: { token: t, tr_type: trType }, body: { tr_cd: trCd, tr_key: DB_MARKET_PREFIX + code } });
  }

  private accountMessage(t: string, trCd: string): string {
    return JSON.stringify({ header: { token: t, tr_type: "3" }, body: { tr_cd: trCd } });
  }
}

/** S00 body → 체결. 심볼은 단축코드(요청 표기 복원은 스트림이 한다). 필드 부족이면 null */
export function parseDbTrade(body: Json, today: string = kstToday()): TradeTick | null {
  try {
    const f = lower(body);
    const price = num(f, "StckPrpr");
    if (!price) return null;
    const falling = text(f, "PrdyVrssclr") === "-" || ["4", "5"].includes(text(f, "PrdyVrsssign"));
    let change = num(f, "PrdyVrss");
    if (change && falling && change.gt(0)) change = change.neg();
    const rate = num(f, "PrdyCtrt");
    const cumulative = num(f, "AcmlVol");
    return {
      symbol: dbStreamNormalizeCode(text(f, "ShrnIscd")),
      price,
      quantity: num(f, "CntgVol") ?? new Decimal(0),
      timestamp: kstTime(text(f, "StckCntghour"), today),
      askPrice: num(f, "askp1"),
      bidPrice: num(f, "bidp1"),
      cumulativeVolume: cumulative ? cumulative.toNumber() : null,
      change,
      changeRate: rate ? rate.div(100) : null,
    };
  } catch { return null; }
}

/** S01 body → 호가창(10단계, 0 호가는 제외) */
export function parseDbOrderBook(body: Json, today: string = kstToday()): OrderBookTick | null {
  try {
    const f = lower(body);
    const levels = (priceField: string, qtyField: string): OrderBookLevel[] => {
      const out: OrderBookLevel[] = [];
      for (let i = 1; i <= 10; i++) {
        const price = num(f, `${priceField}${i}`);
        if (!price || price.isZero()) continue;
        out.push({ price, quantity: num(f, `${qtyField}${i}`) ?? new Decimal(0) });
      }
      return out;
    };
    return {
      symbol: dbStreamNormalizeCode(text(f, "ShrnIscd")),
      timestamp: kstTime(text(f, "BsopHour"), today),
      asks: levels("askp", "AskpRsqn"),
      bids: levels("bidp", "BidpRsqn"),
      totalAskQuantity: num(f, "TotalAskprsqn"),
      totalBidQuantity: num(f, "TotalBidprsqn"),
    };
  } catch { return null; }
}

/**
 * IS0(접수) / IS1(체결·정정·취소·거부) body → 주문 통보. 유형코드 값표가 없어 수량 필드로 판정한다:
 * 거부수량 > 0 → REJECTED, 체결수량 > 0 → FILLED, 취소확인수량 > 0 → CANCELED, 정정확인수량 > 0 → MODIFIED, 그 외 ACCEPTED
 */
export function parseDbOrderEvent(trCd: string, body: Json, today: string = kstToday()): OrderEvent | null {
  try {
    const f = lower(body);
    const orderId = text(f, "Sordno");
    if (!orderId) return null;
    const originalRaw = text(f, "Sorgordno");
    const originalOrderId = originalRaw && originalRaw.replace(/^0+/, "") !== "" && originalRaw.replace(/^0+/, "") !== orderId.replace(/^0+/, "") ? originalRaw : null;
    const side: OrderSide | null = text(f, "Sbnstp") === "1" ? "SELL" : text(f, "Sbnstp") === "2" ? "BUY" : null;
    const symbol = dbStreamNormalizeCode(text(f, "Sshtnisuno")) || null;
    if (trCd === DB_TR_ORDER_ACCEPTED) {
      return {
        orderId, type: "ACCEPTED", timestamp: kstTime(text(f, "Sordtm"), today), symbol, side,
        quantity: num(f, "Sordqty"), price: num(f, "Sordprc"), remainingQuantity: null, originalOrderId, reason: null,
      };
    }
    const positive = (field: string) => (num(f, field)?.gt(0) ?? false);
    let type: OrderEventType;
    if (positive("Srjtqty")) type = "REJECTED";
    else if (positive("Sexecqty")) type = "FILLED";
    else if (positive("Scanccnfqty")) type = "CANCELED";
    else if (positive("Smdfycnfqty")) type = "MODIFIED";
    else type = "ACCEPTED";
    const [quantity, price] = type === "FILLED" ? [num(f, "Sexecqty"), num(f, "Sexecprc")]
      : type === "MODIFIED" ? [num(f, "Smdfycnfqty"), num(f, "Smdfycnfprc")]
      : type === "CANCELED" ? [num(f, "Scanccnfqty"), num(f, "Sordprc")]
      : type === "REJECTED" ? [num(f, "Srjtqty"), num(f, "Sordprc")]
      : [num(f, "Sordqty"), num(f, "Sordprc")];
    return {
      orderId, type, timestamp: kstTime(text(f, "Sexectime") || text(f, "Sordtm"), today), symbol, side,
      quantity, price, remainingQuantity: num(f, "Sunercqty"), originalOrderId, reason: null,
    };
  } catch { return null; }
}
