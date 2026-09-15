/**
 * KIS 실시간 스트림. 체결가 H0STCNT0·호가 H0STASP0 는 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과,
 * 주문 통보 H0STCNI9(모의)/H0STCNI0(실전)는 문서 기반 (Kotlin 레퍼런스와 동일 프로토콜).
 *
 * - 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
 * - 인증: REST POST /oauth2/Approval 의 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 새로 받는다)
 * - 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id","tr_key"}}}
 *   tr_key 는 시세 TR 이면 종목코드, 주문 통보 TR 이면 HTS ID
 * - 데이터 프레임: `0|TR|<건수>|<필드^필드^…>` — 첫 세그먼트 0 은 평문, 1 은 AES-256-CBC 암호문(base64, 구독 응답 output.key/iv).
 *   레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수, 체결가 실측 47·호가 63)
 * - 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd, 암호화 TR 이면 output.iv/key), PINGPONG(그대로 되돌려 보내야 연결 유지)
 *
 * 필드(0부터):
 * - H0STCNT0: 0 단축코드, 1 체결시각 HHMMSS, 2 현재가, 3 전일대비부호(4·5=하락), 4 전일대비, 5 전일대비율(%), 10 매도호가1, 11 매수호가1, 12 체결량, 13 누적거래량
 * - H0STASP0: 0 코드, 1 시각, 2 시간구분, 3–12 매도호가1–10, 13–22 매수호가1–10, 23–32 매도잔량, 33–42 매수잔량, 43 총매도잔량, 44 총매수잔량
 * - H0STCNI9/0: 0 고객ID, 1 계좌, 2 주문번호, 3 원주문번호, 4 매도매수(01 매도/02 매수), 5 정정취소(0/1 정정/2 취소), 8 종목, 9 체결량, 10 체결가,
 *   11 시각, 12 거부여부(0/1), 13 체결여부(1 접수·정정·취소·거부 / 2 체결), 16 주문수량, 25 주문가격
 * 실측 프레임은 conformance/fixtures/kis.json#stream
 */
import { createCipheriv, createDecipheriv } from "node:crypto";
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import { DorNull, kstToday } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import type { BrokerUsage } from "../telemetry.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const KIS_TR_TRADE = "H0STCNT0";
export const KIS_TR_ORDER_BOOK = "H0STASP0";
export const KIS_TR_ORDER_EVENTS_PAPER = "H0STCNI9";
export const KIS_TR_ORDER_EVENTS_LIVE = "H0STCNI0";
const MIN_TRADE_FIELDS = 14;
const MIN_BOOK_FIELDS = 45;
const MIN_ORDER_FIELDS = 17;

export interface KisStreamOptions {
  /** 사용량 텔레메트리 핸들 (어댑터가 넘긴다) */
  usage?: BrokerUsage;
  wsUrl: string;
  custtype: string;
  /** 접속마다 호출 — 새 approval_key */
  approvalKey: () => Promise<string>;
  /** HTS ID — 주문 통보(H0STCNI9/H0STCNI0) 구독 키. 비우면 subscribeOrderEvents 가 throw */
  htsId?: string;
  /** 실전이면 H0STCNI0, 아니면 H0STCNI9 */
  live?: boolean;
}

export class KisMarketStream extends ReconnectingWebSocket implements MarketStream {
  /** 종목코드 → 리스너 (구독 요청 표기는 requestedSymbols) */
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  private readonly requestedSymbols = new Map<string, string>();
  /** 암호화 TR 의 복호화 키 (구독 응답 output.key/iv) — TR 별 */
  private readonly cipherKeys = new Map<string, { key: string; iv: string }>();

  constructor(private readonly options: KisStreamOptions) {
    super("kis", 30_000, 90_000, 0, options.usage);
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  private get trOrderEvents(): string { return this.options.live ? KIS_TR_ORDER_EVENTS_LIVE : KIS_TR_ORDER_EVENTS_PAPER; }

  protected uri(): string { return this.options.wsUrl; }

  protected async onOpen(): Promise<void> {
    const key = await this.options.approvalKey();
    for (const code of this.tradeListeners.keys()) this.send(this.subscribeMessage(key, KIS_TR_TRADE, code));
    for (const code of this.bookListeners.keys()) this.send(this.subscribeMessage(key, KIS_TR_ORDER_BOOK, code));
    if (this.orderListeners.length > 0) this.send(this.subscribeMessage(key, this.trOrderEvents, this.options.htsId ?? ""));
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes = this.register(symbols, listener, this.tradeListeners);
    this.options.usage?.streamSubscribed("TRADES", newCodes.length);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendSubscriptions(KIS_TR_TRADE, newCodes);
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    const newCodes = this.register(symbols, listener, this.bookListeners);
    this.options.usage?.streamSubscribed("ORDER_BOOK", newCodes.length);
    if (newCodes.length > 0 && this.isSocketOpen) this.sendSubscriptions(KIS_TR_ORDER_BOOK, newCodes);
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    if (!this.options.htsId) throw new Error("KIS 주문 통보 구독에는 HTS ID 가 필요합니다 (KisClient htsId)");
    const first = this.orderListeners.length === 0;
    if (first) this.options.usage?.streamSubscribed("ORDER_EVENTS");
    this.orderListeners.push(listener);
    if (first && this.isSocketOpen) this.sendSubscriptions(this.trOrderEvents, [this.options.htsId]);
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

  private sendSubscriptions(trId: string, keys: string[]): void {
    this.options.approvalKey()
      .then((key) => { for (const k of keys) this.send(this.subscribeMessage(key, trId, k)); })
      .catch((e) => console.error(`ERROR hermetix kis stream: 접속키 발급 실패 - ${e}`));
  }

  protected onMessage(text: string): void {
    if (text.startsWith("0|") || text.startsWith("1|")) { this.onDataFrame(text); return; }
    let node: Record<string, unknown>;
    try { node = JSON.parse(text); } catch { return; }
    const header = (node.header ?? {}) as Record<string, unknown>;
    const trId = String(header.tr_id ?? "");
    if (trId === "PINGPONG") { this.send(text); return; }
    const body = node.body as Record<string, unknown> | undefined;
    if (!body) return;
    const rtCd = String(body.rt_cd ?? "");
    const msgCd = String(body.msg_cd ?? "");
    const msg = String(body.msg1 ?? "");
    const trKey = String(header.tr_key ?? "");
    const output = (body.output ?? {}) as Record<string, unknown>;
    if (typeof output.key === "string" && typeof output.iv === "string") this.cipherKeys.set(trId, { key: output.key, iv: output.iv });
    const shown = trId.startsWith("H0STCNI") ? "(hts)" : trKey;
    if (rtCd === "0" || msgCd === "OPSP0000") console.log(`INFO hermetix kis stream: subscribed ${trId} ${shown} (${msg})`);
    else if (msgCd === "OPSP0002") console.log(`INFO hermetix kis stream: already subscribed ${trId}`);
    else console.warn(`WARN hermetix kis stream: ${trId} ${trKey} rt_cd=${rtCd} msg_cd=${msgCd} ${msg}`);
  }

  private onDataFrame(text: string): void {
    const parts = splitFrame(text);
    if (!parts) return;
    const [encrypted, trId, count, rawBody] = parts;
    let body = rawBody;
    if (encrypted === "1") {
      const keys = this.cipherKeys.get(trId);
      if (!keys) { console.warn(`WARN hermetix kis stream: 암호화 프레임(${trId})인데 복호화 키가 없다 — 구독 응답 전 프레임?`); return; }
      try { body = kisDecrypt(rawBody, keys.key, keys.iv); } catch (e) { console.warn(`WARN hermetix kis stream: 복호화 실패(${trId}) - ${e}`); return; }
    }
    const plain = `0|${trId}|${count}|${body}`;
    switch (trId) {
      case KIS_TR_TRADE:
        for (const tick of parseKisFrame(plain)) this.deliver(tick, this.tradeListeners, "리스너");
        break;
      case KIS_TR_ORDER_BOOK:
        for (const tick of parseKisOrderBook(plain)) this.deliver(tick, this.bookListeners, "호가 리스너");
        break;
      case KIS_TR_ORDER_EVENTS_PAPER:
      case KIS_TR_ORDER_EVENTS_LIVE:
        for (const event of parseKisOrderEvents(plain)) {
          this.options.usage?.streamMessage("ORDER_EVENTS");
          for (const l of this.orderListeners) {
            try { l(event); } catch (e) { console.error(`ERROR hermetix kis stream: 주문 통보 리스너 오류 / ${event.orderId}: ${e}`); }
          }
        }
        break;
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
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix kis stream: ${label} 오류 / ${symbol}: ${e}`); }
    }
  }

  private subscribeMessage(key: string, trId: string, trKey: string, trType = "1"): string {
    return JSON.stringify({
      header: { approval_key: key, custtype: this.options.custtype, tr_type: trType, "content-type": "utf-8" },
      body: { input: { tr_id: trId, tr_key: trKey } },
    });
  }
}

/** `0|TR|건수|본문` → [암호화 플래그, TR, 건수, 본문]. 형식이 아니면 null */
function splitFrame(frame: string): [string, string, string, string] | null {
  const parts = frame.split("|");
  if (parts.length < 4) return null;
  return [parts[0], parts[1], parts[2], parts.slice(3).join("|")];
}

/** `0|TR|건수|본문` 을 레코드(필드 목록)로 나눈다. TR 불일치·필드 부족이면 빈 목록 */
function records(frame: string, trId: string | string[], minFields: number): string[][] {
  const parts = splitFrame(frame);
  if (!parts) return [];
  const accepted = Array.isArray(trId) ? trId : [trId];
  if (!accepted.includes(parts[1])) return [];
  const count = Math.max(1, Number.parseInt(parts[2], 10) || 1);
  const fields = parts[3].split("^");
  const width = Math.floor(fields.length / count);
  if (width < minFields) return [];
  const out: string[][] = [];
  for (let i = 0; i < count; i++) out.push(fields.slice(i * width, (i + 1) * width));
  return out;
}

/**
 * 체결가 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다).
 * 알 수 없는 TR·필드 부족은 빈 목록. today 는 KST 날짜 "YYYY-MM-DD"
 */
export function parseKisFrame(frame: string, today: string = kstToday()): TradeTick[] {
  const ticks: TradeTick[] = [];
  for (const f of records(frame, KIS_TR_TRADE, MIN_TRADE_FIELDS)) {
    try {
      ticks.push(parseRecord(f, today));
    } catch { /* 레코드 하나가 깨져도 나머지는 전달 */ }
  }
  return ticks;
}

function parseRecord(f: string[], today: string): TradeTick {
  const sign = f[3];
  let change = DorNull(f[4]);
  // 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다 (실측은 부호 포함)
  if (change && (sign === "4" || sign === "5") && change.gt(0)) change = change.neg();
  const rate = DorNull(f[5]);
  return {
    symbol: f[0],
    price: new Decimal(f[2]),
    quantity: new Decimal(f[12]),
    timestamp: kstDateTime(today, f[1]),
    askPrice: DorNull(f[10]),
    bidPrice: DorNull(f[11]),
    cumulativeVolume: f[13] ? Number.parseInt(f[13], 10) : null,
    change,
    changeRate: rate ? rate.div(100) : null,
  };
}

/** 호가 프레임(H0STASP0) → 호가창 목록. 0 호가는 빈 단계로 보고 뺀다 */
export function parseKisOrderBook(frame: string, today: string = kstToday()): OrderBookTick[] {
  const books: OrderBookTick[] = [];
  for (const f of records(frame, KIS_TR_ORDER_BOOK, MIN_BOOK_FIELDS)) {
    try {
      const levels = (priceFrom: number, qtyFrom: number): OrderBookLevel[] => {
        const out: OrderBookLevel[] = [];
        for (let i = 0; i < 10; i++) {
          const price = DorNull(f[priceFrom + i]);
          if (!price || price.isZero()) continue;
          out.push({ price, quantity: DorNull(f[qtyFrom + i]) ?? new Decimal(0) });
        }
        return out;
      };
      books.push({
        symbol: f[0],
        timestamp: kstDateTime(today, f[1]),
        asks: levels(3, 23),
        bids: levels(13, 33),
        totalAskQuantity: DorNull(f[43]),
        totalBidQuantity: DorNull(f[44]),
      });
    } catch { /* 레코드 하나가 깨져도 나머지는 전달 */ }
  }
  return books;
}

/** 주문 통보 프레임(복호화된 평문 H0STCNI9/H0STCNI0) → 이벤트 목록 */
export function parseKisOrderEvents(frame: string, today: string = kstToday()): OrderEvent[] {
  const events: OrderEvent[] = [];
  for (const f of records(frame, [KIS_TR_ORDER_EVENTS_PAPER, KIS_TR_ORDER_EVENTS_LIVE], MIN_ORDER_FIELDS)) {
    try {
      const rejected = f[12] === "1";
      const filled = f[13] === "2";
      const amendKind = f[5]; // 0 정상, 1 정정, 2 취소
      let type: OrderEventType;
      if (rejected) type = "REJECTED";
      else if (filled) type = "FILLED";
      else if (amendKind === "2") type = "CANCELED";
      else if (amendKind === "1") type = "MODIFIED";
      else type = "ACCEPTED";
      const orderId = f[2].trim();
      const original = f[3]?.trim() ?? "";
      const side: OrderSide | null = f[4] === "01" ? "SELL" : f[4] === "02" ? "BUY" : null;
      events.push({
        orderId,
        type,
        timestamp: kstDateTime(today, f[11].trim()),
        symbol: f[8].trim(),
        side,
        quantity: filled ? DorNull(f[9]) : DorNull(f[16]),
        price: filled ? DorNull(f[10]) : DorNull(f[25]),
        remainingQuantity: null,
        originalOrderId: original && original.replace(/^0+/, "") !== "" && original !== orderId ? original : null,
        reason: null,
      });
    } catch { /* 레코드 하나가 깨져도 나머지는 전달 */ }
  }
  return events;
}

/** KIS 암호화 본문: AES-256-CBC, PKCS7, base64. key 32자·iv 16자는 구독 응답 output 에서 온다 */
export function kisDecrypt(base64: string, key: string, iv: string): string {
  const decipher = createDecipheriv("aes-256-cbc", Buffer.from(key, "utf8"), Buffer.from(iv, "utf8"));
  return Buffer.concat([decipher.update(Buffer.from(base64, "base64")), decipher.final()]).toString("utf8");
}

/** 테스트·픽스처 생성용 — kisDecrypt 의 역 */
export function kisEncrypt(plain: string, key: string, iv: string): string {
  const cipher = createCipheriv("aes-256-cbc", Buffer.from(key, "utf8"), Buffer.from(iv, "utf8"));
  return Buffer.concat([cipher.update(Buffer.from(plain, "utf8")), cipher.final()]).toString("base64");
}
