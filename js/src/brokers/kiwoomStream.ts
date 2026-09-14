/**
 * 키움 REST API 실시간 스트림. 체결 0B·호가 0D 는 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과(00 등록도 return_code 0),
 * 주문체결 00 프레임은 문서 기반 (Kotlin 레퍼런스와 동일 프로토콜).
 *
 * - 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/…
 * - 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} → {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
 * - 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]} → {"trnm":"REG","return_code":0}.
 *   주문체결(00)은 계좌 단위라 item 을 빈 문자열 하나로 등록한다
 * - 데이터: {"data":[{"values":{...},"type":"0B","name":"주식체결","item":"000660"}],"trnm":"REAL"} (data 키가 trnm 앞에 온다)
 * - {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다
 *
 * FID:
 * - 0B 주식체결: 20 체결시각, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가, 15 거래량(+매수/-매도), 13 누적거래량
 * - 0D 주식호가잔량: 21 시각, 41–50 매도호가1–10, 61–70 매도잔량, 51–60 매수호가1–10, 71–80 매수잔량, 121 총매도잔량, 125 총매수잔량
 * - 00 주문체결: 9203 주문번호, 904 원주문번호, 9001 종목(A접두), 913 상태(접수/체결/확인), 905 주문구분(+매수/매수취소…), 907 매도수(1 매도/2 매수),
 *   900 주문수량, 901 주문가격, 902 미체결, 910 체결가, 911 체결량, 908 시각, 919 거부사유
 * REST 와 같이 가격·호가·수량에 등락 부호가 붙으므로 절대값으로 파싱한다. 실측 프레임은 conformance/fixtures/kiwoom.json#stream
 */
import { Decimal } from "decimal.js";
import type { MarketStream, OrderBookListener, OrderEventListener, TradeListener } from "../broker.js";
import { kstToday } from "../broker.js";
import type { OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const KIWOOM_TYPE_TRADE = "0B";
export const KIWOOM_TYPE_ORDER_BOOK = "0D";
export const KIWOOM_TYPE_ORDER_EVENTS = "00";

export interface KiwoomStreamOptions {
  wsUrl: string;
  /** 접속마다 호출 — REST 접근토큰 (만료 시 갱신된다) */
  token: () => Promise<string>;
}

export class KiwoomMarketStream extends ReconnectingWebSocket implements MarketStream {
  private readonly tradeListeners = new Map<string, TradeListener[]>();
  private readonly bookListeners = new Map<string, OrderBookListener[]>();
  private readonly orderListeners: OrderEventListener[] = [];
  private readonly requestedSymbols = new Map<string, string>();
  private loggedIn = false;

  constructor(private readonly options: KiwoomStreamOptions) {
    super("kiwoom");
  }

  get isConnected(): boolean { return this.isSocketOpen && this.loggedIn; }

  protected uri(): string { return this.options.wsUrl; }

  protected async onOpen(): Promise<void> {
    this.loggedIn = false;
    const token = await this.options.token();
    this.send(JSON.stringify({ trnm: "LOGIN", token }));
  }

  protected onDisconnected(): void { this.loggedIn = false; }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes = this.register(symbols, listener, this.tradeListeners);
    if (newCodes.length > 0 && this.isConnected) this.send(this.registerMessage(newCodes, KIWOOM_TYPE_TRADE));
  }

  subscribeOrderBook(symbols: string[], listener: OrderBookListener): void {
    const newCodes = this.register(symbols, listener, this.bookListeners);
    if (newCodes.length > 0 && this.isConnected) this.send(this.registerMessage(newCodes, KIWOOM_TYPE_ORDER_BOOK));
  }

  subscribeOrderEvents(listener: OrderEventListener): void {
    const first = this.orderListeners.length === 0;
    this.orderListeners.push(listener);
    if (first && this.isConnected) this.send(this.registerMessage([""], KIWOOM_TYPE_ORDER_EVENTS));
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

  private registerAll(): void {
    if (this.tradeListeners.size > 0) this.send(this.registerMessage([...this.tradeListeners.keys()], KIWOOM_TYPE_TRADE));
    if (this.bookListeners.size > 0) this.send(this.registerMessage([...this.bookListeners.keys()], KIWOOM_TYPE_ORDER_BOOK));
    if (this.orderListeners.length > 0) this.send(this.registerMessage([""], KIWOOM_TYPE_ORDER_EVENTS));
  }

  protected onMessage(text: string): void {
    let node: Record<string, unknown>;
    try { node = JSON.parse(text); } catch { return; }
    switch (String(node.trnm ?? "")) {
      case "PING":
        this.send(text);
        break;
      case "LOGIN": {
        const code = Number(node.return_code ?? -1);
        if (code === 0) {
          this.loggedIn = true;
          console.log("INFO hermetix kiwoom stream: logged in");
          this.registerAll();
        } else {
          console.error(`ERROR hermetix kiwoom stream: 로그인 실패 return_code=${code} ${node.return_msg ?? ""}`);
        }
        break;
      }
      case "REG": {
        const code = Number(node.return_code ?? -1);
        if (code === 0) console.log("INFO hermetix kiwoom stream: registered");
        else console.warn(`WARN hermetix kiwoom stream: 등록 실패 return_code=${code} ${node.return_msg ?? ""}`);
        break;
      }
      case "REAL":
        for (const tick of parseKiwoomReal(node)) this.deliver(tick, this.tradeListeners, "리스너");
        for (const tick of parseKiwoomOrderBook(node)) this.deliver(tick, this.bookListeners, "호가 리스너");
        for (const event of parseKiwoomOrderEvents(node)) {
          for (const l of this.orderListeners) {
            try { l(event); } catch (e) { console.error(`ERROR hermetix kiwoom stream: 주문 통보 리스너 오류 / ${event.orderId}: ${e}`); }
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
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix kiwoom stream: ${label} 오류 / ${symbol}: ${e}`); }
    }
  }

  private registerMessage(items: string[], type: string): string {
    return JSON.stringify({ trnm: "REG", grp_no: "1", refresh: "1", data: [{ item: items, type: [type] }] });
  }
}

const signed = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/^\+/, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};

const itemsOf = (node: Record<string, unknown>, type: string): Record<string, unknown>[] =>
  ((Array.isArray(node.data) ? node.data : []) as Record<string, unknown>[]).filter((item) => String(item.type ?? "") === type);

const codeOf = (item: Record<string, unknown>): string => String(item.item ?? "").trim().replace(/^A/, "");

/** REAL 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (A 프리픽스 제거). today 는 KST 날짜 "YYYY-MM-DD" */
export function parseKiwoomReal(node: Record<string, unknown>, today: string = kstToday()): TradeTick[] {
  const ticks: TradeTick[] = [];
  for (const item of itemsOf(node, KIWOOM_TYPE_TRADE)) {
    try {
      const v = (item.values ?? {}) as Record<string, unknown>;
      const price = signed(v["10"])?.abs();
      if (!price) continue;
      const rate = signed(v["12"]);
      const cum = signed(v["13"])?.abs();
      ticks.push({
        symbol: codeOf(item),
        price,
        quantity: signed(v["15"])?.abs() ?? new Decimal(0),
        timestamp: kstDateTime(today, String(v["20"] ?? "")),
        askPrice: signed(v["27"])?.abs() ?? null,
        bidPrice: signed(v["28"])?.abs() ?? null,
        cumulativeVolume: cum ? Number(cum) : null,
        change: signed(v["11"]),
        changeRate: rate ? rate.div(100) : null,
      });
    } catch { /* 항목 하나가 깨져도 나머지는 전달 */ }
  }
  return ticks;
}

/** REAL 프레임 → 호가창 목록 (0D 만). 0 호가는 빈 단계로 보고 뺀다 */
export function parseKiwoomOrderBook(node: Record<string, unknown>, today: string = kstToday()): OrderBookTick[] {
  const books: OrderBookTick[] = [];
  for (const item of itemsOf(node, KIWOOM_TYPE_ORDER_BOOK)) {
    try {
      const v = (item.values ?? {}) as Record<string, unknown>;
      const levels = (priceFrom: number, qtyFrom: number): OrderBookLevel[] => {
        const out: OrderBookLevel[] = [];
        for (let i = 0; i < 10; i++) {
          const price = signed(v[String(priceFrom + i)])?.abs();
          if (!price || price.isZero()) continue;
          out.push({ price, quantity: signed(v[String(qtyFrom + i)])?.abs() ?? new Decimal(0) });
        }
        return out;
      };
      books.push({
        symbol: codeOf(item),
        timestamp: kstDateTime(today, String(v["21"] ?? "")),
        asks: levels(41, 61),
        bids: levels(51, 71),
        totalAskQuantity: signed(v["121"])?.abs() ?? null,
        totalBidQuantity: signed(v["125"])?.abs() ?? null,
      });
    } catch { /* 항목 하나가 깨져도 나머지는 전달 */ }
  }
  return books;
}

/** REAL 프레임 → 주문 통보 목록 (00 만) */
export function parseKiwoomOrderEvents(node: Record<string, unknown>, today: string = kstToday()): OrderEvent[] {
  const events: OrderEvent[] = [];
  for (const item of itemsOf(node, KIWOOM_TYPE_ORDER_EVENTS)) {
    try {
      const v = (item.values ?? {}) as Record<string, unknown>;
      const status = String(v["913"] ?? "").trim();
      const kind = String(v["905"] ?? "").trim();
      const reasonText = String(v["919"] ?? "").trim();
      const reason = reasonText === "" ? null : reasonText;
      const filledQty = signed(v["911"])?.abs() ?? null;
      let type: OrderEventType;
      if (reason !== null) type = "REJECTED";
      else if (status.includes("체결") && filledQty && filledQty.gt(0)) type = "FILLED";
      else if (kind.includes("취소")) type = "CANCELED";
      else if (kind.includes("정정")) type = "MODIFIED";
      else type = "ACCEPTED";
      const original = String(v["904"] ?? "").trim();
      const symbol = String(v["9001"] ?? "").trim().replace(/^A/, "");
      const sideCode = String(v["907"] ?? "").trim();
      const side: OrderSide | null = sideCode === "1" ? "SELL" : sideCode === "2" ? "BUY" : null;
      events.push({
        orderId: String(v["9203"] ?? "").trim(),
        type,
        timestamp: kstDateTime(today, String(v["908"] ?? "0")),
        symbol: symbol === "" ? null : symbol,
        side,
        quantity: type === "FILLED" ? filledQty : signed(v["900"])?.abs() ?? null,
        price: type === "FILLED" ? signed(v["910"])?.abs() ?? null : signed(v["901"])?.abs() ?? null,
        remainingQuantity: signed(v["902"])?.abs() ?? null,
        originalOrderId: original !== "" && original.replace(/^0+/, "") !== "" ? original : null,
        reason,
      });
    } catch { /* 항목 하나가 깨져도 나머지는 전달 */ }
  }
  return events;
}
