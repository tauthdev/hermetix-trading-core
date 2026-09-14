/**
 * 키움 REST API 실시간 체결 스트림 (실시간 타입 0B 주식체결). 2026-09-14 모의투자 서버(mockapi) 장중 실측 통과.
 *
 * - 접속: 모의 wss://mockapi.kiwoom.com:10000/api/dostk/websocket, 실전 wss://api.kiwoom.com:10000/…
 * - 로그인: 접속 직후 {"trnm":"LOGIN","token":<접근토큰>} → {"trnm":"LOGIN","return_code":0,"sor_yn":"Y"}. REST 토큰을 그대로 쓴다
 * - 등록: {"trnm":"REG","grp_no":"1","refresh":"1","data":[{"item":[코드…],"type":["0B"]}]} → {"trnm":"REG","return_code":0}
 * - 데이터: {"data":[{"values":{...},"type":"0B","name":"주식체결","item":"000660"}],"trnm":"REAL"} (data 키가 trnm 앞에 온다)
 * - {"trnm":"PING"} 은 받은 그대로 되돌려 보낸다
 *
 * values 의 FID: 20 체결시각 HHMMSS, 10 현재가, 11 전일대비, 12 등락율(%), 27 최우선매도호가, 28 최우선매수호가,
 * 15 거래량(+매수/-매도 체결), 13 누적거래량. REST 와 같이 가격·호가·체결량에 등락 부호가 붙으므로 절대값으로 파싱한다.
 * 실측 프레임은 conformance/fixtures/kiwoom.json#stream
 */
import { Decimal } from "decimal.js";
import type { MarketStream, TradeListener } from "../broker.js";
import { kstToday } from "../broker.js";
import type { TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const KIWOOM_TYPE_TRADE = "0B";

export interface KiwoomStreamOptions {
  wsUrl: string;
  /** 접속마다 호출 — REST 접근토큰 (만료 시 갱신된다) */
  token: () => Promise<string>;
}

export class KiwoomMarketStream extends ReconnectingWebSocket implements MarketStream {
  private readonly listeners = new Map<string, TradeListener[]>();
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
    const newCodes: string[] = [];
    for (const symbol of symbols) {
      const code = symbolCode(symbol);
      if (!this.requestedSymbols.has(code)) this.requestedSymbols.set(code, symbol);
      let list = this.listeners.get(code);
      if (!list) { list = []; this.listeners.set(code, list); newCodes.push(code); }
      list.push(listener);
    }
    if (newCodes.length > 0 && this.isConnected) this.send(this.registerMessage(newCodes));
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
          if (this.listeners.size > 0) this.send(this.registerMessage([...this.listeners.keys()]));
        } else {
          console.error(`ERROR hermetix kiwoom stream: 로그인 실패 return_code=${code} ${node.return_msg ?? ""}`);
        }
        break;
      }
      case "REG": {
        const code = Number(node.return_code ?? -1);
        if (code === 0) console.log(`INFO hermetix kiwoom stream: registered ${[...this.listeners.keys()].join(",")}`);
        else console.warn(`WARN hermetix kiwoom stream: 등록 실패 return_code=${code} ${node.return_msg ?? ""}`);
        break;
      }
      case "REAL":
        for (const tick of parseKiwoomReal(node)) this.deliver(tick);
        break;
      default:
        break;
    }
  }

  private deliver(tick: TradeTick): void {
    const code = tick.symbol;
    const symbol = this.requestedSymbols.get(code) ?? code;
    const normalized = symbol === code ? tick : { ...tick, symbol };
    for (const listener of this.listeners.get(code) ?? []) {
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix kiwoom stream: 리스너 오류 / ${symbol}: ${e}`); }
    }
  }

  private registerMessage(codes: string[]): string {
    return JSON.stringify({ trnm: "REG", grp_no: "1", refresh: "1", data: [{ item: codes, type: [KIWOOM_TYPE_TRADE] }] });
  }
}

const signed = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/^\+/, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};

/** REAL 프레임 → 체결 목록 (0B 만). 심볼은 종목코드 그대로 (A 프리픽스 제거). today 는 KST 날짜 "YYYY-MM-DD" */
export function parseKiwoomReal(node: Record<string, unknown>, today: string = kstToday()): TradeTick[] {
  const items = (Array.isArray(node.data) ? node.data : []) as Record<string, unknown>[];
  const ticks: TradeTick[] = [];
  for (const item of items) {
    if (String(item.type ?? "") !== KIWOOM_TYPE_TRADE) continue;
    try {
      const v = (item.values ?? {}) as Record<string, unknown>;
      const price = signed(v["10"])?.abs();
      if (!price) continue;
      const rate = signed(v["12"]);
      const cum = signed(v["13"])?.abs();
      ticks.push({
        symbol: String(item.item ?? "").trim().replace(/^A/, ""),
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
