/**
 * KIS 실시간 체결가 스트림 (TR H0STCNT0). 2026-09-14 모의투자 서버(ops…:31000) 장중 실측 통과 (Kotlin 레퍼런스와 동일 프로토콜).
 *
 * - 접속: 모의 ws://ops.koreainvestment.com:31000, 실전 :21000. TLS 없음
 * - 인증: REST POST /oauth2/Approval 의 approval_key 를 구독 메시지 헤더에 싣는다 (매 접속마다 새로 받는다)
 * - 구독: {"header":{"approval_key","custtype","tr_type":"1","content-type":"utf-8"},"body":{"input":{"tr_id":"H0STCNT0","tr_key":"005930"}}}
 * - 데이터 프레임: `0|H0STCNT0|<건수>|<필드^필드^…>` — 레코드가 여러 건이면 본문에 이어 붙는다 (필드 폭 = 전체 필드 수 / 건수, 실측 47)
 * - 제어 프레임(JSON): 구독 결과(body.rt_cd/msg_cd), PINGPONG(그대로 되돌려 보내야 연결 유지)
 *
 * 필드(0부터): 0 단축코드, 1 체결시각 HHMMSS, 2 현재가, 3 전일대비부호(4·5=하락), 4 전일대비, 5 전일대비율(%),
 * 10 매도호가1, 11 매수호가1, 12 체결거래량, 13 누적거래량. 실측 프레임은 conformance/fixtures/kis.json#stream
 */
import { Decimal } from "decimal.js";
import type { MarketStream, TradeListener } from "../broker.js";
import { DorNull, kstToday } from "../broker.js";
import type { TradeTick } from "../models.js";
import { symbolCode } from "../models.js";
import { ReconnectingWebSocket, kstDateTime } from "../stream.js";

export const KIS_TR_TRADE = "H0STCNT0";
const MIN_FIELDS = 14;

export interface KisStreamOptions {
  wsUrl: string;
  custtype: string;
  /** 접속마다 호출 — 새 approval_key */
  approvalKey: () => Promise<string>;
}

export class KisMarketStream extends ReconnectingWebSocket implements MarketStream {
  /** 종목코드 → 리스너 (구독 요청 표기는 requestedSymbols) */
  private readonly listeners = new Map<string, TradeListener[]>();
  private readonly requestedSymbols = new Map<string, string>();

  constructor(private readonly options: KisStreamOptions) {
    super("kis");
  }

  get isConnected(): boolean { return this.isSocketOpen; }

  protected uri(): string { return this.options.wsUrl; }

  protected async onOpen(): Promise<void> {
    const key = await this.options.approvalKey();
    for (const code of this.listeners.keys()) this.send(this.subscribeMessage(key, code));
  }

  subscribeTrades(symbols: string[], listener: TradeListener): void {
    const newCodes: string[] = [];
    for (const symbol of symbols) {
      const code = symbolCode(symbol);
      if (!this.requestedSymbols.has(code)) this.requestedSymbols.set(code, symbol);
      let list = this.listeners.get(code);
      if (!list) { list = []; this.listeners.set(code, list); newCodes.push(code); }
      list.push(listener);
    }
    if (newCodes.length > 0 && this.isSocketOpen) {
      this.options.approvalKey()
        .then((key) => { for (const code of newCodes) this.send(this.subscribeMessage(key, code)); })
        .catch((e) => console.error(`ERROR hermetix kis stream: 접속키 발급 실패 - ${e}`));
    }
  }

  protected onMessage(text: string): void {
    if (text.startsWith("0|") || text.startsWith("1|")) {
      for (const tick of parseKisFrame(text)) this.deliver(tick);
      return;
    }
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
    if (rtCd === "0" || msgCd === "OPSP0000") console.log(`INFO hermetix kis stream: subscribed ${trId} ${trKey} (${msg})`);
    else if (msgCd === "OPSP0002") console.log(`INFO hermetix kis stream: already subscribed ${trId} ${trKey}`);
    else console.warn(`WARN hermetix kis stream: ${trId} ${trKey} rt_cd=${rtCd} msg_cd=${msgCd} ${msg}`);
  }

  private deliver(tick: TradeTick): void {
    const code = tick.symbol;
    const symbol = this.requestedSymbols.get(code) ?? code;
    const normalized = symbol === code ? tick : { ...tick, symbol };
    for (const listener of this.listeners.get(code) ?? []) {
      try { listener(normalized); } catch (e) { console.error(`ERROR hermetix kis stream: 리스너 오류 / ${symbol}: ${e}`); }
    }
  }

  private subscribeMessage(key: string, code: string, trType = "1"): string {
    return JSON.stringify({
      header: { approval_key: key, custtype: this.options.custtype, tr_type: trType, "content-type": "utf-8" },
      body: { input: { tr_id: KIS_TR_TRADE, tr_key: code } },
    });
  }
}

/**
 * 데이터 프레임 → 체결 목록. 심볼은 단축코드 그대로 (요청 표기 복원은 스트림이 한다).
 * 알 수 없는 TR·필드 부족은 빈 목록. today 는 KST 날짜 "YYYY-MM-DD"
 */
export function parseKisFrame(frame: string, today: string = kstToday()): TradeTick[] {
  const parts = frame.split("|");
  if (parts.length < 4 || parts[1] !== KIS_TR_TRADE) return [];
  const count = Math.max(1, Number.parseInt(parts[2], 10) || 1);
  const fields = parts.slice(3).join("|").split("^");
  const width = Math.floor(fields.length / count);
  if (width < MIN_FIELDS) return [];
  const ticks: TradeTick[] = [];
  for (let i = 0; i < count; i++) {
    const f = fields.slice(i * width, (i + 1) * width);
    try {
      ticks.push(parseRecord(f, today));
    } catch { /* 레코드 하나가 깨져도 나머지는 전달 */ }
  }
  return ticks;
}

function parseRecord(f: string[], today: string): TradeTick {
  const sign = f[3];
  let change = DorNull(f[4]);
  // 부호 코드: 1 상한 2 상승 3 보합 4 하한 5 하락. 값이 부호 없이 오면 코드로 부호를 붙인다
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
