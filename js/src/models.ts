/**
 * 공통 도메인 모델 (Kotlin 레퍼런스의 dto 와 동일 의미).
 *
 * 금액/수량은 전부 Decimal(decimal.js) - JS number 를 절대 섞지 말 것 (0.1+0.2 문제).
 */
import { Decimal } from "decimal.js";

export { Decimal };

export type CandleInterval = "1m" | "5m" | "1h" | "1d";

/** 거래 환경. PAPER=모의투자(기본), LIVE=실전 — 엔진은 liveTradingEnabled 없이는 LIVE 를 기동하지 않는다. 키는 항상 사용자 기기에서만 쓰인다 */
export type TradingEnvironment = "PAPER" | "LIVE";

const MARKET_PREFIX = /^([A-Z]{2,6}):(.+)$/;

/** `MARKET:CODE` 표기를 { market, code } 로 나눈다. 접두가 없으면 market=null */
export function parseSymbol(symbol: string): { market: string | null; code: string } {
  const m = MARKET_PREFIX.exec(symbol);
  return m ? { market: m[1], code: m[2] } : { market: null, code: symbol };
}

/** 접두를 뗀 브로커 심볼 코드 */
export const symbolCode = (symbol: string): string => parseSymbol(symbol).code;

/** 코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다 */
export function symbolsMatch(a: string, b: string): boolean {
  const x = parseSymbol(a);
  const y = parseSymbol(b);
  return x.code === y.code && (x.market === null || y.market === null || x.market === y.market);
}

/** 심볼의 시장 접두가 지원 시장인지 확인하고 브로커 코드를 돌려준다. 미지원이면 throw */
export function symbolCodeFor(caps: BrokerCapabilities, symbol: string): string {
  const { market, code } = parseSymbol(symbol);
  const markets = caps.markets ?? new Set([caps.market]);
  if (market !== null && !markets.has(market)) {
    throw new Error(`브로커 '${caps.brokerId}' 는 시장 '${market}' 을 지원하지 않습니다 (지원: ${[...markets].join(",")}): ${symbol}`);
  }
  return code;
}
export type OrderSide = "BUY" | "SELL";
export type OrderType = "MARKET" | "LIMIT";
export type TimeInForce = "DAY" | "GTC";
/**
 * 주문 상태 (넥스트증권 공개 스펙 v1.3 부록 D 7종 + UNKNOWN 폴백).
 * PENDING_CANCEL 은 취소 접수 후 미확정 — 원주문이 체결될 수 있으므로 OPEN 으로 분류한다.
 */
export type OrderStatus =
  | "SUBMITTED" | "PARTIALLY_FILLED" | "PENDING_CANCEL" | "FILLED"
  | "CANCELED" | "REJECTED" | "EXPIRED" | "UNKNOWN";

export const isOpenStatus = (s: OrderStatus): boolean =>
  s === "SUBMITTED" || s === "PARTIALLY_FILLED" || s === "PENDING_CANCEL";

export interface Quote {
  symbol: string;
  price: Decimal;
  bidPrice: Decimal | null;
  askPrice: Decimal | null;
  volume: number;
  change: Decimal | null;
  changeRate: Decimal | null;
  timestamp: Date;
}

export interface Candle {
  timestamp: Date;
  open: Decimal;
  high: Decimal;
  low: Decimal;
  close: Decimal;
  volume: number;
}

export interface SessionHours { start: string; end: string; }

export interface MarketDay {
  date: string;             // "2026-08-06"
  open: boolean;
  regular: SessionHours | null;
  timezone: string;
  holiday?: string | null;
}

export interface Account {
  accountId: string;
  currency: string;
  cash: Decimal;
  portfolioValue: Decimal;
  status: string;
  name?: string | null;
}

export interface Holding {
  symbol: string;
  quantity: Decimal;
  avgEntryPrice: Decimal;
  currentPrice?: Decimal | null;
  marketValue?: Decimal | null;
  unrealizedPnl?: Decimal | null;
  unrealizedPnlRate?: Decimal | null;
}

export interface CreateOrderRequest {
  symbol: string;
  side: OrderSide;
  orderType: OrderType;
  quantity: Decimal;
  limitPrice?: Decimal | null;
  timeInForce?: TimeInForce;
  clientOrderId?: string | null;
}

export interface Order {
  orderId: string;
  status: OrderStatus;
  symbol?: string | null;
  side?: OrderSide | null;
  orderType?: OrderType | null;
  quantity?: Decimal | null;
  limitPrice?: Decimal | null;
  filledQuantity?: Decimal | null;
  avgFillPrice?: Decimal | null;
  clientOrderId?: string | null;
  submittedAt?: Date | null;
  canceledAt?: Date | null;
}

export interface Fill {
  fillId: string | null;
  orderId: string | null;
  symbol: string | null;
  side: OrderSide | null;
  quantity: Decimal | null;
  price: Decimal | null;
}

/** 브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 true 로 선언한다. */
export interface BrokerCapabilities {
  brokerId: string;
  market: "US" | "KRX";
  currency: string;
  candleIntervals: ReadonlySet<CandleInterval>;
  clientOrderId: boolean;
  nativeBracket: boolean;
  fractionalShares: boolean;
  /** false 면 어댑터가 메모리 추적 (재시작 시 추적 소실) */
  serverOpenOrders: boolean;
  /** 지원 거래 환경. 생략 시 PAPER 만. 실전(LIVE)은 실측으로 확인한 어댑터만 선언 */
  environments?: ReadonlySet<TradingEnvironment>;
  /** 한 계좌로 다룰 수 있는 시장 목록 (MARKET:CODE 접두 허용 값). 생략 시 {market} */
  markets?: ReadonlySet<string>;
}
