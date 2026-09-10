/**
 * 공통 도메인 모델 (Kotlin 레퍼런스의 dto 와 동일 의미).
 *
 * 금액/수량은 전부 Decimal(decimal.js) - JS number 를 절대 섞지 말 것 (0.1+0.2 문제).
 */
import { Decimal } from "decimal.js";

export { Decimal };

export type CandleInterval = "1m" | "5m" | "1h" | "1d";
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
}
