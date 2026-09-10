/** 브로커 추상화 인터페이스 + 공용 HTTP/유틸. */
import { Decimal } from "decimal.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, Quote, TradingEnvironment,
} from "./models.js";

export interface BrokerClient {
  readonly capabilities: BrokerCapabilities;
  /** 이 인스턴스가 연결된 거래 환경. 생략 시 PAPER. 엔진은 LIVE 면 명시 동의(liveTradingEnabled)를 요구한다 */
  readonly environment?: TradingEnvironment;
  getQuotes(symbols: string[]): Promise<Quote[]>;
  getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]>;
  getCalendar(): Promise<MarketDay[]>;
  getAccount(): Promise<Account>;
  getHoldings(): Promise<Holding[]>;
  getBuyingPower(): Promise<Decimal>;
  createOrder(request: CreateOrderRequest): Promise<Order>;
  getOrders(): Promise<Order[]>;
  getOrder(orderId: string): Promise<Order>;
  cancelOrder(orderId: string): Promise<Order>;
  getFills(): Promise<Fill[]>;
}

/** fetch 기반 최소 HTTP - (status, parsedJson) 반환. 4xx/5xx 도 본문 파싱. */
export async function httpJson(
  url: string,
  init: { method: string; headers?: Record<string, string>; body?: string },
): Promise<[number, Record<string, unknown>]> {
  const res = await fetch(url, init);
  const text = await res.text();
  let parsed: Record<string, unknown> = {};
  try {
    const value = JSON.parse(text);
    parsed = typeof value === "object" && value !== null ? value : {};
  } catch { /* 빈 본문/비 JSON */ }
  return [res.status, parsed];
}

/** 호출 간 최소 간격 보장 (모의 서버 레이트리밋 회피). */
export class Throttle {
  private last = 0;
  private chain: Promise<void> = Promise.resolve();

  constructor(private readonly minIntervalMs: number) {}

  wait(): Promise<void> {
    this.chain = this.chain.then(async () => {
      const delta = this.last + this.minIntervalMs - Date.now();
      if (delta > 0) await sleep(delta);
      this.last = Date.now();
    });
    return this.chain;
  }
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const KST = "Asia/Seoul";

export function kstToday(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: KST }).format(new Date()); // YYYY-MM-DD
}

export function kstYyyymmdd(offsetDays = 0): string {
  const d = new Date(Date.now() + offsetDays * 86400_000);
  return new Intl.DateTimeFormat("en-CA", { timeZone: KST }).format(d).replaceAll("-", "");
}

/** KRX 정규장 합성 캘린더 (공휴일 미반영 - 주문은 서버가 거부하므로 안전). */
export function krxCalendar(days = 31): MarketDay[] {
  const result: MarketDay[] = [];
  for (let offset = 0; offset < days; offset++) {
    const d = new Date(Date.now() + offset * 86400_000);
    const date = new Intl.DateTimeFormat("en-CA", { timeZone: KST }).format(d);
    const weekday = new Intl.DateTimeFormat("en-US", { timeZone: KST, weekday: "short" }).format(d);
    const open = weekday !== "Sat" && weekday !== "Sun";
    result.push({
      date, open,
      regular: open ? { start: "09:00", end: "15:30" } : null,
      timezone: KST,
    });
  }
  return result;
}

/** KRX 호가단위 보정 (2023-01 개정) - 유효 호가로 내림. */
export function krxTickRound(price: Decimal): Decimal {
  let tick: number;
  if (price.lt(2_000)) tick = 1;
  else if (price.lt(5_000)) tick = 5;
  else if (price.lt(20_000)) tick = 10;
  else if (price.lt(50_000)) tick = 50;
  else if (price.lt(200_000)) tick = 100;
  else if (price.lt(500_000)) tick = 500;
  else tick = 1_000;
  return price.divToInt(tick).mul(tick);
}

export const D = (value: unknown, fallback = "0"): Decimal => {
  const text = value === null || value === undefined || value === "" ? fallback : String(value).trim();
  return new Decimal(text || fallback);
};

export const DorNull = (value: unknown): Decimal | null => {
  if (value === null || value === undefined || value === "") return null;
  try {
    return new Decimal(String(value).trim());
  } catch {
    return null;
  }
};
