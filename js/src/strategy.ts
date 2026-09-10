/** 전략 SPI - 전략 작성자가 구현하는 유일한 표면. */
import { Decimal } from "decimal.js";
import type {
  Account, Candle, CandleInterval, Holding, Order, OrderType, Quote, TimeInForce,
} from "./models.js";
import { symbolsMatch } from "./models.js";

export interface StrategySpec {
  name: string;
  symbols: string[];
  candleInterval?: CandleInterval;       // 기본 "1d"
  candleLimit?: number;                  // 기본 30
  pollIntervalSeconds?: number;          // 기본 60
  regularHoursOnly?: boolean;            // 기본 true
}

/** 매수 진입. takeProfit/stopLoss 지정 시 엔진이 자동 청산 (소프트웨어 브라켓 - 재시작 시 소실). */
export interface Buy {
  kind: "buy";
  symbol: string;
  quantity: Decimal;
  orderType?: OrderType;                 // 기본 MARKET
  limitPrice?: Decimal | null;
  timeInForce?: TimeInForce;
  takeProfitPrice?: Decimal | null;
  stopLossPrice?: Decimal | null;
}

/** 매도 청산. 보유 수량 내로 자동 클램프 (공매도 방지). */
export interface Sell {
  kind: "sell";
  symbol: string;
  quantity: Decimal;
  orderType?: OrderType;
  limitPrice?: Decimal | null;
  timeInForce?: TimeInForce;
}

export interface Cancel { kind: "cancel"; orderId: string; }

export type Signal = Buy | Sell | Cancel;

export const buy = (symbol: string, quantity: Decimal, opts: Partial<Omit<Buy, "kind" | "symbol" | "quantity">> = {}): Buy =>
  ({ kind: "buy", symbol, quantity, ...opts });
export const sell = (symbol: string, quantity: Decimal, opts: Partial<Omit<Sell, "kind" | "symbol" | "quantity">> = {}): Sell =>
  ({ kind: "sell", symbol, quantity, ...opts });
export const cancel = (orderId: string): Cancel => ({ kind: "cancel", orderId });

/** 전략 호출 시점의 시장/계좌 스냅샷. */
function bySymbol<T>(map: ReadonlyMap<string, T>, symbol: string): T | undefined {
  const direct = map.get(symbol);
  if (direct !== undefined) return direct;
  for (const [key, value] of map) if (symbolsMatch(key, symbol)) return value;
  return undefined;
}

export class StrategyContext {
  constructor(
    public readonly now: Date,
    public readonly quotes: ReadonlyMap<string, Quote>,
    public readonly candles: ReadonlyMap<string, Candle[]>,
    public readonly account: Account,
    public readonly holdings: ReadonlyMap<string, Holding>,
    public readonly openOrders: readonly Order[],
    public readonly buyingPower: Decimal,
  ) {}

  // 심볼 조회는 MARKET:CODE 접두 유무를 무시하고 코드로 맞춘다 (symbolsMatch)
  quote(symbol: string): Quote | undefined { return bySymbol(this.quotes, symbol); }
  candlesOf(symbol: string): Candle[] { return bySymbol(this.candles, symbol) ?? []; }
  holding(symbol: string): Holding | undefined { return bySymbol(this.holdings, symbol); }
  hasPosition(symbol: string): boolean {
    const h = this.holding(symbol);
    return h !== undefined && h.quantity.gt(0);
  }
  openOrdersOf(symbol: string): Order[] {
    return this.openOrders.filter((o) => o.symbol != null && symbolsMatch(o.symbol, symbol));
  }
  hasOpenOrder(symbol: string): boolean { return this.openOrdersOf(symbol).length > 0; }
}

/**
 * 전략 인터페이스. 규약:
 * - decide 는 엔진이 pollInterval 주기로 호출 (기본: 해당 시장 정규장 중에만)
 * - 반환한 Signal 목록은 엔진이 순서대로 실행. 할 일 없으면 빈 배열
 * - 전략 안에서 브로커 API 직접 호출 금지 - 데이터는 StrategyContext 로 공급된다
 */
export interface Strategy {
  readonly spec: StrategySpec;
  decide(ctx: StrategyContext): Signal[] | Promise<Signal[]>;
}
