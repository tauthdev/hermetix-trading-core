/**
 * Hermetix - 증권사 모의투자 통합 트레이딩 프레임워크 (JavaScript/TypeScript).
 *
 * 빠른 시작:
 *
 *   import { NextClient, StrategyEngine, buy, Decimal } from "hermetix";
 *
 *   const strategy = {
 *     spec: { name: "my-first", symbols: ["AAPL"] },
 *     decide(ctx) {
 *       const q = ctx.quote("AAPL");
 *       if (q && !ctx.hasPosition("AAPL") && !ctx.hasOpenOrder("AAPL")) {
 *         return [buy("AAPL", new Decimal(1), {
 *           takeProfitPrice: q.price.mul("1.04"),
 *           stopLossPrice: q.price.mul("0.98"),
 *         })];
 *       }
 *       return [];
 *     },
 *   };
 *
 *   const broker = new NextClient("pk_test_...", "sk_test_...");
 *   await new StrategyEngine(broker, [strategy]).run();
 *
 * 브로커 전환은 클라이언트 교체 한 줄:
 *   new KisClient(appkey, appsecret, cano)   // 한국투자 모의 (KRX)
 *   new KiwoomClient(appkey, secretkey)      // 키움 모의 (KRX)
 */
export { Decimal } from "./models.js";
export type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, TimeInForce, TradingEnvironment,
} from "./models.js";
export { isOpenStatus, parseSymbol, symbolCode, symbolCodeFor, symbolsMatch } from "./models.js";
export {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
  MarketClosedError, OrderNotFoundError, RateLimitError,
} from "./errors.js";
export type { BrokerClient } from "./broker.js";
export { RateLimiter, Throttle, krxTickRound } from "./broker.js";
export { verifyBrokerConformance } from "./testing.js";
export type { ConformanceReport, ConformanceScenario } from "./testing.js";
export { NextClient } from "./brokers/next.js";
export { KisClient } from "./brokers/kis.js";
export { KiwoomClient } from "./brokers/kiwoom.js";
export { NhClient } from "./brokers/nh.js";
export { DbClient } from "./brokers/db.js";
export type { Buy, Cancel, Sell, Signal, Strategy, StrategySpec } from "./strategy.js";
export { StrategyContext, buy, cancel, sell } from "./strategy.js";
export {
  BracketMonitor, MarketCalendar, OrderExecutor, RiskGuard, StrategyEngine, TradingGuard, pnlReport,
} from "./engine.js";
export type { EngineOptions, PnlReport } from "./engine.js";
