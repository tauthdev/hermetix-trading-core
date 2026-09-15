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
  Fill, Holding, MarketDay, Order, OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, OrderStatus, OrderType, Quote,
  StreamChannel, TimeInForce, TradeTick, TradingEnvironment,
} from "./models.js";
export {
  bestAsk, bestBid, isOpenStatus, normalizeOrderId, orderIdMatches, parseSymbol, symbolCode, symbolCodeFor, symbolsMatch, tradeTickToQuote,
} from "./models.js";
export {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
  MarketClosedError, OrderNotFoundError, RateLimitError,
} from "./errors.js";
export type { BrokerClient, MarketStream, OrderBookListener, OrderEventListener, StreamingBrokerClient, TradeListener } from "./broker.js";
export { RateLimiter, Throttle, isStreamingBrokerClient, krxTickRound } from "./broker.js";
export { ReconnectingWebSocket } from "./stream.js";
export { BrokerUsage, UsageTelemetry, TELEMETRY_ENDPOINT, classifyError, instrumentBroker } from "./telemetry.js";
export type { ErrorClass } from "./telemetry.js";
export { verifyBrokerConformance } from "./testing.js";
export type { ConformanceReport, ConformanceScenario } from "./testing.js";
export { NextClient } from "./brokers/next.js";
export { KisClient } from "./brokers/kis.js";
export { KisMarketStream, kisDecrypt, kisEncrypt, parseKisFrame, parseKisOrderBook, parseKisOrderEvents } from "./brokers/kisStream.js";
export { KiwoomClient } from "./brokers/kiwoom.js";
export { KiwoomMarketStream, parseKiwoomOrderBook, parseKiwoomOrderEvents, parseKiwoomReal } from "./brokers/kiwoomStream.js";
export { NhClient } from "./brokers/nh.js";
export { NhMarketStream, nhChannels, parseNhOrderBook, parseNhOrderEvents, parseNhTrade } from "./brokers/nhStream.js";
export { DbClient } from "./brokers/db.js";
export { DbMarketStream, dbStreamNormalizeCode, parseDbOrderBook, parseDbOrderEvent, parseDbTrade } from "./brokers/dbStream.js";
export { LsClient } from "./brokers/ls.js";
export { LsMarketStream, parseLsOrderBook, parseLsOrderEvents, parseLsTrade } from "./brokers/lsStream.js";
export { TossClient } from "./brokers/toss.js";
export { TossMarketStream, parseTossOrderBook, parseTossOrderEvent, parseTossTrade, tossCanonicalSymbol, tossTopicKey } from "./brokers/tossStream.js";
export { KbClient } from "./brokers/kb.js";
export type { Buy, Cancel, Sell, Signal, Strategy, StrategySpec, TickTrigger } from "./strategy.js";
export { StrategyContext, buy, cancel, sell } from "./strategy.js";
export {
  BracketMonitor, MarketCalendar, OrderExecutor, RiskGuard, StrategyEngine, TradingGuard, pnlReport,
} from "./engine.js";
export type { EngineOptions, PnlReport } from "./engine.js";
