"""Hermetix - 증권사 모의투자 통합 트레이딩 프레임워크 (Python).

빠른 시작:

    from decimal import Decimal
    from hermetix import NextClient, Strategy, StrategySpec, StrategyEngine, Buy, CandleInterval

    class MyStrategy(Strategy):
        spec = StrategySpec(name="my-first", symbols=["AAPL"])

        def decide(self, ctx):
            q = ctx.quote("AAPL")
            if q and not ctx.has_position("AAPL") and not ctx.has_open_order("AAPL"):
                return [Buy("AAPL", Decimal(1),
                            take_profit_price=q.price * Decimal("1.04"),
                            stop_loss_price=q.price * Decimal("0.98"))]
            return []

    broker = NextClient(client_id="pk_test_...", client_secret="sk_test_...")
    StrategyEngine(broker, [MyStrategy()]).run()

브로커 전환은 클라이언트 교체 한 줄:

    broker = KisClient(appkey=..., appsecret=..., cano=...)      # 한국투자 모의
    broker = KiwoomClient(appkey=..., secretkey=...)             # 키움 모의
"""
from .broker import (BrokerClient, MarketStream, OrderBookListener, OrderEventListener, RateLimiter, StreamingBrokerClient,
                     TradeListener)
from .testing import ConformanceReport, ConformanceScenario, verify_broker_conformance
from .brokers.db import DbClient
from .brokers.kb import KbClient
from .brokers.kis import KisClient
from .brokers.ls import LsClient
from .brokers.nh import NhClient
from .brokers.toss import TossClient
from .brokers.kiwoom import KiwoomClient
from .brokers.next import NextClient
from .brokers.db_stream import DbMarketStream
from .brokers.kis_stream import KisMarketStream
from .brokers.kiwoom_stream import KiwoomMarketStream
from .brokers.ls_stream import LsMarketStream
from .brokers.nh_stream import NhMarketStream
from .brokers.toss_stream import TossMarketStream
from .engine import BracketMonitor, MarketCalendar, OrderExecutor, RiskGuard, StrategyEngine, TradingGuard, pnl_report
from .errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
    MarketClosedError, OrderNotFoundError, RateLimitError,
)
from .models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, OrderStatus,
    OrderType, Quote, StreamChannel, TimeInForce, TradeTick, TradingEnvironment, normalize_order_id, parse_symbol, symbol_code,
    symbols_match,
)
from .strategy import Buy, Cancel, Sell, Signal, Strategy, StrategyContext, StrategySpec, TickTrigger

__version__ = "0.10.0"

__all__ = [
    "BrokerClient", "RateLimiter", "MarketStream", "StreamingBrokerClient", "TradeListener", "NextClient", "KisClient", "KiwoomClient", "NhClient", "DbClient",
    "LsClient", "TossClient", "KbClient",
    "ConformanceReport", "ConformanceScenario", "verify_broker_conformance",
    "StrategyEngine", "TradingGuard", "BracketMonitor", "OrderExecutor", "RiskGuard", "MarketCalendar", "pnl_report",
    "TradingEnvironment", "parse_symbol", "symbol_code", "symbols_match",
    "Strategy", "StrategySpec", "StrategyContext", "Signal", "Buy", "Sell", "Cancel", "TickTrigger",
    "StreamChannel", "TradeTick", "OrderBookLevel", "OrderBookTick", "OrderEvent", "OrderEventType", "normalize_order_id",
    "OrderBookListener", "OrderEventListener",
    "KisMarketStream", "KiwoomMarketStream", "NhMarketStream", "DbMarketStream", "LsMarketStream", "TossMarketStream",
    "Account", "BrokerCapabilities", "Candle", "CandleInterval", "CreateOrderRequest",
    "Fill", "Holding", "MarketDay", "Order", "OrderSide", "OrderStatus", "OrderType", "Quote", "TimeInForce",
    "BrokerApiError", "AuthError", "RateLimitError", "MarketClosedError",
    "InsufficientFundsError", "InvalidOrderError", "OrderNotFoundError",
]
