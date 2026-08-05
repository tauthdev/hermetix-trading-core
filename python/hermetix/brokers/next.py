"""넥스트증권 모의투자 어댑터 (미국주식).

실측 기반 (openapi.nextsecurities.dev, 2026-08):
- OAuth client_credentials, 토큰 24h. 401 시 1회 재발급-재시도
- 에러 엔벨로프: {"error": {type, code, message, ...}}
- 고급 주문(STOP/BRACKET)/kill-switch/modify/cancel-all 은 서버 미배포
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from ..broker import BrokerClient, _Http
from ..errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
    MarketClosedError, OrderNotFoundError, RateLimitError,
)
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote,
    SessionHours,
)


def _d(value) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class NextClient(BrokerClient):

    capabilities = BrokerCapabilities(
        broker_id="next",
        market="US",
        currency="USD",
        candle_intervals=frozenset(CandleInterval),
        client_order_id=True,
        native_bracket=False,  # 서버 /v1/orders/advanced 배포 시 전환 예정
        fractional_shares=False,
        server_open_orders=True,
    )

    def __init__(self, client_id: str, client_secret: str, account_id: str = "acc_main",
                 base_url: str = "https://openapi.nextsecurities.dev"):
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_id = account_id
        self._http = _Http(base_url)
        self._token: str | None = None
        self._token_expires_at = 0.0
        self._token_lock = threading.Lock()

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        body = self._get("/v1/market/quotes", query={"symbols": ",".join(symbols)})
        return [
            Quote(
                symbol=q["symbol"],
                price=_d(q["price"]),
                bid_price=_d(q.get("bidPrice")),
                ask_price=_d(q.get("askPrice")),
                volume=int(q.get("volume") or 0),
                change=_d(q.get("change")),
                change_rate=_d(q.get("changeRate")),
                timestamp=_ts(q.get("timestamp")) or datetime.now(timezone.utc),
            )
            for q in body.get("quotes", [])
        ]

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        query = {"symbol": symbol, "interval": interval.value}
        if limit is not None:
            query["limit"] = str(limit)
        body = self._get("/v1/market/candles", query=query)
        return [
            Candle(
                timestamp=_ts(c["timestamp"]),
                open=_d(c["open"]), high=_d(c["high"]), low=_d(c["low"]), close=_d(c["close"]),
                volume=int(c.get("volume") or 0),
            )
            for c in body.get("candles", [])
        ]

    def get_calendar(self) -> list[MarketDay]:
        body = self._get("/v1/market/calendar")
        days = []
        for d in body.get("calendar", []):
            regular = (d.get("sessions") or {}).get("regular")
            days.append(MarketDay(
                date=d["date"],
                open=bool(d.get("open")),
                regular=SessionHours(regular["start"], regular["end"]) if regular else None,
                timezone=d.get("timezone", "America/New_York"),
                holiday=d.get("holiday"),
            ))
        return days

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        body = self._get("/v1/account", account=True)
        return Account(
            account_id=body["accountId"],
            currency=body.get("currency", "USD"),
            cash=_d(body["cash"]),
            portfolio_value=_d(body["portfolioValue"]),
            status=body.get("status", "ACTIVE"),
            name=body.get("name"),
        )

    def get_holdings(self) -> list[Holding]:
        body = self._get("/v1/account/holdings", account=True)
        return [
            Holding(
                symbol=h["symbol"],
                quantity=_d(h["quantity"]),
                avg_entry_price=_d(h["avgEntryPrice"]),
                current_price=_d(h.get("currentPrice")),
                market_value=_d(h.get("marketValue")),
                unrealized_pnl=_d(h.get("unrealizedPnl")),
                unrealized_pnl_rate=_d(h.get("unrealizedPnlRate")),
            )
            for h in body.get("holdings", [])
        ]

    def get_buying_power(self) -> Decimal:
        return _d(self._get("/v1/account/buying-power", account=True)["buyingPower"])

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        payload = {
            "symbol": request.symbol,
            "side": request.side.value,
            "orderType": request.order_type.value,
            "quantity": str(request.quantity),
            "timeInForce": request.time_in_force.value,
        }
        if request.limit_price is not None:
            payload["limitPrice"] = str(request.limit_price)
        if request.client_order_id:
            payload["clientOrderId"] = request.client_order_id

        body = self._post("/v1/orders", payload, account=True)
        return self._order(body)

    def get_orders(self) -> list[Order]:
        body = self._get("/v1/orders", account=True)
        return [self._order(o) for o in body.get("orders", [])]

    def get_order(self, order_id: str) -> Order:
        return self._order(self._get(f"/v1/orders/{order_id}", account=True))

    def cancel_order(self, order_id: str) -> Order:
        return self._order(self._request("DELETE", f"/v1/orders/{order_id}", account=True))

    def get_fills(self) -> list[Fill]:
        body = self._get("/v1/orders/fills", account=True)
        return [
            Fill(
                fill_id=f.get("fillId"), order_id=f.get("orderId"), symbol=f.get("symbol"),
                side=OrderSide(f["side"]) if f.get("side") in ("BUY", "SELL") else None,
                quantity=_d(f.get("quantity")), price=_d(f.get("price")),
            )
            for f in body.get("fills", [])
        ]

    # ---------------------------------------------------------------- internal

    @staticmethod
    def _order(body: dict) -> Order:
        status = body.get("status", "UNKNOWN")
        return Order(
            order_id=body["orderId"],
            status=OrderStatus(status) if status in OrderStatus.__members__ else OrderStatus.UNKNOWN,
            symbol=body.get("symbol"),
            side=OrderSide(body["side"]) if body.get("side") in ("BUY", "SELL") else None,
            order_type=OrderType(body["orderType"]) if body.get("orderType") in ("MARKET", "LIMIT") else None,
            quantity=_d(body.get("quantity")),
            limit_price=_d(body.get("limitPrice")),
            filled_quantity=_d(body.get("filledQuantity")),
            avg_fill_price=_d(body.get("avgFillPrice")),
            client_order_id=body.get("clientOrderId"),
            submitted_at=_ts(body.get("submittedAt")),
            canceled_at=_ts(body.get("canceledAt")),
        )

    def _get(self, path: str, *, query: dict | None = None, account: bool = False) -> dict:
        return self._request("GET", path, query=query, account=account)

    def _post(self, path: str, payload: dict, *, account: bool = False) -> dict:
        return self._request("POST", path, json_body=payload, account=account)

    def _request(self, method: str, path: str, *, query: dict | None = None,
                 json_body: dict | None = None, account: bool = False) -> dict:
        def call() -> dict:
            headers = {"Authorization": f"Bearer {self._get_token()}"}
            if account and self._account_id:
                headers["X-Nextsecurities-Account"] = self._account_id
            status, body = self._http.request(method, path, headers=headers, query=query, json_body=json_body)
            if status < 200 or status >= 300:
                raise self._map_error(status, body.get("error") or {})
            return body

        try:
            return call()
        except AuthError:
            # 토큰 만료 - 1회 재발급 후 재시도
            self._token = None
            return call()

    @staticmethod
    def _map_error(status: int, error: dict) -> BrokerApiError:
        code = error.get("code")
        message = f"Next({code}) {error.get('message', '')} requestId={error.get('requestId')}"
        if status == 401 or error.get("type") == "authentication":
            return AuthError(status, code, message)
        if status == 429:
            return RateLimitError(status, code, message)
        if code == "order-not-found":
            return OrderNotFoundError(code, message)
        if code and "insufficient" in code:
            return InsufficientFundsError(status, code, message)
        if code == "trading-halted" or (code and "market-closed" in code):
            return MarketClosedError(status, code, message)
        if error.get("type") == "validation":
            return InvalidOrderError(status, code, message)
        return BrokerApiError(status, code, message)

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 60:
            return self._token
        with self._token_lock:
            if self._token and time.time() < self._token_expires_at - 60:
                return self._token
            status, body = self._http.request(
                "POST", "/v1/oauth/token",
                form_body={"grant_type": "client_credentials",
                           "client_id": self._client_id, "client_secret": self._client_secret},
            )
            if status != 200 or "access_token" not in body:
                error = body.get("error") or {}
                raise AuthError(status, error.get("code"), f"Next 토큰 발급 실패: {error.get('message')}")
            self._token = body["access_token"]
            self._token_expires_at = time.time() + float(body.get("expires_in", 86400))
            return self._token
