/**
 * 넥스트증권 모의투자 어댑터 (미국주식). 실측 기반 (2026-08).
 * OAuth client_credentials, 토큰 24h, 401 시 1회 재발급-재시도.
 */
import { Decimal } from "decimal.js";
import { BrokerClient, D, DorNull, httpJson } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
  MarketClosedError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote,
} from "../models.js";

interface NextError { type?: string; code?: string; message?: string; requestId?: string; }

export class NextClient implements BrokerClient {
  readonly capabilities: BrokerCapabilities = {
    brokerId: "next",
    market: "US",
    currency: "USD",
    candleIntervals: new Set<CandleInterval>(["1m", "5m", "1h", "1d"]),
    clientOrderId: true,
    nativeBracket: false,
    fractionalShares: false,
    serverOpenOrders: true,
  };

  private token: string | null = null;
  private tokenExpiresAt = 0;

  constructor(
    private readonly clientId: string,
    private readonly clientSecret: string,
    private readonly accountId: string = "acc_main",
    private readonly baseUrl: string = "https://openapi.nextsecurities.dev",
  ) {}

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const body = await this.request("GET", `/v1/market/quotes?symbols=${symbols.join(",")}`);
    return (body.quotes as Record<string, unknown>[]).map((q) => ({
      symbol: String(q.symbol),
      price: D(q.price),
      bidPrice: DorNull(q.bidPrice),
      askPrice: DorNull(q.askPrice),
      volume: Number(q.volume ?? 0),
      change: DorNull(q.change),
      changeRate: DorNull(q.changeRate),
      timestamp: q.timestamp ? new Date(String(q.timestamp)) : new Date(),
    }));
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    let path = `/v1/market/candles?symbol=${symbol}&interval=${interval}`;
    if (limit !== undefined) path += `&limit=${limit}`;
    const body = await this.request("GET", path);
    return (body.candles as Record<string, unknown>[]).map((c) => ({
      timestamp: new Date(String(c.timestamp)),
      open: D(c.open), high: D(c.high), low: D(c.low), close: D(c.close),
      volume: Number(c.volume ?? 0),
    }));
  }

  async getCalendar(): Promise<MarketDay[]> {
    const body = await this.request("GET", "/v1/market/calendar");
    return (body.calendar as Record<string, unknown>[]).map((d) => {
      const regular = (d.sessions as Record<string, { start: string; end: string }> | null)?.regular ?? null;
      return {
        date: String(d.date),
        open: Boolean(d.open),
        regular,
        timezone: String(d.timezone ?? "America/New_York"),
        holiday: (d.holiday as string | null) ?? null,
      };
    });
  }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const body = await this.request("GET", "/v1/account", { account: true });
    return {
      accountId: String(body.accountId),
      currency: String(body.currency ?? "USD"),
      cash: D(body.cash),
      portfolioValue: D(body.portfolioValue),
      status: String(body.status ?? "ACTIVE"),
      name: (body.name as string | null) ?? null,
    };
  }

  async getHoldings(): Promise<Holding[]> {
    const body = await this.request("GET", "/v1/account/holdings", { account: true });
    return (body.holdings as Record<string, unknown>[]).map((h) => ({
      symbol: String(h.symbol),
      quantity: D(h.quantity),
      avgEntryPrice: D(h.avgEntryPrice),
      currentPrice: DorNull(h.currentPrice),
      marketValue: DorNull(h.marketValue),
      unrealizedPnl: DorNull(h.unrealizedPnl),
      unrealizedPnlRate: DorNull(h.unrealizedPnlRate),
    }));
  }

  async getBuyingPower(): Promise<Decimal> {
    const body = await this.request("GET", "/v1/account/buying-power", { account: true });
    return D(body.buyingPower);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const payload: Record<string, unknown> = {
      symbol: request.symbol,
      side: request.side,
      orderType: request.orderType,
      quantity: request.quantity.toString(),
      timeInForce: request.timeInForce ?? "DAY",
    };
    if (request.limitPrice) payload.limitPrice = request.limitPrice.toString();
    if (request.clientOrderId) payload.clientOrderId = request.clientOrderId;
    return this.toOrder(await this.request("POST", "/v1/orders", { account: true, json: payload }));
  }

  async getOrders(): Promise<Order[]> {
    const body = await this.request("GET", "/v1/orders", { account: true });
    return (body.orders as Record<string, unknown>[]).map((o) => this.toOrder(o));
  }

  async getOrder(orderId: string): Promise<Order> {
    return this.toOrder(await this.request("GET", `/v1/orders/${orderId}`, { account: true }));
  }

  async cancelOrder(orderId: string): Promise<Order> {
    return this.toOrder(await this.request("DELETE", `/v1/orders/${orderId}`, { account: true }));
  }

  async getFills(): Promise<Fill[]> {
    const body = await this.request("GET", "/v1/orders/fills", { account: true });
    return (body.fills as Record<string, unknown>[]).map((f) => ({
      fillId: (f.fillId as string) ?? null,
      orderId: (f.orderId as string) ?? null,
      symbol: (f.symbol as string) ?? null,
      side: (f.side as OrderSide) ?? null,
      quantity: DorNull(f.quantity),
      price: DorNull(f.price),
    }));
  }

  // -------------------------------------------------------------- internal

  private toOrder(body: Record<string, unknown>): Order {
    const statuses = ["SUBMITTED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"];
    const status = statuses.includes(String(body.status)) ? (body.status as OrderStatus) : "UNKNOWN";
    return {
      orderId: String(body.orderId),
      status,
      symbol: (body.symbol as string) ?? null,
      side: (body.side as OrderSide) ?? null,
      orderType: (body.orderType as OrderType) ?? null,
      quantity: DorNull(body.quantity),
      limitPrice: DorNull(body.limitPrice),
      filledQuantity: DorNull(body.filledQuantity),
      avgFillPrice: DorNull(body.avgFillPrice),
      clientOrderId: (body.clientOrderId as string) ?? null,
      submittedAt: body.submittedAt ? new Date(String(body.submittedAt)) : null,
      canceledAt: body.canceledAt ? new Date(String(body.canceledAt)) : null,
    };
  }

  private async request(
    method: string, path: string,
    opts: { account?: boolean; json?: Record<string, unknown> } = {},
  ): Promise<Record<string, unknown>> {
    const call = async (): Promise<Record<string, unknown>> => {
      const headers: Record<string, string> = { Authorization: `Bearer ${await this.getToken()}` };
      if (opts.account) headers["X-Nextsecurities-Account"] = this.accountId;
      if (opts.json) headers["Content-Type"] = "application/json";
      const [status, body] = await httpJson(this.baseUrl + path, {
        method, headers, body: opts.json ? JSON.stringify(opts.json) : undefined,
      });
      if (status < 200 || status >= 300) throw this.mapError(status, (body.error ?? {}) as NextError);
      return body;
    };
    try {
      return await call();
    } catch (e) {
      if (e instanceof AuthError) {
        this.token = null; // 토큰 만료 - 1회 재발급 후 재시도
        return call();
      }
      throw e;
    }
  }

  private mapError(status: number, error: NextError): BrokerApiError {
    const code = error.code ?? null;
    const message = `Next(${code}) ${error.message ?? ""} requestId=${error.requestId}`;
    if (status === 401 || error.type === "authentication") return new AuthError(status, code, message);
    if (status === 429) return new RateLimitError(status, code, message);
    if (code === "order-not-found") return new OrderNotFoundError(code, message);
    if (code?.includes("insufficient")) return new InsufficientFundsError(status, code, message);
    if (code === "trading-halted" || code?.includes("market-closed")) return new MarketClosedError(status, code, message);
    if (error.type === "validation") return new InvalidOrderError(status, code, message);
    return new BrokerApiError(status, code, message);
  }

  private async getToken(): Promise<string> {
    if (this.token && Date.now() < this.tokenExpiresAt - 60_000) return this.token;
    const form = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.clientId,
      client_secret: this.clientSecret,
    });
    const [status, body] = await httpJson(`${this.baseUrl}/v1/oauth/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: form.toString(),
    });
    if (status !== 200 || typeof body.access_token !== "string") {
      const error = (body.error ?? {}) as NextError;
      throw new AuthError(status, error.code ?? null, `Next 토큰 발급 실패: ${error.message}`);
    }
    this.token = body.access_token;
    this.tokenExpiresAt = Date.now() + Number(body.expires_in ?? 86400) * 1000;
    return this.token;
  }
}
