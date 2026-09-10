/**
 * 한국투자증권(KIS) 모의투자 어댑터 (KRX). 실측 기반 (2026-08).
 *
 * - 초당 요청 제한 -> 600ms 쓰로틀 + EGW00201 백오프 재시도
 * - 토큰 발급 1분당 1회 제한 (토큰 24h 캐시)
 * - 모의 서버는 미체결/체결 조회 미제공 -> 메모리 주문 추적, 체결은 보유수량 변화 근사
 * - 취소는 지점번호 없이 ODNO 만으로 동작 / 캔들은 일봉만 / 지정가는 호가단위 보정
 */
import { Decimal } from "decimal.js";
import {
  BrokerClient, D, DorNull, Throttle, httpJson, krxCalendar, krxTickRound, kstToday, kstYyyymmdd, sleep,
} from "../broker.js";
import { AuthError, BrokerApiError, MarketClosedError, RateLimitError } from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, Quote,
  TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";
import { isOpenStatus } from "../models.js";

interface Tracked { order: Order; baselineQty: Decimal; day: string; }

export class KisClient implements BrokerClient {
  readonly capabilities: BrokerCapabilities = {
    brokerId: "kis",
    market: "KRX",
    currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false,
    nativeBracket: false,
    fractionalShares: false,
    serverOpenOrders: false, // 모의 서버가 주문 조회 미제공 - 어댑터 내부 추적
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
  };

  private token: string | null = null;
  private tokenExpiresAt = 0;
  private readonly throttle: Throttle;
  private readonly tracked = new Map<string, Tracked>();

  static readonly PAPER_URL = "https://openapivts.koreainvestment.com:29443";
  static readonly LIVE_URL = "https://openapi.koreainvestment.com:9443";
  readonly baseUrl: string;

  /**
   * baseUrl 을 비우면 환경에 따라 결정(모의 openapivts:29443 / 실전 openapi:9443).
   * throttleMs 0 이면 자동 — 모의 600(초당 2건), 실전 100(초당 20건 한도의 절반). 계좌 TR ID 는 모의 V / 실전 T 프리픽스.
   */
  constructor(
    private readonly appkey: string,
    private readonly appsecret: string,
    private readonly cano: string,
    private readonly acntPrdtCd: string = "01",
    baseUrl: string = "",
    throttleMs = 0,
    readonly environment: TradingEnvironment = "PAPER",
  ) {
    const live = environment === "LIVE";
    this.baseUrl = baseUrl || (live ? KisClient.LIVE_URL : KisClient.PAPER_URL);
    this.throttle = new Throttle(throttleMs || (live ? 100 : 600));
  }

  /** 계좌 TR ID — 모의 V, 실전 T 프리픽스 (예: tr("TTC0802U") → VTTC0802U / TTTC0802U) */
  tr(suffix: string): string { return (this.environment === "LIVE" ? "T" : "V") + suffix; }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const body = await this.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
        { query: { FID_COND_MRKT_DIV_CODE: "J", FID_INPUT_ISCD: symbolCodeFor(this.capabilities, symbol) } });
      const out = body.output as Record<string, unknown>;
      const rate = DorNull(out.prdy_ctrt);
      quotes.push({
        symbol,
        price: D(out.stck_prpr),
        bidPrice: null, askPrice: null,
        volume: Number(D(out.acml_vol)),
        change: DorNull(out.prdy_vrss),
        changeRate: rate ? rate.div(100) : null, // % -> 비율
        timestamp: new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("KIS 어댑터는 일봉(1d)만 지원합니다");
    const count = limit ?? 30;
    const body = await this.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
      { query: {
        FID_COND_MRKT_DIV_CODE: "J", FID_INPUT_ISCD: symbolCodeFor(this.capabilities, symbol),
        FID_INPUT_DATE_1: kstYyyymmdd(-(count * 1.6 + 10)),
        FID_INPUT_DATE_2: kstYyyymmdd(),
        FID_PERIOD_DIV_CODE: "D", FID_ORG_ADJ_PRC: "0",
      } });
    const candles = (body.output2 as Record<string, unknown>[])
      .filter((r) => r.stck_bsop_date)
      .map((r) => ({
        timestamp: kstDate(String(r.stck_bsop_date)),
        open: D(r.stck_oprc), high: D(r.stck_hgpr), low: D(r.stck_lwpr), close: D(r.stck_clpr),
        volume: Number(D(r.acml_vol)),
      }))
      .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime()); // 최신순 -> 과거→최신
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> {
    return krxCalendar();
  }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const summary = ((await this.balance()).output2 as Record<string, unknown>[])[0];
    if (!summary) throw new BrokerApiError(200, null, "KIS 잔고 요약(output2)이 비어 있습니다");
    return {
      accountId: this.cano, currency: "KRW",
      cash: D(summary.dnca_tot_amt), portfolioValue: D(summary.tot_evlu_amt), status: "ACTIVE",
    };
  }

  async getHoldings(): Promise<Holding[]> {
    return ((await this.balance()).output1 as Record<string, unknown>[])
      .filter((row) => D(row.hldg_qty).gt(0))
      .map((row) => {
        const rate = DorNull(row.evlu_pfls_rt);
        return {
          symbol: String(row.pdno),
          quantity: D(row.hldg_qty),
          avgEntryPrice: D(row.pchs_avg_pric),
          currentPrice: DorNull(row.prpr),
          marketValue: DorNull(row.evlu_amt),
          unrealizedPnl: DorNull(row.evlu_pfls_amt),
          unrealizedPnlRate: rate ? rate.div(100) : null,
        };
      });
  }

  async getBuyingPower(): Promise<Decimal> {
    const body = await this.call("GET", "/uapi/domestic-stock/v1/trading/inquire-psbl-order", this.tr("TTC8908R"),
      { query: { ...this.acct(), PDNO: "005930", ORD_UNPR: "", ORD_DVSN: "01",
                 CMA_EVLU_AMT_ICLD_YN: "N", OVRS_ICLD_YN: "N" } });
    return D((body.output as Record<string, unknown>).ord_psbl_cash);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const trId = this.tr(request.side === "BUY" ? "TTC0802U" : "TTC0801U");
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const body = await this.call("POST", "/uapi/domestic-stock/v1/trading/order-cash", trId,
      { json: { ...this.acct(), PDNO: code,
                ORD_DVSN: isLimit ? "00" : "01",
                ORD_QTY: request.quantity.toString(),
                // KRX 호가단위 보정 - 맞지 않는 지정가는 거래소가 거부한다
                ORD_UNPR: isLimit ? krxTickRound(request.limitPrice!).toString() : "0" } });
    const order: Order = {
      orderId: String((body.output as Record<string, unknown>).ODNO),
      status: "SUBMITTED",
      symbol: code, side: request.side, orderType: request.orderType, // 보유/추적과 같은 단일 시장 표기
      quantity: request.quantity, limitPrice: request.limitPrice ?? null,
      filledQuantity: new Decimal(0), submittedAt: new Date(),
    };
    this.tracked.set(order.orderId, {
      order, baselineQty: await this.holdingQty(code), day: kstToday(),
    });
    return order;
  }

  async getOrders(): Promise<Order[]> {
    await this.refreshTracked();
    return [...this.tracked.values()].filter((t) => isOpenStatus(t.order.status)).map((t) => t.order);
  }

  async getOrder(orderId: string): Promise<Order> {
    await this.refreshTracked();
    // 추적 밖(재시작 등)은 알 수 없어 취소로 간주
    return this.tracked.get(orderId)?.order ?? { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    // 실측: 모의 서버는 지점번호 없이 ODNO 만으로 취소된다
    await this.call("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", this.tr("TTC0803U"),
      { json: { ...this.acct(), KRX_FWDG_ORD_ORGNO: "", ORGN_ODNO: orderId, ORD_DVSN: "00",
                RVSE_CNCL_DVSN_CD: "02", ORD_QTY: "0", ORD_UNPR: "0", QTY_ALL_ORD_YN: "Y" } });
    const canceled: Order = { orderId, status: "CANCELED", canceledAt: new Date() };
    const tracked = this.tracked.get(orderId);
    if (tracked) this.tracked.set(orderId, { ...tracked, order: { ...tracked.order, ...canceled } });
    return canceled;
  }

  async getFills(): Promise<Fill[]> {
    await this.refreshTracked();
    return [...this.tracked.values()]
      .filter((t) => t.order.status === "FILLED")
      .map((t) => ({
        fillId: t.order.orderId, orderId: t.order.orderId,
        symbol: t.order.symbol ?? null, side: t.order.side ?? null,
        quantity: t.order.quantity ?? null, price: t.order.limitPrice ?? null,
      }));
  }

  // -------------------------------------------------------------- internal

  /** 추적 중인 미체결의 체결 여부를 보유 수량 변화로 판정 (모의 서버 제약의 근사). */
  private async refreshTracked(): Promise<void> {
    const today = kstToday();
    for (const [id, t] of this.tracked) if (t.day !== today) this.tracked.delete(id); // DAY 주문 소멸

    const open = [...this.tracked.values()].filter((t) => isOpenStatus(t.order.status));
    if (open.length === 0) return;

    const holdings = new Map((await this.getHoldings()).map((h) => [h.symbol, h.quantity]));
    for (const t of open) {
      const current = holdings.get(t.order.symbol!) ?? new Decimal(0);
      const qty = t.order.quantity ?? new Decimal(0);
      const filled = t.order.side === "BUY"
        ? current.gte(t.baselineQty.plus(qty))
        : current.lte(t.baselineQty.minus(qty));
      if (filled) {
        this.tracked.set(t.order.orderId,
          { ...t, order: { ...t.order, status: "FILLED", filledQuantity: qty } });
      }
    }
  }

  private async holdingQty(symbol: string): Promise<Decimal> {
    return (await this.getHoldings()).find((h) => h.symbol === symbol)?.quantity ?? new Decimal(0);
  }

  private balance(): Promise<Record<string, unknown>> {
    return this.call("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", this.tr("TTC8434R"),
      { query: { ...this.acct(), AFHR_FLPR_YN: "N", OFL_YN: "", INQR_DVSN: "02", UNPR_DVSN: "01",
                 FUND_STTL_ICLD_YN: "N", FNCG_AMT_AUTO_RDPT_YN: "N", PRCS_DVSN: "00",
                 CTX_AREA_FK100: "", CTX_AREA_NK100: "" } });
  }

  private acct(): Record<string, string> {
    return { CANO: this.cano, ACNT_PRDT_CD: this.acntPrdtCd };
  }

  private async call(
    method: string, path: string, trId: string,
    opts: { query?: Record<string, string>; json?: Record<string, string> } = {},
  ): Promise<Record<string, unknown>> {
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.callOnce(method, path, trId, opts);
      } catch (e) {
        if (e instanceof RateLimitError && attempt < 3) {
          await sleep(1000 * (attempt + 1)); // 초당 요청 제한 백오프
          continue;
        }
        throw e;
      }
    }
  }

  private async callOnce(
    method: string, path: string, trId: string,
    opts: { query?: Record<string, string>; json?: Record<string, string> },
  ): Promise<Record<string, unknown>> {
    await this.throttle.wait();
    let url = this.baseUrl + path;
    if (opts.query) url += "?" + new URLSearchParams(opts.query).toString();
    const [status, body] = await httpJson(url, {
      method,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        authorization: `Bearer ${await this.getToken()}`,
        appkey: this.appkey, appsecret: this.appsecret, tr_id: trId, custtype: "P",
      },
      body: opts.json ? JSON.stringify(opts.json) : undefined,
    });
    if (status < 200 || status >= 300 || body.rt_cd !== "0") {
      const code = (body.msg_cd as string) ?? null;
      const msg = `KIS(${trId}) ${body.msg1 ?? ""}`.trim();
      if (code === "EGW00201") throw new RateLimitError(status, code, msg);
      if (msg.includes("장종료") || msg.includes("장운영일이 아닙")) throw new MarketClosedError(status, code, msg);
      if (status === 401) throw new AuthError(status, code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return body;
  }

  private async getToken(): Promise<string> {
    if (this.token && Date.now() < this.tokenExpiresAt - 300_000) return this.token;
    await this.throttle.wait();
    const [status, body] = await httpJson(`${this.baseUrl}/oauth2/tokenP`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grant_type: "client_credentials", appkey: this.appkey, appsecret: this.appsecret }),
    });
    if (status !== 200 || typeof body.access_token !== "string") {
      throw new AuthError(status, (body.error_code as string) ?? null,
        `KIS 토큰 발급 실패: ${body.error_description ?? ""} (발급은 1분당 1회 제한)`);
    }
    this.token = body.access_token;
    this.tokenExpiresAt = Date.now() + Number(body.expires_in ?? 86400) * 1000;
    return this.token;
  }
}

function kstDate(yyyymmdd: string): Date {
  return new Date(`${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`);
}
