/**
 * 어댑터 컨포먼스 검증 — 모든 BrokerClient 구현이 지켜야 하는 공통 모델 규약을 한 시나리오로 확인한다.
 * 시세 → 캔들 → 캘린더 → 계좌 → 보유 → 매수가능 → 주문 → 조회 → 취소 → 체결 순으로 호출하고 위반을 모은다.
 * 상태 전이(취소 후 재조회)는 검사하지 않는다 — 정적 골든 픽스처로 재생 가능해야 하기 때문이다.
 * 새 어댑터 기여 조건은 `violations` 가 비어 있는 것이다 (conformance/README.md). Kotlin BrokerConformance 와 같은 검사 항목.
 */
import { Decimal } from "decimal.js";
import type { BrokerClient } from "./broker.js";
import { isOpenStatus, parseSymbol, symbolsMatch } from "./models.js";

export interface ConformanceScenario { symbol: string; limitPrice: Decimal; quantity?: Decimal; }

export interface ConformanceReport { steps: string[]; violations: string[]; passed: boolean; }

const HHMM = /^\d{2}:\d{2}$/;
const TEN = new Decimal(10);

export async function verifyBrokerConformance(broker: BrokerClient, scenario: ConformanceScenario): Promise<ConformanceReport> {
  const steps: string[] = [];
  const violations: string[] = [];
  const check = (cond: boolean, message: string) => { if (!cond) violations.push(message); };
  const step = async (name: string, fn: () => Promise<void>) => {
    steps.push(name);
    try { await fn(); } catch (e) { violations.push(`${name}: 예외 ${(e as Error).name}: ${(e as Error).message}`); }
  };
  const caps = broker.capabilities;
  const markets = caps.markets ?? new Set([caps.market]);
  const environments = caps.environments ?? new Set(["PAPER"]);
  const quantity = scenario.quantity ?? new Decimal(1);

  await step("capabilities", async () => {
    check(caps.brokerId.length > 0, "capabilities.brokerId 가 비어 있다");
    check(caps.currency.length > 0, "capabilities.currency 가 비어 있다");
    check(markets.has(caps.market), `capabilities.market(${caps.market}) 이 markets 에 없다`);
    check(environments.has(broker.environment ?? "PAPER"), `environment(${broker.environment}) 이 선언된 environments 에 없다`);
    check(caps.candleIntervals.size > 0, "candleIntervals 가 비어 있다");
  });

  await step("quotes", async () => {
    const quotes = await broker.getQuotes([scenario.symbol]);
    check(quotes.length === 1, `quotes: 1건을 기대했는데 ${quotes.length}건`);
    const q = quotes[0];
    if (q) {
      check(q.symbol === scenario.symbol, `quotes: 심볼은 요청 표기 그대로여야 한다 (요청=${scenario.symbol}, 응답=${q.symbol})`);
      check(q.price.gt(0), `quotes: price 는 양수여야 한다 (${q.price})`);
      check(!Number.isNaN(q.timestamp.getTime()) && q.timestamp.getTime() > 0, "quotes: timestamp 가 없다");
      check(q.volume >= 0, `quotes: volume 음수 (${q.volume})`);
      if (q.changeRate) check(q.changeRate.abs().lte(TEN), `quotes: changeRate 는 비율이어야 한다 (% 로 보임: ${q.changeRate})`);
    }
  });

  await step("candles", async () => {
    const interval = [...caps.candleIntervals].sort()[0];
    const candles = await broker.getCandles(scenario.symbol, interval, 3);
    check(candles.length > 0, "candles: 비어 있다");
    check(candles.length <= 3, `candles: limit=3 을 넘겼다 (${candles.length})`);
    check(candles.every((c, i) => i === 0 || candles[i - 1].timestamp < c.timestamp), "candles: 시각이 오름차순이 아니다");
    for (const c of candles) {
      check(c.low.lte(c.high) && c.open.gte(c.low) && c.open.lte(c.high) && c.close.gte(c.low) && c.close.lte(c.high),
        `candles: OHLC 범위 위반 ${c.timestamp.toISOString()} o=${c.open} h=${c.high} l=${c.low} c=${c.close}`);
      check(c.volume >= 0, "candles: volume 음수");
    }
  });

  await step("calendar", async () => {
    const days = await broker.getCalendar();
    check(days.length > 0, "calendar: 비어 있다");
    for (const d of days) {
      check(/^\d{4}-\d{2}-\d{2}$/.test(d.date), `calendar: 날짜 형식 위반 ${d.date}`);
      try { new Intl.DateTimeFormat("en-US", { timeZone: d.timezone }); } catch { violations.push(`calendar: 타임존 위반 ${d.timezone}`); }
      if (d.open) {
        check(d.regular !== null, `calendar: 개장일 ${d.date} 에 정규장 세션이 없다`);
        if (d.regular) check(HHMM.test(d.regular.start) && HHMM.test(d.regular.end), `calendar: 세션 시각은 HH:mm 이어야 한다 (${d.regular.start}~${d.regular.end})`);
      }
    }
  });

  await step("account", async () => {
    const a = await broker.getAccount();
    check(a.accountId.length > 0, "account: accountId 비어 있음");
    check(a.currency === caps.currency, `account: currency(${a.currency}) 가 capabilities.currency(${caps.currency}) 와 다르다`);
    check(a.cash.gte(0), "account: cash 음수");
    check(a.portfolioValue.gte(0), "account: portfolioValue 음수");
  });

  await step("holdings", async () => {
    for (const h of await broker.getHoldings()) {
      check(h.symbol.length > 0, "holdings: 심볼 비어 있음");
      const { market } = parseSymbol(h.symbol);
      check(market === null || markets.has(market), `holdings: 미지원 시장 접두 ${h.symbol}`);
      check(h.quantity.gt(0), `holdings: quantity 는 양수여야 한다 (${h.symbol}=${h.quantity})`);
      check(h.avgEntryPrice.gte(0), `holdings: avgEntryPrice 음수 (${h.symbol})`);
      if (h.unrealizedPnlRate) check(h.unrealizedPnlRate.abs().lte(TEN), `holdings: unrealizedPnlRate 는 비율이어야 한다 (% 로 보임: ${h.symbol}=${h.unrealizedPnlRate})`);
    }
  });

  await step("buyingPower", async () => {
    check((await broker.getBuyingPower()).gte(0), "buyingPower 음수");
  });

  let orderId: string | null = null;
  await step("createOrder", async () => {
    const order = await broker.createOrder({
      symbol: scenario.symbol, side: "BUY", orderType: "LIMIT", quantity, limitPrice: scenario.limitPrice, clientOrderId: "conformance-1",
    });
    check(order.orderId.length > 0, "createOrder: orderId 비어 있음");
    check(isOpenStatus(order.status), `createOrder: 접수 직후 상태는 미체결(open)이어야 한다 (${order.status})`);
    if (order.symbol) check(symbolsMatch(order.symbol, scenario.symbol), `createOrder: 심볼 불일치 (${order.symbol})`);
    orderId = order.orderId || null;
  });

  if (orderId) {
    const id: string = orderId;
    await step("getOrder", async () => {
      const order = await broker.getOrder(id);
      check(order.orderId === id, `getOrder: orderId 불일치 (${order.orderId})`);
      check(order.status !== "UNKNOWN", "getOrder: status UNKNOWN");
    });
    await step("getOrders", async () => {
      const orders = await broker.getOrders();
      check(orders.some((o) => o.orderId === id), `getOrders: 방금 낸 주문 ${id} 가 목록에 없다`);
      for (const o of orders) check(o.status !== "UNKNOWN", `getOrders: status UNKNOWN (${o.orderId})`);
    });
    await step("cancelOrder", async () => {
      const canceled = await broker.cancelOrder(id);
      check(canceled.orderId === id, `cancelOrder: orderId 불일치 (${canceled.orderId})`);
      check(canceled.status === "PENDING_CANCEL" || canceled.status === "CANCELED", `cancelOrder: status 는 PENDING_CANCEL/CANCELED 이어야 한다 (${canceled.status})`);
    });
  }

  await step("fills", async () => {
    for (const f of await broker.getFills()) {
      if (f.quantity) check(f.quantity.gt(0), `fills: quantity 는 양수 (${f.orderId})`);
      if (f.price) check(f.price.gt(0), `fills: price 는 양수 (${f.orderId})`);
    }
  });

  return { steps, violations, passed: violations.length === 0 };
}
