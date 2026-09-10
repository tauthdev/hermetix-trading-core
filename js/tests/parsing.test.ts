/** 어댑터 정규화 검증 - 실측 골든 픽스처 재생 (Kotlin/Python 과 동일 정답지). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { AuthError, Decimal, KisClient, KiwoomClient, NextClient, isOpenStatus, krxTickRound } from "../src/index.js";

const stub = (responses: Record<string, unknown>[]) => {
  const queue = [...responses];
  return async () => queue.shift()!;
};

test("next: quote 파싱 (v1.3 — outcome=OK 만, 등락률 %→비율, KST 시각)", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    quotes: [
      { symbol: "AAPL", outcome: "OK", session: "REGULAR", requestedAt: "2026-09-10T23:10:00+09:00",
        price: "308.91", previousClose: "333.43", change: "-24.52", changeRate: "-7.3539",
        bidPrice: null, bidSize: null, askPrice: null, askSize: null,
        volume: "132756799", lastTradeAt: "2026-09-10T23:09:58+09:00" },
      { symbol: "NOPE", outcome: "NOT_FOUND", session: "CLOSED", requestedAt: "2026-09-10T23:10:00+09:00" },
    ],
  }]);
  const quotes = await client.getQuotes(["AAPL", "NOPE"]);
  assert.deepEqual(quotes.map((q) => q.symbol), ["AAPL"]);
  const [q] = quotes;
  assert.equal(q.price.toString(), "308.91");
  assert.equal(q.bidPrice, null);
  assert.equal(q.changeRate!.toString(), "-0.073539");
  assert.equal(q.volume, 132756799);
  assert.equal(q.timestamp.toISOString(), "2026-09-10T14:09:58.000Z");
});

test("next: 캔들 time 에 오프셋이 없으면 KST 로 해석", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    symbol: "AAPL", interval: "1d", nextCursor: null,
    candles: [{ time: "2026-09-09T22:30:00", session: "REGULAR", open: "339.73", high: "344.56",
                low: "337.35", close: "338.19", volume: "56298904" }],
  }]);
  const [c] = await client.getCandles("AAPL", "1d", 3);
  assert.equal(c.timestamp.toISOString(), "2026-09-09T13:30:00.000Z");
  assert.equal(c.volume, 56298904);
});

test("next: 캘린더 KST 세션 → 뉴욕 현지 HH:mm, status → open", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    calendar: [
      { date: "2026-09-10", status: "OPEN", holidayName: null, sessions: [
        { type: "PRE", open: "2026-09-10T17:00:00+09:00", close: "2026-09-10T22:30:00+09:00" },
        { type: "REGULAR", open: "2026-09-10T22:30:00+09:00", close: "2026-09-11T05:00:00+09:00" }] },
      { date: "2026-11-27", status: "HALF_DAY", holidayName: "Day After Thanksgiving", sessions: [
        { type: "REGULAR", open: "2026-11-27T23:30:00+09:00", close: "2026-11-28T03:00:00+09:00" }] },
      { date: "2026-12-25", status: "CLOSED", holidayName: "Christmas", sessions: [] },
    ],
  }]);
  const days = await client.getCalendar();
  assert.equal(days[0].open, true);
  assert.equal(days[0].timezone, "America/New_York");
  assert.deepEqual(days[0].regular, { start: "09:30", end: "16:00" });
  assert.equal(days[1].regular!.end, "13:00"); // 반장일 (EST)
  assert.equal(days[1].holiday, "Day After Thanksgiving");
  assert.equal(days[2].open, false);
  assert.equal(days[2].regular, null);
});

test("next: 계좌 총평가 = cashAmount + 보유 평가금액, 손익률 %→비율", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([
    { accountId: "acc_main", currency: "USD", cashAmount: "1000.50", requestedAt: "2026-09-10T23:10:00+09:00" },
    { currency: "USD", holdings: [
      { symbol: "AAPL", name: "Apple Inc.", quantity: "2", sellableQuantity: "2", averageBuyPrice: "300.00",
        currentPrice: "310.00", purchaseAmount: "600.00", evaluationAmount: "620.00",
        evaluationPnl: "20.00", evaluationPnlRate: "3.3333" }], requestedAt: "2026-09-10T23:10:00+09:00" },
  ]);
  const account = await client.getAccount();
  assert.equal(account.cash.toString(), "1000.5");
  assert.equal(account.portfolioValue.toString(), "1620.5");
  (client as any).request = stub([
    { currency: "USD", holdings: [
      { symbol: "AAPL", name: "Apple Inc.", quantity: "2", averageBuyPrice: "300.00", evaluationAmount: "620.00",
        evaluationPnl: "20.00", evaluationPnlRate: "3.3333" }] },
  ]);
  const [h] = await client.getHoldings();
  assert.equal(h.avgEntryPrice.toString(), "300");
  assert.equal(h.marketValue!.toString(), "620");
  assert.equal(h.unrealizedPnlRate!.toString(), "0.033333");
});

test("next: 주문 상세 requestId → clientOrderId, 생성 본문에 market·clientOrderId", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    orderId: "ord_1", requestId: "s-1", market: "US", symbol: "AAPL", side: "BUY", orderType: "LIMIT",
    timeInForce: "DAY", status: "PARTIALLY_FILLED", quantity: "10", filledQuantity: "4", limitPrice: "200",
    avgFillPrice: "199.5", requestedAt: "2026-09-10T23:10:00+09:00", updatedAt: "2026-09-10T23:12:00+09:00",
  }]);
  const order = await client.getOrder("ord_1");
  assert.equal(order.clientOrderId, "s-1");
  assert.equal(order.status, "PARTIALLY_FILLED");
  assert.equal(order.orderType, "LIMIT");
  assert.equal(order.submittedAt!.toISOString(), "2026-09-10T14:10:00.000Z");

  const sent: Record<string, unknown>[] = [];
  (client as any).request = async (_m: string, _p: string, opts: { json?: Record<string, unknown> }) => {
    sent.push(opts.json!);
    return { orderId: "ord_2", market: "US", status: "SUBMITTED", requestedAt: "2026-09-10T23:10:00+09:00" };
  };
  await client.createOrder({ symbol: "AAPL", side: "BUY", orderType: "LIMIT", quantity: new Decimal(1),
                             limitPrice: new Decimal(200), clientOrderId: "s-2" });
  assert.equal(sent[0].market, "US");
  assert.equal(sent[0].clientOrderId, "s-2");
  assert.equal(sent[0].limitPrice, "200");
});

test("next: PENDING_CANCEL 은 open 으로 분류 (v1.3 부록 D)", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    orderId: "ord_1", status: "PENDING_CANCEL", symbol: "AAPL", side: "BUY",
    orderType: "LIMIT", quantity: "1", limitPrice: "200", filledQuantity: "0",
  }]);
  const order = await client.cancelOrder("ord_1");
  assert.equal(order.status, "PENDING_CANCEL");
  assert.equal(isOpenStatus(order.status), true);
});

test("next: v1.3 공통 헤더 (X-Request-Id, X-Next-Account-Id)", async () => {
  const seen: { url: string; headers: Record<string, string> }[] = [];
  const restore = (globalThis as any).fetch;
  (globalThis as any).fetch = async (url: string, init: { headers: Record<string, string> }) => {
    seen.push({ url, headers: init.headers });
    const body = url.endsWith("/v1/oauth/token")
      ? { access_token: "tok", token_type: "Bearer", expires_in: 43200 }
      : url.endsWith("/v1/account/holdings")
        ? { currency: "USD", holdings: [] }
        : { accountId: "acc_main", currency: "USD", cashAmount: "1" };
    return { status: 200, text: async () => JSON.stringify(body) };
  };
  try {
    const client = new NextClient("k", "s", "acc_main", "http://next.test");
    await client.getAccount();
  } finally {
    (globalThis as any).fetch = restore;
  }
  const [, account] = seen;
  assert.equal(account.headers["X-Next-Account-Id"], "acc_main");
  assert.equal(account.headers["X-Nextsecurities-Account"], undefined);
  assert.match(account.headers["X-Request-Id"], /^[A-Za-z0-9._-]{1,64}$/);
});

test("next: 토큰 발급 실패는 OAuth 표준 에러 형식", async () => {
  const restore = (globalThis as any).fetch;
  (globalThis as any).fetch = async () => ({
    status: 401, text: async () => JSON.stringify({ error: "invalid_client", error_description: "인증 실패" }),
  });
  try {
    const client = new NextClient("k", "s", "acc_main", "http://next.test");
    await assert.rejects(client.getQuotes(["AAPL"]), (e: any) =>
      e instanceof AuthError && e.errorCode === "invalid_client" && String(e.message).includes("인증 실패"));
  } finally {
    (globalThis as any).fetch = restore;
  }
});

test("kis: %단위 등락률 -> 비율", async () => {
  const client = new KisClient("k", "s", "50199202");
  (client as any).call = stub([{
    output: { stck_prpr: "239500", acml_vol: "27393580", prdy_vrss: "-23000", prdy_ctrt: "-8.76" },
  }]);
  const [q] = await client.getQuotes(["005930"]);
  assert.equal(q.price.toString(), "239500");
  assert.equal(q.changeRate!.toString(), "-0.0876");
});

test("kis: 일봉 최신순 -> 과거→최신 정렬", async () => {
  const client = new KisClient("k", "s", "50199202");
  (client as any).call = stub([{
    output2: [
      { stck_bsop_date: "20260731", stck_oprc: "304", stck_hgpr: "310", stck_lwpr: "300", stck_clpr: "308", acml_vol: "1" },
      { stck_bsop_date: "20260730", stck_oprc: "333", stck_hgpr: "334", stck_lwpr: "329", stck_clpr: "333", acml_vol: "2" },
    ],
  }]);
  const candles = await client.getCandles("005930", "1d", 2);
  assert.ok(candles[0].timestamp < candles[1].timestamp);
  assert.equal(candles[1].close.toString(), "308");
});

test("kis: 추적 주문의 체결을 보유수량 변화로 판정", async () => {
  const client = new KisClient("k", "s", "50199202");
  let qty = new Decimal(2);
  (client as any).getHoldings = async () => [{ symbol: "005930", quantity: qty }];
  (client as any).call = async () => ({ output: { ODNO: "0000035787" } });

  const order = await client.createOrder({
    symbol: "005930", side: "BUY", orderType: "LIMIT",
    quantity: new Decimal(1), limitPrice: new Decimal(185400),
  });
  assert.equal((await client.getOrder(order.orderId)).status, "SUBMITTED");
  qty = new Decimal(3); // 체결로 보유 증가
  assert.equal((await client.getOrder(order.orderId)).status, "FILLED");
});

test("kiwoom: 부호 접두/zero-padded 정규화", async () => {
  const client = new KiwoomClient("k", "s");
  (client as any).call = stub([
    { cur_prc: "-239500", trde_qty: "27393580", pred_pre: "-23000", flu_rt: "-8.76" },
  ]);
  const [q] = await client.getQuotes(["005930"]);
  assert.equal(q.price.toString(), "239500");     // 부호 제거
  assert.equal(q.change!.toString(), "-23000");   // 대비는 부호 유지
  assert.equal(q.changeRate!.toString(), "-0.0876");
});

test("kiwoom: 계좌 zero-padded 금액", async () => {
  const client = new KiwoomClient("k", "s");
  (client as any).call = stub([
    { entr: "000000100000000", ord_alow_amt: "000000100000000" },
    { prsm_dpst_aset_amt: "000000100000000", tot_evlt_amt: "000000000000000" },
  ]);
  const account = await client.getAccount();
  assert.equal(account.cash.toString(), "100000000");
});

test("kiwoom: 보유 A 프리픽스 제거", async () => {
  const client = new KiwoomClient("k", "s");
  (client as any).balance = stub([{
    acnt_evlt_remn_indv_tot: [
      { stk_cd: "A005930", rmnd_qty: "000000000001", pur_pric: "000000239500",
        cur_prc: "-240000", evlt_amt: "000000240000", evltv_prft: "500", prft_rt: "0.21" },
    ],
  }]);
  const [h] = await client.getHoldings();
  assert.equal(h.symbol, "005930");
  assert.equal(h.currentPrice!.toString(), "240000");
});

test("krx 호가단위 보정", () => {
  assert.equal(krxTickRound(new Decimal("246258.99")).toString(), "246000"); // 20만~50만: 500원
  assert.equal(krxTickRound(new Decimal("197650")).toString(), "197600");    // 5만~20만: 100원
  assert.equal(krxTickRound(new Decimal("1999")).toString(), "1999");        // ~2천: 1원
});
