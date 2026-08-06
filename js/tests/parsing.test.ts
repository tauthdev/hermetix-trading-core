/** 어댑터 정규화 검증 - 실측 골든 픽스처 재생 (Kotlin/Python 과 동일 정답지). */
import assert from "node:assert/strict";
import { test } from "node:test";
import { Decimal, KisClient, KiwoomClient, NextClient, krxTickRound } from "../src/index.js";

const stub = (responses: Record<string, unknown>[]) => {
  const queue = [...responses];
  return async () => queue.shift()!;
};

test("next: quote 파싱 (2026-08-03 실측)", async () => {
  const client = new NextClient("k", "s");
  (client as any).request = stub([{
    quotes: [{ symbol: "AAPL", price: "308.91", bidPrice: null, askPrice: null,
               volume: 132756799, change: "-24.52", changeRate: "-0.073539",
               timestamp: "2026-07-31T04:00:00Z" }],
  }]);
  const [q] = await client.getQuotes(["AAPL"]);
  assert.equal(q.price.toString(), "308.91");
  assert.equal(q.bidPrice, null);
  assert.equal(q.changeRate!.toString(), "-0.073539");
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
