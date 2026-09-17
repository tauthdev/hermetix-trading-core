/** 브로커 팩토리 — docs/broker-factory.md 규약: 통일된 자격 증명이 각 생성자 인자로 정확히 매핑되고, 잘못된 입력은 Error. 네트워크 없음 */
import assert from "node:assert/strict";
import { test } from "node:test";
import hermetix, {
  DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, TossClient, brokers, client, db, kb, kis, kiwoom, ls, next, nh, toss,
} from "../src/index.js";

// private 필드는 매핑 검증용으로만 들여다본다
const priv = (c: object): Record<string, unknown> => c as unknown as Record<string, unknown>;
const base = { apiKey: "KEY", apiSecret: "SECRET" };

test("brokers 목록과 default export", () => {
  assert.deepEqual([...brokers], ["next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb"]);
  assert.equal(hermetix.next, next);
  assert.equal(hermetix.client, client);
  assert.deepEqual([...hermetix.brokers], [...brokers]);
});

test("next — client_id/client_secret/account_id, 기본값 유지", () => {
  const c = next({ ...base, account: "acc_1" });
  assert.ok(c instanceof NextClient);
  assert.equal(priv(c).clientId, "KEY");
  assert.equal(priv(c).clientSecret, "SECRET");
  assert.equal(priv(c).accountId, "acc_1");
  assert.equal(priv(c).baseUrl, "https://openapi.nextsecurities.dev");
  assert.equal(c.environment, "PAPER");
  assert.equal(priv(next(base)).accountId, "acc_main"); // account 생략 → 생성자 기본값
  assert.equal(priv(next({ ...base, base_url: "http://localhost:1" })).baseUrl, "http://localhost:1");
});

test("kis — appkey/appsecret/cano + extra(acnt_prdt_cd, hts_id, throttle_seconds→ms)", () => {
  const c = kis({ ...base, account: "12345678", environment: "LIVE", acnt_prdt_cd: "02", hts_id: "HTS", throttle_seconds: 0.25, ws_url: "ws://x", base_url: "http://b" });
  assert.ok(c instanceof KisClient);
  assert.equal(priv(c).appkey, "KEY");
  assert.equal(priv(c).appsecret, "SECRET");
  assert.equal(priv(c).cano, "12345678");
  assert.equal(priv(c).acntPrdtCd, "02");
  assert.equal(c.htsId, "HTS");
  assert.equal(c.environment, "LIVE");
  assert.equal(c.baseUrl, "http://b");
  assert.equal(priv(kis({ ...base, account: "1" })).acntPrdtCd, "01"); // 기본값
  assert.throws(() => kis(base), /account/);
});

test("kiwoom — appkey/secretkey", () => {
  const c = kiwoom({ ...base, environment: "LIVE" });
  assert.ok(c instanceof KiwoomClient);
  assert.equal(priv(c).appkey, "KEY");
  assert.equal(priv(c).secretkey, "SECRET");
  assert.equal(c.environment, "LIVE");
  assert.equal(kiwoom(base).environment, "PAPER");
});

test("nh — app_key/app_secret/account_no + market_cd/order_market_cd/auth_url", () => {
  const c = nh({ ...base, account: "ACC", market_cd: "NXT", order_market_cd: "KRX", auth_url: "https://auth" });
  assert.ok(c instanceof NhClient);
  assert.equal(priv(c).appKey, "KEY");
  assert.equal(priv(c).appSecret, "SECRET");
  assert.equal(priv(c).accountNo, "ACC");
  assert.equal(priv(c).marketCd, "NXT");
  assert.equal(priv(c).orderMarketCd, "KRX");
  assert.equal(c.authUrl, "https://auth");
  assert.equal(priv(nh(base)).accountNo, ""); // 비우면 첫 계좌 자동 선택 (생성자 기본값)
});

test("ls — mac_address/exch_gubun/chart_throttle_seconds", () => {
  const c = ls({ ...base, mac_address: "00:11", exch_gubun: "K", chart_throttle_seconds: 2 });
  assert.ok(c instanceof LsClient);
  assert.equal(priv(c).appKey, "KEY");
  assert.equal(priv(c).macAddress, "00:11");
  assert.equal(priv(c).exchGubun, "K");
  assert.equal(c.baseUrl, "https://openapi.ls-sec.co.kr:8080");
});

test("db — mac_address/market_div_code", () => {
  const c = db({ ...base, mac_address: "aa", market_div_code: "N" });
  assert.ok(c instanceof DbClient);
  assert.equal(priv(c).appSecret, "SECRET");
  assert.equal(priv(c).macAddress, "aa");
  assert.equal(priv(c).marketDivCode, "N");
  assert.equal(c.environment, "PAPER");
});

test("toss — client_id/client_secret/account_seq, 기본 LIVE", () => {
  const c = toss({ ...base, account: "7" });
  assert.ok(c instanceof TossClient);
  assert.equal(priv(c).clientId, "KEY");
  assert.equal(priv(c).accountSeq, "7");
  assert.equal(c.environment, "LIVE");
  assert.equal(c.baseUrl, "https://openapi.tossinvest.com");
  assert.equal(priv(toss(base)).accountSeq, "");
});

test("kb — excg_clsf/sor_order_ccd/chart_market_clsf, 기본 LIVE", () => {
  const c = kb({ ...base, excg_clsf: "2", sor_order_ccd: "S", chart_market_clsf: "1" });
  assert.ok(c instanceof KbClient);
  assert.equal(priv(c).appKey, "KEY");
  assert.equal(priv(c).excgClsf, "2");
  assert.equal(priv(c).sorOrderCcd, "S");
  assert.equal(priv(c).chartMarketClsf, "1");
  assert.equal(c.environment, "LIVE");
});

test("client(id) 는 ID 로 고르고 공통 인터페이스를 돌려준다, 모르는 ID 는 Error", () => {
  const c = client("KIS", { ...base, account: "1" });
  assert.ok(c instanceof KisClient);
  assert.equal(c.capabilities.brokerId, "kis");
  assert.ok(client("next", base) instanceof NextClient);
  assert.throws(() => client("binance", base), /모르는 브로커 'binance'/);
  for (const id of brokers) assert.ok(client(id, { ...base, account: "1" }));
});

test("잘못된 입력은 조용히 넘어가지 않는다", () => {
  assert.throws(() => next({ apiKey: "", apiSecret: "S" }), /apiKey/);
  assert.throws(() => next({ apiKey: "K", apiSecret: "" }), /apiSecret/);
  assert.throws(() => next({ ...base, hts_id: "x" }), /모르는 항목 'hts_id'/); // next 에 없는 extra
  assert.throws(() => kiwoom({ ...base, custtype: "P" }), /모르는 항목 'custtype'/);
  assert.throws(() => kis({ ...base, account: "1", throttle_seconds: -1 }), /throttle_seconds/);
  assert.throws(() => kb({ ...base, environment: "SANDBOX" as never }), /environment/);
  assert.throws(() => next(undefined as never), /자격 증명/);
});
