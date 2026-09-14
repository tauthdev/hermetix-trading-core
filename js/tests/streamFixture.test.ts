/** 골든 픽스처의 stream 섹션 — 네 언어가 같은 프레임을 같은 TradeTick 으로 파싱하는지 (Kotlin StreamFixtureTest 대응). */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { parseKisFrame } from "../src/brokers/kisStream.js";
import { parseKiwoomReal } from "../src/brokers/kiwoomStream.js";
import type { TradeTick } from "../src/models.js";

const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);
const TODAY = "2026-09-14";

interface Expected {
  symbol: string; price: string; quantity: string; time: string; askPrice: string; bidPrice: string;
  cumulativeVolume: number; change: string; changeRate: string;
}

function stream(broker: string) {
  const fx = JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as {
    stream: { channel: string; frames: string[]; expected: Expected[] };
  };
  assert.ok(fx.stream, `${broker} 픽스처에 stream 섹션이 없다`);
  return fx.stream;
}

const kstTime = (d: Date) =>
  new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Seoul", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(d);

function assertMatches(ticks: TradeTick[], expected: Expected[]) {
  assert.equal(ticks.length, expected.length);
  ticks.forEach((tick, i) => {
    const e = expected[i];
    assert.equal(tick.symbol, e.symbol);
    assert.ok(tick.price.eq(e.price), `price ${tick.price} != ${e.price}`);
    assert.ok(tick.quantity.eq(e.quantity), `quantity ${tick.quantity} != ${e.quantity}`);
    assert.ok(tick.askPrice!.eq(e.askPrice), "askPrice");
    assert.ok(tick.bidPrice!.eq(e.bidPrice), "bidPrice");
    assert.equal(tick.cumulativeVolume, e.cumulativeVolume);
    assert.ok(tick.change!.eq(e.change), `change ${tick.change} != ${e.change}`);
    assert.ok(tick.changeRate!.eq(e.changeRate), `changeRate ${tick.changeRate} != ${e.changeRate}`);
    assert.equal(kstTime(tick.timestamp), e.time);
  });
}

test("kis - H0STCNT0 실측 프레임", () => {
  const s = stream("kis");
  assert.equal(s.channel, "TRADES");
  assertMatches(s.frames.flatMap((f) => parseKisFrame(f, TODAY)), s.expected);
});

test("kiwoom - REAL 0B 실측 프레임", () => {
  const s = stream("kiwoom");
  assert.equal(s.channel, "TRADES");
  assertMatches(s.frames.flatMap((f) => parseKiwoomReal(JSON.parse(f), TODAY)), s.expected);
});

test("kis 파서 - 다른 TR·필드 부족은 빈 목록, 여러 레코드는 폭으로 나눈다", () => {
  const rec = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000";
  assert.equal(parseKisFrame(`0|H0STASP0|001|${rec}`, TODAY).length, 0);
  assert.equal(parseKisFrame("0|H0STCNT0|001|005930^093012", TODAY).length, 0);
  const two = parseKisFrame(`0|H0STCNT0|002|${rec}^${rec}`, TODAY);
  assert.equal(two.length, 2);
  assert.ok(two[0].change!.eq(-300)); // 부호코드 5(하락) + 무부호 값 → 음수
});

test("kiwoom 파서 - 0B 외 타입 무시, A 프리픽스 제거", () => {
  const orderbook = { trnm: "REAL", data: [{ type: "0D", name: "주식호가잔량", item: "005930", values: { "21": "093012" } }] };
  assert.equal(parseKiwoomReal(orderbook, TODAY).length, 0);
  const real = { trnm: "REAL", data: [{ type: "0B", item: "A005930", values: { "20": "093012", "10": "-71500", "15": "-15", "13": "10" } }] };
  const tick = parseKiwoomReal(real, TODAY)[0];
  assert.equal(tick.symbol, "005930");
  assert.ok(tick.price.eq(71500));
  assert.ok(tick.quantity.eq(15));
});
