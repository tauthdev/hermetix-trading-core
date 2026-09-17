/**
 * 브로커 팩토리 — `hermetix.next({...})` 규약 (docs/broker-factory.md).
 *
 * ccxt 의 `new ccxt.binance({...})` 처럼 브로커 ID 한 토큰만 바꾸면 증권사가 바뀐다. 네 언어가 같은 자격 증명 모양을 쓴다.
 * 팩토리는 통일된 자격 증명을 각 클라이언트의 기존 생성자 인자로 **매핑만** 한다 — 인증·환경 결정·쓰로틀 기본값은 생성자 로직 그대로.
 * 생략한 인자는 `undefined` 로 넘겨 생성자 기본값이 살아 있게 한다.
 */
import type { BrokerClient } from "./broker.js";
import type { TradingEnvironment } from "./models.js";
import { DbClient } from "./brokers/db.js";
import { KbClient } from "./brokers/kb.js";
import { KisClient } from "./brokers/kis.js";
import { KiwoomClient } from "./brokers/kiwoom.js";
import { LsClient } from "./brokers/ls.js";
import { NextClient } from "./brokers/next.js";
import { NhClient } from "./brokers/nh.js";
import { TossClient } from "./brokers/toss.js";

/** 모든 브로커가 같은 모양으로 받는 자격 증명. 브로커별 선택 항목은 규약 표의 snake_case 키로 함께 넘긴다 (예: `hts_id`) */
export interface Credentials {
  apiKey: string;
  apiSecret: string;
  account?: string;
  environment?: TradingEnvironment;
  [extra: string]: unknown;
}

/** 브로커 ID — `BrokerCapabilities.brokerId` 와 같고, 목록 순서도 규약대로 */
export const brokers = ["next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb"] as const;
export type BrokerId = (typeof brokers)[number];

const COMMON_EXTRA = ["base_url", "ws_url", "throttle_seconds"] as const;
const EXTRA_KEYS: Record<BrokerId, readonly string[]> = {
  next: ["base_url"],
  kis: [...COMMON_EXTRA, "acnt_prdt_cd", "hts_id"],
  kiwoom: [...COMMON_EXTRA],
  nh: [...COMMON_EXTRA, "auth_url", "market_cd", "order_market_cd"],
  ls: [...COMMON_EXTRA, "mac_address", "exch_gubun", "chart_throttle_seconds"],
  db: [...COMMON_EXTRA, "mac_address", "market_div_code"],
  toss: [...COMMON_EXTRA],
  kb: ["base_url", "throttle_seconds", "excg_clsf", "sor_order_ccd", "chart_market_clsf"],
};
const BASE_KEYS = new Set(["apiKey", "apiSecret", "account", "environment"]);

type Extra = Record<string, unknown>;

/** 규약 검증: 빈 키/시크릿, 모르는 extra 키(오타를 조용히 무시하지 않는다), 필요한 브로커의 빈 account */
function validate(id: BrokerId, creds: Credentials, accountRequired = false): Extra {
  if (!creds || typeof creds !== "object") throw new Error(`hermetix.${id}: 자격 증명 객체가 필요합니다`);
  if (!creds.apiKey) throw new Error(`hermetix.${id}: apiKey 가 비어 있습니다`);
  if (!creds.apiSecret) throw new Error(`hermetix.${id}: apiSecret 이 비어 있습니다`);
  if (accountRequired && !creds.account) throw new Error(`hermetix.${id}: account 가 필요합니다 (${id === "kis" ? "cano, 계좌번호 앞 8자리" : "계좌 식별자"})`);
  if (creds.environment !== undefined && creds.environment !== "PAPER" && creds.environment !== "LIVE") {
    throw new Error(`hermetix.${id}: environment 는 PAPER 또는 LIVE 여야 합니다: ${String(creds.environment)}`);
  }
  const allowed = EXTRA_KEYS[id];
  const extra: Extra = {};
  for (const key of Object.keys(creds)) {
    if (BASE_KEYS.has(key)) continue;
    if (!allowed.includes(key)) throw new Error(`hermetix.${id}: 모르는 항목 '${key}' — 허용: ${allowed.join(", ") || "(없음)"}`);
    if (creds[key] !== undefined) extra[key] = creds[key];
  }
  return extra;
}

const str = (extra: Extra, key: string): string | undefined => (extra[key] === undefined ? undefined : String(extra[key]));
/** `throttle_seconds`(초) → 생성자의 ms 인자 */
const ms = (extra: Extra, key: string): number | undefined => {
  if (extra[key] === undefined) return undefined;
  const n = Number(extra[key]);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${key} 는 0 이상의 초 단위 숫자여야 합니다: ${String(extra[key])}`);
  return Math.round(n * 1000);
};

export function next(creds: Credentials): NextClient {
  const e = validate("next", creds);
  return new NextClient(creds.apiKey, creds.apiSecret, creds.account ?? undefined, str(e, "base_url"), creds.environment);
}

export function kis(creds: Credentials): KisClient {
  const e = validate("kis", creds, true);
  return new KisClient(creds.apiKey, creds.apiSecret, creds.account as string, str(e, "acnt_prdt_cd"), str(e, "base_url"), ms(e, "throttle_seconds"), creds.environment, str(e, "ws_url"), str(e, "hts_id"));
}

export function kiwoom(creds: Credentials): KiwoomClient {
  const e = validate("kiwoom", creds);
  return new KiwoomClient(creds.apiKey, creds.apiSecret, str(e, "base_url"), ms(e, "throttle_seconds"), creds.environment, str(e, "ws_url"));
}

export function nh(creds: Credentials): NhClient {
  const e = validate("nh", creds);
  return new NhClient(creds.apiKey, creds.apiSecret, creds.account ?? undefined, str(e, "base_url"), str(e, "auth_url"), str(e, "market_cd"), str(e, "order_market_cd"), ms(e, "throttle_seconds"), creds.environment, str(e, "ws_url"));
}

export function ls(creds: Credentials): LsClient {
  const e = validate("ls", creds);
  return new LsClient(creds.apiKey, creds.apiSecret, str(e, "base_url"), str(e, "mac_address"), str(e, "exch_gubun"), ms(e, "throttle_seconds"), ms(e, "chart_throttle_seconds"), creds.environment, str(e, "ws_url"));
}

export function db(creds: Credentials): DbClient {
  const e = validate("db", creds);
  return new DbClient(creds.apiKey, creds.apiSecret, str(e, "base_url"), str(e, "mac_address"), str(e, "market_div_code"), ms(e, "throttle_seconds"), creds.environment, str(e, "ws_url"));
}

export function toss(creds: Credentials): TossClient {
  const e = validate("toss", creds);
  return new TossClient(creds.apiKey, creds.apiSecret, creds.account ?? undefined, str(e, "base_url"), ms(e, "throttle_seconds"), creds.environment, str(e, "ws_url"));
}

export function kb(creds: Credentials): KbClient {
  const e = validate("kb", creds);
  return new KbClient(creds.apiKey, creds.apiSecret, str(e, "base_url"), str(e, "excg_clsf"), str(e, "sor_order_ccd"), str(e, "chart_market_clsf"), ms(e, "throttle_seconds"), creds.environment);
}

const FACTORIES: Record<BrokerId, (creds: Credentials) => BrokerClient> = { next, kis, kiwoom, nh, ls, db, toss, kb };

/** 브로커 ID(문자열, 대소문자 무관)로 만든다 — 설정 파일의 값으로 증권사를 고를 때. 모르는 ID 는 Error */
export function client(id: string, creds: Credentials): BrokerClient {
  const key = String(id).trim().toLowerCase() as BrokerId;
  const factory = FACTORIES[key];
  if (!factory) throw new Error(`hermetix.client: 모르는 브로커 '${id}' — 지원: ${brokers.join(", ")}`);
  return factory(creds);
}

/** `import hermetix from "hermetix"; hermetix.next({...})` 용 */
export const hermetix = Object.freeze({ next, kis, kiwoom, nh, ls, db, toss, kb, client, brokers });
