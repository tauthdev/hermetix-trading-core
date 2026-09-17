# 브로커 팩토리 — `hermetix.next(...)` 규약 (0.11.0)

ccxt 의 `new ccxt.binance({...})` 처럼 **브로커 ID 한 토큰만 바꾸면 증권사가 바뀌는** 생성 규약입니다. 네 언어(Kotlin·Python·JS·Go)가 같은 이름·같은 자격 증명 모양을 씁니다. 기존 클래스(`KisClient`, `KisApiClient`, `NewKisClient` …)는 그대로 남고, 팩토리는 그 위의 얇은 층입니다.

## 자격 증명 (Credentials) — 모든 브로커가 같은 모양

| 키 (언어별 표기) | 의미 | 브로커별 대응 |
|---|---|---|
| `api_key` / `apiKey` / `APIKey` | API 키 | next `client_id` · kis `appkey` · kiwoom `appkey` · nh/db/ls/kb `app_key` · toss `client_id` |
| `api_secret` / `apiSecret` / `APISecret` | API 시크릿 | next `client_secret` · kis `appsecret` · kiwoom `secretkey` · nh/db/ls/kb `app_secret` · toss `client_secret` |
| `account` | 계좌 식별자 | next `account_id`(기본 `acc_main`) · kis `cano` · nh `account_no` · toss `account_seq` · kiwoom/db/ls/kb 는 쓰지 않음(무시) |
| `environment` | `PAPER` \| `LIVE` | 생략 시 브로커 기본값 (next/kis/kiwoom/nh/db/ls = PAPER, toss/kb = LIVE — 기존 생성자 기본값과 동일) |
| `extra` | 브로커별 선택 항목 (문자열 맵) | 아래 표 |

`extra` 의 키는 **각 언어의 기존 생성자 인자 이름을 snake_case 로** 쓴다 (언어가 달라도 같은 키):

| 브로커 | extra 키 |
|---|---|
| 공통 | `base_url`, `ws_url`, `throttle_seconds` (Go/JS/Kotlin 은 ms 인자로 변환) |
| kis | `acnt_prdt_cd`(기본 01), `custtype`(기본 P), `hts_id` |
| nh | `auth_url`, `market_cd`, `order_market_cd` |
| db | `mac_address`, `market_div_code` |
| ls | `mac_address`, `exch_gubun`, `chart_throttle_seconds` |
| kb | `excg_clsf`, `sor_order_ccd`, `chart_market_clsf` |
| toss | (없음) |

모르는 extra 키는 **에러**로 거부한다 (오타를 조용히 무시하지 않는다). `api_key`·`api_secret` 이 비면 에러. `account` 가 필요한 브로커(kis)에서 비면 에러.

언어별로 기존 생성자가 받지 않는 항목은 그 언어에서 "지원하지 않는 extra" 에러다 (2026-09-17 기준):

| 항목 | 지원 언어 |
|---|---|
| kis `custtype` | Python · Kotlin (JS · Go 는 클라이언트에 해당 설정 없음) |
| next `ws_url`, `throttle_seconds` | 없음 (넥스트는 웹소켓·쓰로틀 설정이 없다 — `base_url` 만) |
| kb `ws_url` | 없음 (KB 는 웹소켓이 없다) |

`client(id)` 의 ID 는 대소문자를 가리지 않는다. `account` 를 비운 next 는 네 언어 모두 `acc_main` 이 된다.

## 팩토리 표면

| 언어 | 브로커별 | ID 로 | 목록 |
|---|---|---|---|
| Python | `hermetix.next(api_key=…, api_secret=…, account=…, environment=…, **extra)` | `hermetix.client("next", …)` | `hermetix.brokers` (tuple) |
| JS/TS | `hermetix.next({ apiKey, apiSecret, account, environment, ...extra })` — default export 객체와 named export 둘 다 | `client("next", creds)` | `brokers` (readonly array) |
| Go | `hermetix.Next(hermetix.Credentials{…})` | `hermetix.Client("next", creds) (BrokerClient, error)` | `hermetix.Brokers` (slice) |
| Kotlin | `Hermetix.next(Credentials(…))` | `Hermetix.client("next", creds)` | `Hermetix.brokers` (List) |

- 브로커 ID 와 목록 순서: `next`, `kis`, `kiwoom`, `nh`, `ls`, `db`, `toss`, `kb` (`BrokerCapabilities.brokerId` 와 같다)
- 반환 타입은 그 브로커의 구체 클래스(Python `NextClient`, Go `*NextClient` …). `client(id)` 는 공통 인터페이스(`BrokerClient`)를 돌려주고, 모르는 ID 는 에러(Python `ValueError`, JS `Error`, Go `error`, Kotlin `IllegalArgumentException`)
- 팩토리는 자격 증명을 기존 생성자 인자로 **매핑만** 한다. 인증·환경 결정·쓰로틀 기본값은 기존 생성자 로직 그대로
- Go 의 `Credentials.Extra` 는 `map[string]string`, Kotlin 은 `Map<String, String>`, JS 는 객체 나머지 필드, Python 은 `**extra`

## 예시 (네 언어가 같은 모양)

```python
import hermetix
client = hermetix.next(api_key="pk_test_…", api_secret="sk_test_…", account="acc_main")
```
```ts
import hermetix from "hermetix";
const client = hermetix.next({ apiKey: "pk_test_…", apiSecret: "sk_test_…", account: "acc_main" });
```
```go
client, err := hermetix.Next(hermetix.Credentials{APIKey: "pk_test_…", APISecret: "sk_test_…", Account: "acc_main"})
```
```kotlin
val client = Hermetix.next(Credentials(apiKey = "pk_test_…", apiSecret = "sk_test_…", account = "acc_main"))
```

증권사를 바꾸면 `next` 만 `kis` 로 바뀐다. 그 브로커에 필요한 `account`·`extra` 는 위 표대로.
