# 어댑터 컨포먼스 킷

새 브로커 어댑터가 Hermetix 공통 모델 규약을 지키는지 검증하는 표준 시나리오와, 네 언어(Kotlin/Python/JS/Go)가 함께 쓰는 골든 픽스처입니다. **새 어댑터 기여 조건은 이 킷 통과입니다** (로드맵 Phase B).

## 검증 시나리오

각 언어의 `BrokerConformance`(Kotlin) / `verify_broker_conformance`(Python) / `verifyBrokerConformance`(JS) / `VerifyBrokerConformance`(Go) 가 어댑터에 아래 흐름을 돌리고 위반 목록을 돌려줍니다.

| 단계 | 호출 | 검사 |
|---|---|---|
| capabilities | `capabilities`, `environment` | brokerId·currency 비어있지 않음, `market ∈ markets`, `environment ∈ environments`, 캔들 주기 1개 이상 |
| quotes | `getQuotes([symbol])` | 정확히 1건, 심볼은 **요청 표기 그대로**, price > 0, timestamp 존재 |
| candles | `getCandles(symbol, 첫 지원 주기, 3)` | 1건 이상 ≤ 3건, 시각 **오름차순**, low ≤ open/close ≤ high, volume ≥ 0 |
| calendar | `getCalendar()` | 1일 이상, 날짜 `YYYY-MM-DD`, 타임존 유효, 개장일은 정규장 `HH:mm` 존재 |
| account | `getAccount()` | currency == capabilities.currency, cash ≥ 0, portfolioValue ≥ 0 |
| holdings | `getHoldings()` | 심볼 비어있지 않음, quantity > 0, avgEntryPrice ≥ 0, 손익률은 **비율**(\|rate\| ≤ 10) |
| buyingPower | `getBuyingPower()` | ≥ 0, currency 일치 |
| createOrder | `BUY LIMIT quantity @ limitPrice` | orderId 비어있지 않음, status 가 미체결(open) |
| getOrder / getOrders | 방금 주문 | orderId 일치, 목록에 포함 |
| cancelOrder | 방금 주문 | status ∈ {PENDING_CANCEL, CANCELED} |
| fills | `getFills()` | quantity > 0, price > 0 (있을 때) |

의도적으로 **상태 전이(취소 후 재조회)는 검사하지 않습니다** — 정적 픽스처로 재생 가능하게 하기 위해서입니다. 실서버 상태 전이는 각 언어의 env-gated 스모크 테스트가 맡습니다.

## 골든 픽스처 (`fixtures/*.json`)

```json
{
  "broker": "kis",
  "scenario": { "symbol": "005930", "quantity": "1", "limitPrice": "70000" },
  "routes": [
    { "method": "POST", "path": "/oauth2/tokenP", "body": { ... } },
    { "header": ["tr_id", "FHKST01010100"], "body": { ... } }
  ]
}
```

- `routes` 는 위에서부터 첫 매칭을 씁니다. 조건은 `method`, `path`(정확히 일치), `header`(이름·값 일치) 이며 지정한 것만 검사합니다
- `status` 생략 시 200
- 넥스트·토스는 method+path, KIS 는 `tr_id` 헤더, 키움은 `api-id` 헤더, LS 는 `tr_cd` 헤더, NH·DB·KB 는 path 로 라우팅합니다 — 각 브로커의 실제 요청 방식과 같습니다
- 픽스처 출처: 넥스트는 공개 스펙 v1.3 문서, KIS·키움은 2026-08 모의서버 실측 시 어댑터가 읽는 필드를 그대로 재구성한 값입니다. **nh·db·ls·toss·kb 는 실측 없이 공식 SDK·문서에서 재구성한 값**이라 실측 응답으로 교체되기 전까지 "미검증" 입니다 (toss·kb 는 모의투자가 없어 실계좌 실측이 필요)

각 언어 테스트 하네스(가짜 HTTP)가 같은 파일을 읽어 어댑터의 **실제 요청 경로(헤더·인증·쓰로틀 포함)** 를 통과시킵니다. 네 언어가 같은 픽스처로 같은 시나리오를 통과하므로 포팅 간 동작 일치가 보증됩니다.

### `stream` 섹션 (실시간 채널, 0.8.0)

웹소켓 체결가를 제공하는 어댑터(kis·kiwoom)는 픽스처에 `stream` 섹션을 추가로 둡니다. REST 시나리오와 달리 **프레임 파서만** 검증합니다 — 프레임 문자열을 어댑터 파서에 넣어 언어 중립 `expected` 와 같은 `TradeTick` 이 나오는지 확인합니다 (Kotlin: `StreamFixtureTest`).

```json
"stream": {
  "channel": "TRADES",
  "frames": ["0|H0STCNT0|001|005930^093012^71500^…"],
  "expected": [{ "symbol": "005930", "price": "71500", "quantity": "15", "time": "09:30:12", "askPrice": "71500", "bidPrice": "71400",
                 "cumulativeVolume": 1234567, "change": "-300", "changeRate": "-0.0042" }]
}
```

- `frames` 는 브로커가 보내는 텍스트 프레임 그대로 (키움은 JSON 을 문자열로), `time` 은 거래소 현지(KST) `HH:mm:ss`, 숫자는 문자열
- `measured` 플래그: kis·kiwoom 의 체결가(`frames`)와 `orderBook` 은 2026-09-14 모의 웹소켓 장중 실측 프레임 그대로(KIS 는 한 프레임에 여러 레코드가 이어 붙는 실제 형태). kis·kiwoom 의 `orderEvents` 와 nh·db·ls·toss 의 전 섹션은 **공식 문서·SDK·AsyncAPI 예시에서 재구성한 값**이라 `"measured": false` 로 표시합니다 — 실측 제보가 오면 실제 프레임으로 교체하고 `true` 로 올립니다
- 각 섹션은 `frames`(브로커 원시 텍스트 프레임)와 `expected`(언어 중립 값)로 구성되며 `orderBook`(asks/bids 가격·잔량·총잔량)·`orderEvents`(orderId·type·side·수량·가격·잔량·원주문) 하위 섹션이 같은 규칙을 따릅니다. next·kb 는 웹소켓이 없어 `stream` 섹션이 없습니다

## 새 어댑터 추가 절차

1. 모의서버 실측(토큰·시세·캔들·잔고·주문·에러 포맷)으로 `fixtures/<broker>.json` 작성
2. `BrokerClient` 구현
3. 언어별 컨포먼스 테스트에 브로커 케이스 추가 → 통과
4. 자동설정(Kotlin) / 생성자 노출(포트) 등록, README 지원 표 갱신

## 실행

```bash
cd kotlin && ./gradlew :hermetix-broker:test --tests '*BrokerConformanceTest*'
cd python && pytest tests/test_conformance.py
cd js && npm test          # tests/conformance.test.ts 포함
cd go && go test -run Conformance ./...
```
