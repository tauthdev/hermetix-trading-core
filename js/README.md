# Hermetix JavaScript/TypeScript

증권사 모의투자 통합 트레이딩 프레임워크 — Node.js 구현 (TypeScript, Node 22+). 버전 0.10.0

## 설치

```bash
npm install hermetix        # Node 22+, 런타임 의존성은 decimal.js 하나
```

의존성은 `decimal.js` 하나입니다 — JS 의 부동소수점(0.1+0.2≠0.3)으로 돈을 계산하지 않기 위한 필수 선택. **금액에 number 를 절대 섞지 마세요.** 실시간 웹소켓은 Node 22 내장 `WebSocket` 을 써서 추가 의존성이 없습니다.

## 빠른 시작

```ts
import { NextClient, StrategyEngine, buy, Decimal } from "hermetix";

const strategy = {
  spec: { name: "my-first", symbols: ["AAPL"] },
  decide(ctx) {
    const q = ctx.quote("AAPL");
    if (q && !ctx.hasPosition("AAPL") && !ctx.hasOpenOrder("AAPL")) {
      return [buy("AAPL", new Decimal(1), {
        takeProfitPrice: q.price.mul("1.04"),   // 익절/손절은 엔진이 자동 실행
        stopLossPrice: q.price.mul("0.98"),
      })];
    }
    return [];
  },
};

const broker = new NextClient("pk_test_...", "sk_test_...");
await new StrategyEngine(broker, [strategy]).run();
```

브로커 전환은 클라이언트 교체 한 줄:

```ts
new KisClient(appkey, appsecret, cano)   // 한국투자 모의 (KRX, 일봉만)
new NhClient(appKey, appSecret)          // NH투자증권 NH PLUG (⚠️ 미검증, 문서 기반)
new DbClient(appKey, appSecret)          // DB증권 (⚠️ 미검증, 문서 기반)
new LsClient(appKey, appSecret)          // LS증권 (⚠️ 미검증, 문서 기반)
new TossClient(clientId, clientSecret)   // 토스증권 (⚠️ 실전 전용, 미검증) — liveTradingEnabled 필수, accountSeq 비우면 첫 위탁계좌
new KbClient(appKey, appSecret)          // KB증권 오픈베타 (⚠️ 실전 전용, 미검증) — liveTradingEnabled 필수
new KiwoomClient(appkey, secretkey)      // 키움 모의 (KRX, 일봉만)
```

## 실전투자로 전환

모의에서 검증한 뒤 실제 계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```ts
const broker = new KisClient(appkey, appsecret, cano, "01", "", 0, "LIVE");  // 호스트·TR ID 자동 전환
await new StrategyEngine(broker, [strategy], 5, {
  liveTradingEnabled: true,                       // 실전 명시 동의
  maxOrderValue: new Decimal(1_000_000),          // 주문 1건 상한 (브로커 통화)
  maxDailyOrderValue: new Decimal(5_000_000),     // 하루(UTC) 누적 상한 — 매수·매도 합산
}).run();
```

- 넥스트증권은 키 프리픽스가 환경을 결정합니다 (`pk_test_`=모의, `pk_live_`=실전). `new NextClient(id, secret, account, baseUrl, "LIVE")` 에서 어긋나면 throw
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석됩니다
- 키는 항상 당신의 기기에서만 쓰입니다. Hermetix 는 어떤 서버로도 키를 보내지 않습니다

## 실시간 체결가 트리거 (0.8.0)

`spec.trigger = "ON_TRADE"` 로 선언하면 브로커 웹소켓 체결가 틱마다 전략을 호출합니다. 전략 코드는 바뀌지 않습니다.

```ts
const strategy = {
  spec: { name: "scalp", symbols: ["005930"], trigger: "ON_TRADE", minTickIntervalMs: 1000 },
  decide(ctx) { /* ctx.quote("005930") 은 마지막 체결 틱(가격·호가·누적거래량) */ return []; },
};
await new StrategyEngine(new KisClient(appkey, appsecret, cano), [strategy]).run();
```

- 지원 브로커: `kis`(H0STCNT0)·`kiwoom`(0B) — 2026-09 모의 웹소켓 장중 실측. 넥스트증권은 공개 스펙에 웹소켓이 없어 폴링만
- 몰려온 틱은 하나로 합치고 `minTickIntervalMs`(기본 1000) 보다 촘촘히는 부르지 않습니다. 캔들·계좌·미체결은 여전히 REST 라 틱마다 `1 + 심볼 수 + 3` 호출이 나갑니다
- 스트림이 끊기면 지수 백오프로 재접속하고 구독을 복원하며, 그동안 `pollIntervalSeconds` 폴링이 안전망으로 돕니다. 스트림을 선언하지 않은 브로커에서는 경고 후 폴링으로 동작합니다
- Node 22+ 필요 (내장 `WebSocket`). 테스트의 가짜 서버만 `ws` 를 devDependency 로 씁니다

### 호가·주문 통보 (2차 채널)

```ts
const strategy = {
  spec: { name: "book", symbols: ["005930"], trigger: "ON_TRADE", orderBook: true },
  decide(ctx) {
    const book = ctx.orderBook("005930");            // 10단계 호가·잔량 (asks/bids 최우선부터), 없으면 undefined
    if (book && bestAsk(book)!.quantity.gt(bestBid(book)!.quantity.mul(3))) return [];
    return [];
  },
};
// KIS 주문 통보는 HTS ID 가 있어야 구독된다 (9번째 인자). 없으면 경고만 남기고 체결 판정은 폴링으로 계속한다
new KisClient(appkey, appsecret, cano, "01", "", 0, "PAPER", "", htsId);
```

- `spec.orderBook = true` → 심볼 호가창 스트림(kis `H0STASP0`·kiwoom `0D`, 2026-09 모의 실측)을 구독해 `ctx.orderBook(symbol)` 로 공급합니다. 호가는 틱을 촉발하지 않습니다
- 주문 통보(kis `H0STCNI9/0`·kiwoom `00`)는 브로커가 제공하면 엔진이 자동 구독합니다 — 진입 주문 체결을 서버 조회 없이 브라켓에 반영하고(`BracketMonitor.onOrderEvent`), KIS 모의처럼 주문 조회가 없는 어댑터의 메모리 추적도 즉시 확정합니다(`applyOrderEvent`). 통보 프레임은 문서 기반으로 실측 전입니다
- KIS 통보 프레임은 AES-256-CBC 암호문이며 구독 응답의 key/iv 로 복호화합니다 (Node 내장 `crypto`, 추가 의존성 없음)


## nh·db·ls·toss 실시간 (문서 기반, 실측 전)

KIS·키움과 같은 `MarketStream` 인터페이스로 NH PLUG·DB증권·LS증권·토스증권의 체결가·호가·주문 통보를 구독합니다. 넷 다 공식 문서·SDK·AsyncAPI 로 만든 **미검증** 구현입니다 (해당 증권사 계좌가 있는 사용자의 실측 제보로 승격). 넥스트증권·KB증권은 웹소켓 스펙이 없어 폴링만 됩니다.

| 브로커 | 접속 | 채널 | 비고 |
|---|---|---|---|
| `NhClient` | 모의 `wss://moapi.nhplug.com:17070/websocket` / 운영 `:7070` | 체결 `oc`/`nc`/`mc`·호가 `ob`/`nb`/`mb` (생성자 `marketCd` KRX/NXT/UNT 로 선택), 통보 `d2`+`d3` | 토큰은 매 메시지 헤더. 모의는 시세 "미제공" 표기라 통보만 올 수 있음. 세션당 등록 10건(SDK)/30건(공식), 앱키당 세션 2개 |
| `DbClient` | 모의 `:17070/websocket` / 운영 `:7070` | 체결 `S00`·호가 `S01` (`tr_key` `"J "+코드`), 통보 `IS0`+`IS1` (`tr_type` 3, 해제 없음) | 접속 후 10초 안에 첫 전송. 계좌당 세션 2개·종목 50개. 본문 필드명은 대소문자 무시 조회 |
| `LsClient` | 모의 `:29443/websocket` / 실전 `:9443` | 체결 `S3_`+`K3_`·호가 `H1_`+`HA_` (KOSPI·KOSDAQ 둘 다 등록), 통보 `SC0`~`SC4` | 토큰 익일 07:00 만료. `unsubscribeTrades/OrderBook/OrderEvents` 로 해제 |
| `TossClient` | `wss://openapi-ws.tossinvest.com/ws/v1` (실전만) | 선언형 구독 — 배열 하나가 구독 집합 전체 (`trade:kr/us`, `orderbook:kr/us`, `personal:order`) | 핸드셰이크 `Authorization: Bearer` — Node 22 내장 WebSocket(undici) 의 `headers` 옵션으로 싣습니다. 60초마다 텍스트 `PING`, 계정당 연결 2개·구독 100개·선언 5회/초, 체결 틱에 누적거래량·등락 없음 |

```ts
const broker = new TossClient(clientId, clientSecret);   // 또는 NhClient / DbClient / LsClient
const stream = broker.openStream();
stream.subscribeTrades(["KRX:005930", "US:AAPL"], (t) => console.log(t.symbol, t.price.toString()));
stream.subscribeOrderBook(["KRX:005930"], (b) => console.log(bestAsk(b), bestBid(b)));
stream.subscribeOrderEvents((e) => console.log(e.type, e.orderId, e.quantity?.toString()));
stream.connect();
```

각 스트림 클래스 상단 주석에 프로토콜 근거와 미확정 항목을 적었고, 파서(`parseNhTrade`, `parseDbTrade`, `parseLsTrade`, `parseTossTrade` 등)는 `conformance/fixtures/{nh,db,ls,toss}.json#stream` 의 문서 프레임으로 검증합니다.

## 공식 전략 예제 (examples/)

Kotlin 전략 레포 3종과 동일 로직:

```bash
npm run build
HERMETIX_BROKER=next NEXT_CLIENT_ID=... node dist/examples/larry.js
HERMETIX_BROKER=kis  KIS_APPKEY=...     node dist/examples/grid.js   # KRX 호환
```

## 테스트

```bash
cd js && npm install
npm test                          # 오프라인 (골든 픽스처 재생)
node dist/tests/smoke.js next     # 실서버 (환경변수로 키 주입)
```

동작 상세는 [코어 문서](../docs/architecture.md)와 동일합니다 (KIS 메모리 주문추적, 키움 방언 정규화, KRX 호가단위 보정, 소프트웨어 브라켓, 비상정지 포함).
