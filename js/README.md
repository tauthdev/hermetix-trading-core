# Hermetix JavaScript/TypeScript

증권사 모의투자 통합 트레이딩 프레임워크 — Node.js 구현 (TypeScript, Node 18+).

의존성은 `decimal.js` 하나입니다 — JS 의 부동소수점(0.1+0.2≠0.3)으로 돈을 계산하지 않기 위한 필수 선택. **금액에 number 를 절대 섞지 마세요.**

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
