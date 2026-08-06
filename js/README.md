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
new KiwoomClient(appkey, secretkey)      // 키움 모의 (KRX, 일봉만)
```

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
