# Hermetix Python

증권사 모의투자 통합 트레이딩 프레임워크 — Python 구현. **런타임 의존성 0개** (표준 라이브러리만).

Kotlin 구현(레퍼런스)과 같은 동작을 보장합니다: 같은 브로커 어댑터 3종, 같은 전략 규약, 같은 안전장치. 금액은 전부 `Decimal` — float 를 섞지 마세요.

## 설치

```bash
pip install ./python          # 레포 루트에서 (PyPI 배포 전)
```

## 빠른 시작

```python
from decimal import Decimal
from hermetix import NextClient, Strategy, StrategySpec, StrategyEngine, Buy, CandleInterval

class Ma20Strategy(Strategy):
    spec = StrategySpec(name="ma20", symbols=["AAPL"],
                        candle_interval=CandleInterval.DAY_1, candle_limit=20)

    def decide(self, ctx):
        q = ctx.quote("AAPL")
        candles = ctx.candles_of("AAPL")
        if not q or len(candles) < 20 or ctx.has_position("AAPL") or ctx.has_open_order("AAPL"):
            return []
        ma20 = sum(c.close for c in candles) / 20
        if q.price > ma20:
            return [Buy("AAPL", Decimal(1),
                        take_profit_price=q.price * Decimal("1.04"),   # 익절/손절은
                        stop_loss_price=q.price * Decimal("0.98"))]    # 엔진이 자동 실행
        return []

broker = NextClient(client_id="pk_test_...", client_secret="sk_test_...")
StrategyEngine(broker, [Ma20Strategy()]).run()   # 정규장 중에만 틱, Ctrl+C 로 종료
```

브로커 전환은 클라이언트 교체 한 줄:

```python
broker = KisClient(appkey=..., appsecret=..., cano=...)   # 한국투자 모의 (KRX)
broker = NhClient(app_key=..., app_secret=...)            # NH투자증권 NH PLUG (⚠️ 미검증, 문서 기반)
broker = DbClient(app_key=..., app_secret=...)            # DB증권 (⚠️ 미검증, 문서 기반)
broker = KiwoomClient(appkey=..., secretkey=...)          # 키움 모의 (KRX)
```

KRX 브로커는 일봉(DAY_1)만 지원합니다 — 엔진이 기동 시 비호환 전략을 걸러내고 이유를 알려줍니다.

## 실전투자로 전환

모의에서 검증한 뒤 실제 계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```python
from decimal import Decimal
from hermetix import KisClient, StrategyEngine, TradingEnvironment

broker = KisClient(appkey=..., appsecret=..., cano=..., environment=TradingEnvironment.LIVE)  # 호스트·TR ID 자동 전환
StrategyEngine(
    broker, [MyStrategy()],
    live_trading_enabled=True,               # 실전 명시 동의
    max_order_value=Decimal(1_000_000),      # 주문 1건 상한 (브로커 통화)
    max_daily_order_value=Decimal(5_000_000),  # 하루(UTC) 누적 상한 — 매수·매도 합산
).run()
```

- 넥스트증권은 키 프리픽스가 환경을 결정합니다 (`pk_test_`=모의, `pk_live_`=실전). `environment` 와 어긋나면 생성 시 `ValueError`
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석됩니다
- 키는 항상 당신의 기기에서만 쓰입니다. Hermetix 는 어떤 서버로도 키를 보내지 않습니다

## 수익률

```python
from hermetix import pnl_report
print(pnl_report(broker, initial_capital=Decimal(20000)))
```

## 구조

```
hermetix/
├── models.py     공통 도메인 모델 (Quote/Candle/Order/... 전부 Decimal)
├── errors.py     타입화된 에러 (RateLimit/MarketClosed/... 엔진이 타입별 반응)
├── broker.py     BrokerClient ABC + HTTP/쓰로틀/KRX 캘린더
├── brokers/      next.py · kis.py · kiwoom.py (방언 정규화는 어댑터 책임)
├── strategy.py   Strategy/StrategySpec/StrategyContext/Signal(Buy·Sell·Cancel)
└── engine.py     StrategyEngine/브라켓/비상정지/PnL
```

## 공식 전략 예제 (examples/)

Kotlin 전략 레포 3종과 동일 로직의 실행 가능한 단일 파일:

| 파일 | 전략 | 실행 |
|---|---|---|
| `examples/larry.py` | 변동성 돌파 | `HERMETIX_BROKER=next NEXT_CLIENT_ID=... python examples/larry.py` |
| `examples/trend_breakout.py` | WMA 추세선 돌파 | 동일 |
| `examples/grid.py` | 목표가 스캘핑 (KRX 호환) | `HERMETIX_BROKER=kis KIS_APPKEY=... python examples/grid.py` |

## 테스트

```bash
pip install -e './python[dev]'
pytest python/tests/                      # 오프라인 (골든 픽스처 재생)
python python/tests/smoke.py next|kis|kiwoom   # 실서버 (환경변수로 키 주입)
```

동작 상세(브로커별 제약, 소프트웨어 브라켓, 상태 지도)는 [코어 문서](../docs/architecture.md)와 동일합니다.
