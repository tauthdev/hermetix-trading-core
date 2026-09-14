# Hermetix Python

증권사 모의투자 통합 트레이딩 프레임워크 — Python 구현. **런타임 의존성 0개** (표준 라이브러리만). 실시간 스트림만 선택 의존성 `websockets`(+ KIS 주문 통보 복호화용 `cryptography`)를 씁니다 (`pip install 'hermetix[stream]'`).

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
broker = LsClient(app_key=..., app_secret=...)            # LS증권 (⚠️ 미검증, 문서 기반)
broker = TossClient(client_id=..., client_secret=...)     # 토스증권 (⚠️ 실전 전용, 미검증) — live_trading_enabled 필수
broker = KbClient(app_key=..., app_secret=...)            # KB증권 오픈베타 (⚠️ 실전 전용, 미검증) — live_trading_enabled 필수
broker = KiwoomClient(appkey=..., secretkey=...)          # 키움 모의 (KRX)
```

KRX 브로커는 일봉(DAY_1)만 지원합니다(토스는 1m/1d) — 엔진이 기동 시 비호환 전략을 걸러내고 이유를 알려줍니다.

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

## 실시간 스트림 (0.8.0)

`kis`·`kiwoom` 은 웹소켓 체결가·호가 스트림(2026-09 모의 실측)과 주문 통보 스트림(문서 기반, 실측 전)을 제공합니다. 전략 코드는 그대로 두고 `trigger` 만 바꾸면 체결 틱마다 `decide()` 가 호출됩니다.

```bash
pip install 'hermetix[stream]'      # websockets + cryptography 선택 의존성 — 코어는 여전히 의존성 0
```

```python
from hermetix import KisClient, Strategy, StrategySpec, StrategyEngine, TickTrigger

class Scalper(Strategy):
    spec = StrategySpec(name="scalp", symbols=["005930"],
                        trigger=TickTrigger.ON_TRADE,          # 체결 틱마다 호출
                        min_tick_interval_seconds=1.0,         # 연속 호출 사이 최소 간격 (캔들·계좌 REST 폭주 방지)
                        poll_interval_seconds=60,              # 스트림이 끊겼을 때의 안전망 주기
                        order_book=True)                       # 호가창 스트림도 구독 -> ctx.order_book(symbol)
    ...

StrategyEngine(KisClient(appkey=..., appsecret=..., cano=...), [Scalper()]).run()
```

- 몰려온 틱은 하나로 합쳐지고, 스트림이 끊기면 자동 재접속하는 동안 폴링이 계속 돕니다
- 스트림 틱이 전략의 모든 심볼을 덮으면 `ctx.quote()` 는 REST 대신 마지막 체결 틱(가격·호가·누적거래량)입니다. 캔들·계좌·미체결은 여전히 REST
- 스트림을 선언하지 않은 브로커(`next` 는 공개 스펙에 웹소켓 없음, 나머지는 미구현)에서 `ON_TRADE` 를 쓰면 경고 후 폴링으로 동작합니다
- `order_book=True` 전략은 심볼의 10단계 호가창을 `ctx.order_book(symbol)` 로 받습니다 (`best_ask`/`best_bid`/`asks`/`bids`/총잔량). 호가는 틱을 촉발하지 않습니다
- 브로커가 주문 통보 채널을 제공하면 엔진이 자동 구독해 진입 주문 체결을 서버 조회 없이 브라켓에 반영하고, KIS 모의처럼 주문 조회가 없는 어댑터의 메모리 추적도 즉시 확정합니다. KIS 는 `KisClient(..., hts_id="HTS아이디")` 가 필요하고(통보 프레임은 AES 암호문이라 `cryptography` 사용), 비우면 경고 후 폴링 판정으로 동작합니다
- 프레임 파서: `hermetix.brokers.kis_stream.parse_kis_frame / parse_kis_order_book / parse_kis_order_events`, `hermetix.brokers.kiwoom_stream.parse_kiwoom_real / parse_kiwoom_order_book / parse_kiwoom_order_events` — 골든 픽스처 `conformance/fixtures/*.json#stream` 으로 검증

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
├── broker.py     BrokerClient ABC + MarketStream/StreamingBrokerClient + HTTP/쓰로틀/KRX 캘린더
├── stream.py     ReconnectingWebSocket (websockets 선택 의존성, 재접속·유휴 감시·직렬 전송)
├── brokers/      next.py · kis.py · kiwoom.py · … (방언 정규화는 어댑터 책임), kis_stream.py · kiwoom_stream.py
├── strategy.py   Strategy/StrategySpec(TickTrigger·order_book)/StrategyContext(order_book)/Signal(Buy·Sell·Cancel)
└── engine.py     StrategyEngine(ON_TRADE 트리거·호가·주문통보)/브라켓(통보로 활성화)/비상정지/PnL
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
