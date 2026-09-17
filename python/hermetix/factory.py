"""브로커 팩토리 — ``hermetix.next(...)`` 처럼 브로커 ID 한 토큰만 바꾸면 증권사가 바뀐다.

규약은 ``docs/broker-factory.md`` 가 정본이다. 네 언어(Kotlin·Python·JS·Go)가 같은 이름·같은 자격 증명 모양을 쓴다.

    import hermetix
    client = hermetix.next(api_key="pk_test_...", api_secret="sk_test_...", account="acc_main")
    client = hermetix.kis(api_key="...", api_secret="...", account="12345678", hts_id="myid")
    client = hermetix.client("kiwoom", api_key="...", api_secret="...")

팩토리는 통일된 자격 증명(api_key·api_secret·account·environment·extra)을 기존 생성자 인자로 **매핑만** 한다 —
인증·환경 결정·쓰로틀 기본값은 각 클라이언트 클래스 그대로다. 기존 클래스 직접 생성(``KisClient(...)``)도 계속 된다.

``next`` 는 내장 함수와 이름이 같으므로 ``from hermetix import next`` 대신 ``hermetix.next(...)`` 로 쓰기를 권한다.
"""
from __future__ import annotations

from typing import Callable

from .broker import BrokerClient
from .brokers.db import DbClient
from .brokers.kb import KbClient
from .brokers.kis import KisClient
from .brokers.kiwoom import KiwoomClient
from .brokers.ls import LsClient
from .brokers.next import NextClient
from .brokers.nh import NhClient
from .brokers.toss import TossClient
from .models import TradingEnvironment

#: 브로커 ID 목록 (BrokerCapabilities.broker_id 와 같다). 순서는 규약대로
brokers: tuple[str, ...] = ("next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb")

# 공통 extra 키 → 그 브로커 생성자 인자 이름. None 이면 그 브로커는 받지 않는다 (→ ValueError)
_COMMON_EXTRA = ("base_url", "ws_url", "throttle_seconds")
# 브로커별 (허용 extra 키 집합, api_key 인자명, api_secret 인자명, account 인자명 또는 None, account 필수 여부)
_SPEC: dict[str, tuple[type, frozenset[str], str, str, str | None, bool]] = {
    "next":   (NextClient,   frozenset({"base_url"}),                                              "client_id", "client_secret", "account_id", False),
    "kis":    (KisClient,    frozenset({*_COMMON_EXTRA, "acnt_prdt_cd", "custtype", "hts_id"}),   "appkey",    "appsecret",     "cano",       True),
    "kiwoom": (KiwoomClient, frozenset(_COMMON_EXTRA),                                            "appkey",    "secretkey",     None,         False),
    "nh":     (NhClient,     frozenset({*_COMMON_EXTRA, "auth_url", "market_cd", "order_market_cd"}), "app_key", "app_secret",  "account_no", False),
    "ls":     (LsClient,     frozenset({*_COMMON_EXTRA, "mac_address", "exch_gubun", "chart_throttle_seconds"}), "app_key", "app_secret", None, False),
    "db":     (DbClient,     frozenset({*_COMMON_EXTRA, "mac_address", "market_div_code"}),       "app_key",   "app_secret",    None,         False),
    "toss":   (TossClient,   frozenset(_COMMON_EXTRA),                                            "client_id", "client_secret", "account_seq", False),
    "kb":     (KbClient,     frozenset({"base_url", "throttle_seconds", "excg_clsf", "sor_order_ccd", "chart_market_clsf"}), "app_key", "app_secret", None, False),
}


def _environment(value: TradingEnvironment | str | None) -> TradingEnvironment | None:
    if value is None or isinstance(value, TradingEnvironment):
        return value
    try:
        return TradingEnvironment(str(value).upper())
    except ValueError:
        raise ValueError(f"environment 는 PAPER 또는 LIVE 여야 합니다: {value!r}") from None


def client(broker_id: str, api_key: str, api_secret: str, account: str = "",
           environment: TradingEnvironment | str | None = None, **extra) -> BrokerClient:
    """브로커 ID 로 클라이언트를 만든다. 모르는 ID·extra 키, 빈 키는 ``ValueError``."""
    spec = _SPEC.get(broker_id.lower())
    if spec is None:
        raise ValueError(f"모르는 브로커 ID: {broker_id!r} (가능: {', '.join(brokers)})")
    cls, allowed, key_arg, secret_arg, account_arg, account_required = spec
    if not api_key or not api_secret:
        raise ValueError(f"{broker_id}: api_key 와 api_secret 은 비울 수 없습니다")
    unknown = sorted(set(extra) - allowed)
    if unknown:
        raise ValueError(f"{broker_id}: 모르는 extra 키 {unknown} (가능: {sorted(allowed)})")
    if account_required and not account:
        raise ValueError(f"{broker_id}: account 가 필요합니다 (docs/broker-factory.md 표 참고)")
    kwargs: dict = {key_arg: api_key, secret_arg: api_secret, **extra}
    if account_arg is not None and account:
        kwargs[account_arg] = account
    env = _environment(environment)
    if env is not None:
        kwargs["environment"] = env
    return cls(**kwargs)


def _factory(broker_id: str) -> Callable[..., BrokerClient]:
    def make(api_key: str, api_secret: str, account: str = "",
             environment: TradingEnvironment | str | None = None, **extra) -> BrokerClient:
        return client(broker_id, api_key, api_secret, account, environment, **extra)
    make.__name__ = broker_id
    make.__qualname__ = broker_id
    make.__doc__ = f"``hermetix.client('{broker_id}', ...)`` 와 같다. 자격 증명 매핑은 docs/broker-factory.md."
    return make


next = _factory("next")      # noqa: A001 — 브로커 ID 가 곧 함수 이름 (hermetix.next 로 쓴다)
kis = _factory("kis")
kiwoom = _factory("kiwoom")
nh = _factory("nh")
ls = _factory("ls")
db = _factory("db")
toss = _factory("toss")
kb = _factory("kb")

__all__ = ["brokers", "client", "next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb"]
