"""브로커 팩토리 — docs/broker-factory.md 규약대로 자격 증명이 기존 생성자 인자로 매핑되는지 (네트워크 없이 생성만)."""
import pytest

import hermetix
from hermetix import (DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, TossClient,
                      TradingEnvironment)


def test_brokers_order():
    assert hermetix.brokers == ("next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb")


def test_next_maps_client_id_secret_account():
    c = hermetix.next(api_key="pk_test_abc", api_secret="sk_test_x", account="acc_1")
    assert isinstance(c, NextClient)
    assert (c._client_id, c._client_secret, c._account_id) == ("pk_test_abc", "sk_test_x", "acc_1")
    assert c.environment == TradingEnvironment.PAPER
    assert hermetix.next(api_key="pk_test_abc", api_secret="s")._account_id == "acc_main"  # 기본값은 생성자 그대로


def test_kis_maps_appkey_appsecret_cano_and_extra():
    c = hermetix.kis(api_key="k", api_secret="s", account="12345678", environment="LIVE",
                     acnt_prdt_cd="03", custtype="B", hts_id="myid", throttle_seconds=0.2, base_url="https://x")
    assert isinstance(c, KisClient)
    assert (c._appkey, c._appsecret, c._cano, c._acnt_prdt_cd, c._custtype, c._hts_id) == ("k", "s", "12345678", "03", "B", "myid")
    assert c.environment == TradingEnvironment.LIVE
    assert c._http.base_url == "https://x"


def test_kiwoom_ignores_account():
    c = hermetix.kiwoom(api_key="k", api_secret="s", account="ignored", ws_url="wss://w")
    assert isinstance(c, KiwoomClient)
    assert (c._appkey, c._secretkey, c._ws_url) == ("k", "s", "wss://w")


def test_nh_maps_account_no_and_market_codes():
    c = hermetix.nh(api_key="k", api_secret="s", account="0001", market_cd="NXT", order_market_cd="NXT", auth_url="https://a")
    assert isinstance(c, NhClient)
    assert (c._app_key, c._app_secret, c._account_no, c._market_cd, c._order_market_cd) == ("k", "s", "0001", "NXT", "NXT")
    assert c._auth_http.base_url == "https://a"


def test_ls_db_kb_extra():
    ls = hermetix.ls(api_key="k", api_secret="s", mac_address="aa", exch_gubun="K", chart_throttle_seconds=2.0)
    assert isinstance(ls, LsClient) and (ls._mac_address, ls._exch_gubun) == ("aa", "K")
    db = hermetix.db(api_key="k", api_secret="s", mac_address="bb", market_div_code="U")
    assert isinstance(db, DbClient) and (db._mac_address, db._market_div_code) == ("bb", "U")
    kb = hermetix.kb(api_key="k", api_secret="s", excg_clsf="2", sor_order_ccd="S", chart_market_clsf="1")
    assert isinstance(kb, KbClient) and (kb._excg_clsf, kb._sor_order_ccd, kb._chart_market_clsf) == ("2", "S", "1")
    assert kb.environment == TradingEnvironment.LIVE  # 브로커 기본값 유지


def test_toss_maps_account_seq():
    c = hermetix.toss(api_key="cid", api_secret="cs", account="7", environment=TradingEnvironment.PAPER)
    assert isinstance(c, TossClient)
    assert (c._client_id, c._client_secret, c._account_seq, c.environment) == ("cid", "cs", "7", TradingEnvironment.PAPER)


def test_client_by_id_and_unknown_id():
    assert isinstance(hermetix.client("next", api_key="pk_test_a", api_secret="s"), NextClient)
    with pytest.raises(ValueError, match="모르는 브로커 ID"):
        hermetix.client("binance", api_key="k", api_secret="s")


def test_rejects_unknown_extra_empty_keys_and_missing_account():
    with pytest.raises(ValueError, match="모르는 extra"):
        hermetix.kiwoom(api_key="k", api_secret="s", hts_id="x")
    with pytest.raises(ValueError, match="api_key"):
        hermetix.next(api_key="", api_secret="s")
    with pytest.raises(ValueError, match="account"):
        hermetix.kis(api_key="k", api_secret="s")
    with pytest.raises(ValueError, match="environment"):
        hermetix.kis(api_key="k", api_secret="s", account="1", environment="SANDBOX")
