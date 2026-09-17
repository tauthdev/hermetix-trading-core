package com.tripleauth.hermetix

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.NextApiClient
import com.tripleauth.hermetix.client.db.DbApiClient
import com.tripleauth.hermetix.client.kb.KbApiClient
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.ls.LsApiClient
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.toss.TossApiClient
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/** 브로커 팩토리 — docs/broker-factory.md 의 자격 증명 매핑·에러·목록 (네트워크 없음) */
class HermetixFactoryTest {

    private val base = Credentials(apiKey = "KEY", apiSecret = "SECRET", account = "12345678")

    @Test
    fun `brokers 목록은 규약 순서다`() {
        assertThat(Hermetix.brokers).containsExactly("next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb")
    }

    @Test
    fun `next - client_id·client_secret·account_id 로 매핑, account 생략 시 acc_main`() {
        val p = Hermetix.nextProperties(Credentials("pk_test_x", "sk_test_y"))
        assertThat(p.clientId).isEqualTo("pk_test_x")
        assertThat(p.clientSecret).isEqualTo("sk_test_y")
        assertThat(p.accountId).isEqualTo("acc_main")
        assertThat(p.environment).isEqualTo(TradingEnvironment.PAPER)
        assertThat(Hermetix.nextProperties(base.copy(account = "acc_2", environment = TradingEnvironment.LIVE, extra = mapOf("base_url" to "https://x"))))
            .satisfies({ assertThat(it.accountId).isEqualTo("acc_2"); assertThat(it.environment).isEqualTo(TradingEnvironment.LIVE); assertThat(it.baseUrl).isEqualTo("https://x") })
    }

    @Test
    fun `kis - appkey·appsecret·cano 와 extra(acnt_prdt_cd·custtype·hts_id·throttle_seconds)`() {
        val p = Hermetix.kisProperties(base.copy(extra = mapOf("acnt_prdt_cd" to "22", "custtype" to "B", "hts_id" to "HTS", "throttle_seconds" to "0.6", "ws_url" to "ws://w")))
        assertThat(p.appkey).isEqualTo("KEY")
        assertThat(p.appsecret).isEqualTo("SECRET")
        assertThat(p.cano).isEqualTo("12345678")
        assertThat(p.acntPrdtCd).isEqualTo("22")
        assertThat(p.custtype).isEqualTo("B")
        assertThat(p.htsId).isEqualTo("HTS")
        assertThat(p.throttleMillis).isEqualTo(600)
        assertThat(p.wsUrl).isEqualTo("ws://w")
        assertThat(p.environment).isEqualTo(TradingEnvironment.PAPER)
        // 기본값은 기존 Properties 와 같다
        val d = Hermetix.kisProperties(base)
        assertThat(d.acntPrdtCd).isEqualTo("01")
        assertThat(d.custtype).isEqualTo("P")
        assertThat(d.throttleMillis).isEqualTo(0)
    }

    @Test
    fun `kis 는 account 가 필수다`() {
        assertThatThrownBy { Hermetix.kisProperties(base.copy(account = "")) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("account")
    }

    @Test
    fun `kiwoom - appkey·secretkey, account 는 무시`() {
        val p = Hermetix.kiwoomProperties(base.copy(extra = mapOf("throttle_seconds" to "2")))
        assertThat(p.appkey).isEqualTo("KEY")
        assertThat(p.secretkey).isEqualTo("SECRET")
        assertThat(p.throttleMillis).isEqualTo(2000)
        assertThat(p.environment).isEqualTo(TradingEnvironment.PAPER)
    }

    @Test
    fun `nh - app_key·app_secret·account_no 와 auth_url·market_cd·order_market_cd`() {
        val p = Hermetix.nhProperties(base.copy(extra = mapOf("auth_url" to "https://a", "market_cd" to "NXT", "order_market_cd" to "NXT")))
        assertThat(p.appKey).isEqualTo("KEY")
        assertThat(p.appSecret).isEqualTo("SECRET")
        assertThat(p.accountNo).isEqualTo("12345678")
        assertThat(p.authUrl).isEqualTo("https://a")
        assertThat(p.marketCd).isEqualTo("NXT")
        assertThat(p.orderMarketCd).isEqualTo("NXT")
        assertThat(p.throttleMillis).isEqualTo(250)
    }

    @Test
    fun `ls - mac_address·exch_gubun·chart_throttle_seconds`() {
        val p = Hermetix.lsProperties(base.copy(extra = mapOf("mac_address" to "AA:BB", "exch_gubun" to "K", "chart_throttle_seconds" to "1.5")))
        assertThat(p.appKey).isEqualTo("KEY")
        assertThat(p.macAddress).isEqualTo("AA:BB")
        assertThat(p.exchGubun).isEqualTo("K")
        assertThat(p.chartThrottleMillis).isEqualTo(1500)
        assertThat(p.throttleMillis).isEqualTo(500)
    }

    @Test
    fun `db - mac_address·market_div_code`() {
        val p = Hermetix.dbProperties(base.copy(extra = mapOf("mac_address" to "AA", "market_div_code" to "U")))
        assertThat(p.appSecret).isEqualTo("SECRET")
        assertThat(p.macAddress).isEqualTo("AA")
        assertThat(p.marketDivCode).isEqualTo("U")
        assertThat(p.baseUrl).isEqualTo("https://openapi.dbsec.co.kr:8443")
    }

    @Test
    fun `toss - client_id·client_secret·account_seq, 기본 LIVE`() {
        val p = Hermetix.tossProperties(base.copy(account = "7"))
        assertThat(p.clientId).isEqualTo("KEY")
        assertThat(p.clientSecret).isEqualTo("SECRET")
        assertThat(p.accountSeq).isEqualTo("7")
        assertThat(p.environment).isEqualTo(TradingEnvironment.LIVE)
        assertThat(Hermetix.tossProperties(base.copy(environment = TradingEnvironment.PAPER)).environment).isEqualTo(TradingEnvironment.PAPER)
    }

    @Test
    fun `kb - excg_clsf·sor_order_ccd·chart_market_clsf, 기본 LIVE`() {
        val p = Hermetix.kbProperties(base.copy(extra = mapOf("excg_clsf" to "2", "sor_order_ccd" to "N", "chart_market_clsf" to "1")))
        assertThat(p.appKey).isEqualTo("KEY")
        assertThat(p.excgClsf).isEqualTo("2")
        assertThat(p.sorOrderCcd).isEqualTo("N")
        assertThat(p.chartMarketClsf).isEqualTo("1")
        assertThat(p.environment).isEqualTo(TradingEnvironment.LIVE)
    }

    @Test
    fun `빈 api_key·api_secret, 모르는 extra 키, 숫자가 아닌 throttle 은 거부한다`() {
        assertThatThrownBy { Hermetix.nextProperties(Credentials("", "s")) }.isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("api_key")
        assertThatThrownBy { Hermetix.nextProperties(Credentials("k", " ")) }.isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("api_secret")
        assertThatThrownBy { Hermetix.kiwoomProperties(base.copy(extra = mapOf("hts_id" to "x"))) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("hts_id")
        assertThatThrownBy { Hermetix.kisProperties(base.copy(extra = mapOf("throttle_seconds" to "fast"))) }
            .isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("throttle_seconds")
    }

    @Test
    fun `브로커별 함수와 client(id) 는 구체 클래스를 돌려주고, 모르는 ID 는 거부한다`() {
        assertThat(Hermetix.next(base)).isInstanceOf(NextApiClient::class.java)
        assertThat(Hermetix.kis(base)).isInstanceOf(KisApiClient::class.java)
        assertThat(Hermetix.kiwoom(base)).isInstanceOf(KiwoomApiClient::class.java)
        assertThat(Hermetix.nh(base)).isInstanceOf(NhApiClient::class.java)
        assertThat(Hermetix.ls(base)).isInstanceOf(LsApiClient::class.java)
        assertThat(Hermetix.db(base)).isInstanceOf(DbApiClient::class.java)
        assertThat(Hermetix.toss(base)).isInstanceOf(TossApiClient::class.java)
        assertThat(Hermetix.kb(base)).isInstanceOf(KbApiClient::class.java)

        val byId: Map<String, Class<out BrokerClient>> = mapOf(
            "next" to NextApiClient::class.java, "kis" to KisApiClient::class.java, "kiwoom" to KiwoomApiClient::class.java, "nh" to NhApiClient::class.java,
            "ls" to LsApiClient::class.java, "db" to DbApiClient::class.java, "toss" to TossApiClient::class.java, "kb" to KbApiClient::class.java,
        )
        byId.forEach { (id, type) ->
            val client = Hermetix.client(id, base)
            assertThat(client).isInstanceOf(type)
            assertThat(client.capabilities.brokerId).isEqualTo(id)
        }
        assertThat(Hermetix.client("KIS", base)).isInstanceOf(KisApiClient::class.java) // 대소문자 무시
        assertThatThrownBy { Hermetix.client("binance", base) }.isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining("binance")
    }
}
