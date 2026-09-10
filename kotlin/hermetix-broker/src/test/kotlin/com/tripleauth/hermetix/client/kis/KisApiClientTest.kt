package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderType
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/** 환경(모의/실전)에 따른 호스트·TR ID 선택과 시장 접두 심볼 처리를 검증한다. */
class KisApiClientTest {

    private lateinit var server: MockWebServer

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun client(environment: TradingEnvironment) = KisApiClient(
        KisApiProperties(
            environment = environment,
            baseUrl = server.url("/").toString().removeSuffix("/"),
            appkey = "k", appsecret = "s", cano = "50199202",
            throttleMillis = 1,
        ),
        objectMapper,
    )

    private fun json(body: String) = MockResponse().setHeader("Content-Type", "application/json").setBody(body)

    @Test
    fun `환경별 기본 호스트·쓰로틀·TR 프리픽스`() {
        val paper = KisApiProperties(environment = TradingEnvironment.PAPER)
        val live = KisApiProperties(environment = TradingEnvironment.LIVE)

        assertThat(paper.resolvedBaseUrl()).isEqualTo("https://openapivts.koreainvestment.com:29443")
        assertThat(live.resolvedBaseUrl()).isEqualTo("https://openapi.koreainvestment.com:9443")
        assertThat(paper.tr("TTC0802U")).isEqualTo("VTTC0802U")
        assertThat(live.tr("TTC0802U")).isEqualTo("TTTC0802U")
        assertThat(paper.resolvedThrottleMillis()).isEqualTo(600)
        assertThat(live.resolvedThrottleMillis()).isEqualTo(100)
        assertThat(KisApiProperties(baseUrl = "http://custom", throttleMillis = 50).resolvedBaseUrl()).isEqualTo("http://custom")
        assertThat(KisApiProperties(throttleMillis = 50).resolvedThrottleMillis()).isEqualTo(50)
    }

    @Test
    fun `실전 환경은 T 프리픽스 TR 로 주문하고 시장 접두 심볼은 코드만 보낸다`() {
        server.enqueue(json("""{"access_token":"tok","token_type":"Bearer","expires_in":86400}"""))
        server.enqueue(json("""{"rt_cd":"0","msg_cd":"","msg1":"","output":{"ODNO":"0001234","KRX_FWDG_ORD_ORGNO":"","ORD_TMD":"090000"}}"""))
        // 주문 접수 후 추적 기준 보유수량 조회(잔고)
        server.enqueue(json("""{"rt_cd":"0","msg_cd":"","msg1":"","output1":[],"output2":[{"dnca_tot_amt":"0","tot_evlu_amt":"0"}]}"""))

        val client = client(TradingEnvironment.LIVE)
        assertThat(client.environment).isEqualTo(TradingEnvironment.LIVE)

        val order = client.createOrder(
            CreateOrderRequest(symbol = "KRX:005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal("1"), limitPrice = BigDecimal("70000")),
        )

        assertThat(order.orderId).isEqualTo("0001234")
        assertThat(order.symbol).isEqualTo("005930")

        server.takeRequest() // token
        val orderRequest = server.takeRequest()
        assertThat(orderRequest.getHeader("tr_id")).isEqualTo("TTTC0802U")
        val body = objectMapper.readTree(orderRequest.body.readUtf8())
        assertThat(body["PDNO"].asText()).isEqualTo("005930")
        assertThat(body["ORD_UNPR"].asText()).isEqualTo("70000")
        assertThat(server.takeRequest().getHeader("tr_id")).isEqualTo("TTTC8434R") // 잔고
    }

    @Test
    fun `모의 환경은 V 프리픽스 TR 을 쓴다`() {
        server.enqueue(json("""{"access_token":"tok","token_type":"Bearer","expires_in":86400}"""))
        server.enqueue(json("""{"rt_cd":"0","msg_cd":"","msg1":"","output":{"ord_psbl_cash":"1000000"}}"""))

        val buyingPower = client(TradingEnvironment.PAPER).getBuyingPower()

        assertThat(buyingPower.buyingPower).isEqualByComparingTo(BigDecimal("1000000"))
        server.takeRequest() // token
        assertThat(server.takeRequest().getHeader("tr_id")).isEqualTo("VTTC8908R")
    }
}
