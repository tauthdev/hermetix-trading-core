package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.client.dto.CandleInterval
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class NextApiClientTest {

    private lateinit var server: MockWebServer
    private lateinit var client: NextApiClient

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
        val properties = NextApiProperties(
            baseUrl = server.url("/").toString().removeSuffix("/"),
            clientId = "pk_test",
            clientSecret = "sk_test",
            accountId = "acc_main",
        )
        client = NextApiClient(properties, TokenManager(properties, objectMapper), objectMapper)
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun enqueueToken(expiresIn: Long = 86400) {
        server.enqueue(
            MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("""{"access_token":"tok-1","token_type":"Bearer","expires_in":$expiresIn}"""),
        )
    }

    @Test
    fun `실측 스키마의 quotes 응답을 파싱한다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """
                {"quotes":[{"symbol":"AAPL","price":"308.91","bidPrice":null,"askPrice":null,
                "volume":132756799,"change":"-24.52","changeRate":"-0.073539","timestamp":"2026-07-31T04:00:00Z"}]}
                """.trimIndent(),
            ),
        )

        val response = client.getQuotes(listOf("AAPL"))

        assertThat(response.quotes).hasSize(1)
        assertThat(response.quotes[0].price).isEqualByComparingTo(BigDecimal("308.91"))
        assertThat(response.quotes[0].bidPrice).isNull()

        server.takeRequest() // token
        val request = server.takeRequest()
        assertThat(request.path).isEqualTo("/v1/market/quotes?symbols=AAPL")
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer tok-1")
    }

    @Test
    fun `candles 요청에 interval과 limit 파라미터를 붙인다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"symbol":"AAPL","interval":"1d","candles":[{"timestamp":"2026-07-29T04:00:00Z","open":"339.73","high":"344.56","low":"337.35","close":"338.19","volume":56298904}]}""",
            ),
        )

        val response = client.getCandles("AAPL", CandleInterval.DAY_1, 3)

        assertThat(response.candles).hasSize(1)
        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/market/candles?symbol=AAPL&interval=1d&limit=3")
    }

    @Test
    fun `종목 상세를 조회한다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json").setBody(
                """{"symbol":"AAPL","name":"Apple Inc.","type":"STOCK","exchange":"NASDAQ","currency":"USD",
                "tradable":true,"fractionable":true,"shortable":true,"easyToBorrow":true,"marginable":true,
                "minOrderSize":null,"status":"ACTIVE","restrictions":[]}""".trimIndent(),
            ),
        )

        val detail = client.getInstrument("AAPL")

        assertThat(detail.tradable).isTrue()
        assertThat(detail.fractionable).isTrue()
        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/instruments/AAPL")
    }

    @Test
    fun `종목 검색 파라미터를 붙인다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json")
                .setBody("""{"instruments":[],"nextCursor":null}"""),
        )

        client.getInstruments(search = "apple", limit = 5)

        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/instruments?search=apple&limit=5")
    }

    @Test
    fun `에러 엔벨로프를 NextApiException으로 변환한다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setResponseCode(404).setHeader("Content-Type", "application/json").setBody(
                """{"error":{"type":"not_found","code":"symbol-not-found","message":"종목 정보를 찾을 수 없습니다","param":null,"requestId":"req_1","data":null,"docUrl":"https://docs"}}""",
            ),
        )

        assertThatThrownBy { client.getQuotes(listOf("NOPE")) }
            .isInstanceOf(NextApiException::class.java)
            .hasMessageContaining("symbol-not-found")
    }

    @Test
    fun `401이면 토큰을 재발급하고 1회 재시도한다`() {
        enqueueToken()
        server.enqueue(
            MockResponse().setResponseCode(401).setHeader("Content-Type", "application/json").setBody(
                """{"error":{"type":"authentication","code":"invalid-token","message":"만료","param":null,"requestId":"req_2","data":null,"docUrl":null}}""",
            ),
        )
        enqueueToken()
        server.enqueue(
            MockResponse().setHeader("Content-Type", "application/json")
                .setBody("""{"accountId":"acc_main","name":"주력 계좌","currency":"USD","cash":"100","portfolioValue":"200","status":"ACTIVE"}"""),
        )

        val account = client.getAccount()

        assertThat(account.accountId).isEqualTo("acc_main")
        assertThat(server.requestCount).isEqualTo(4)

        server.takeRequest() // token 1
        val first = server.takeRequest()
        assertThat(first.getHeader("X-Nextsecurities-Account")).isEqualTo("acc_main")
    }
}
