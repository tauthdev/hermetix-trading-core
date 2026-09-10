package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.InsufficientFundsError
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant

/** 넥스트증권 공개 스펙 v1.3 응답 형태를 공통 모델로 정규화하는지 검증한다 (픽스처는 v1.3 문서 기준). */
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
            clientId = "pk_test_demo",
            clientSecret = "sk_test_demo",
            accountId = "acc_main",
        )
        client = NextApiClient(properties, TokenManager(properties, objectMapper), objectMapper)
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun enqueueToken(expiresIn: Long = 43200) {
        server.enqueue(json("""{"access_token":"tok-1","token_type":"Bearer","expires_in":$expiresIn}"""))
    }

    private fun json(body: String, status: Int = 200): MockResponse =
        MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body.trimIndent())

    // ------------------------------------------------------------------ market

    @Test
    fun `quotes - outcome=OK 만 공통 모델로, 등락률은 %를 비율로, 시각은 KST를 Instant로`() {
        enqueueToken()
        server.enqueue(
            json(
                """
                {"quotes":[
                  {"symbol":"AAPL","outcome":"OK","session":"REGULAR","requestedAt":"2026-09-10T23:10:00+09:00",
                   "price":"308.91","previousClose":"333.43","change":"-24.52","changeRate":"-7.3539",
                   "bidPrice":"308.90","bidSize":"100","askPrice":"308.92","askSize":"200",
                   "volume":"132756799","lastTradeAt":"2026-09-10T23:09:58+09:00"},
                  {"symbol":"NOPE","outcome":"NOT_FOUND","session":"CLOSED","requestedAt":"2026-09-10T23:10:00+09:00"}
                ]}
                """,
            ),
        )

        val response = client.getQuotes(listOf("AAPL", "NOPE"))

        assertThat(response.quotes).hasSize(1)
        val quote = response.quotes[0]
        assertThat(quote.symbol).isEqualTo("AAPL")
        assertThat(quote.price).isEqualByComparingTo(BigDecimal("308.91"))
        assertThat(quote.changeRate).isEqualByComparingTo(BigDecimal("-0.073539"))
        assertThat(quote.volume).isEqualTo(132756799L)
        assertThat(quote.timestamp).isEqualTo(Instant.parse("2026-09-10T14:09:58Z"))

        server.takeRequest() // token
        val request = server.takeRequest()
        assertThat(request.path).isEqualTo("/v1/market/quotes?symbols=AAPL,NOPE")
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer tok-1")
        // v1.3: 토큰 발급 외 전 고객 API 에 X-Request-Id 필수 (영숫자·._- 만, 64자 이하)
        assertThat(request.getHeader("X-Request-Id")).matches("^[A-Za-z0-9._-]{1,64}$")
    }

    @Test
    fun `candles - time 필드를 timestamp 로, 오프셋 없는 시각은 KST 로 해석`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"symbol":"AAPL","interval":"1d","candles":[
                  {"time":"2026-09-09T22:30:00","session":"REGULAR","open":"339.73","high":"344.56","low":"337.35","close":"338.19","volume":"56298904"}
                ],"nextCursor":null}""",
            ),
        )

        val response = client.getCandles("AAPL", CandleInterval.DAY_1, 3)

        assertThat(response.candles).hasSize(1)
        assertThat(response.candles[0].timestamp).isEqualTo(Instant.parse("2026-09-09T13:30:00Z"))
        assertThat(response.candles[0].volume).isEqualTo(56298904L)
        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/market/candles?symbol=AAPL&interval=1d&limit=3")
    }

    @Test
    fun `calendar - KST 세션 시각을 뉴욕 현지 HHmm 으로, status 를 open 으로 정규화`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"calendar":[
                  {"date":"2026-09-10","status":"OPEN","holidayName":null,"sessions":[
                    {"type":"PRE","open":"2026-09-10T17:00:00+09:00","close":"2026-09-10T22:30:00+09:00"},
                    {"type":"REGULAR","open":"2026-09-10T22:30:00+09:00","close":"2026-09-11T05:00:00+09:00"},
                    {"type":"AFTER","open":"2026-09-11T05:00:00+09:00","close":"2026-09-11T09:00:00+09:00"}]},
                  {"date":"2026-11-27","status":"HALF_DAY","holidayName":"Day After Thanksgiving","sessions":[
                    {"type":"REGULAR","open":"2026-11-27T23:30:00+09:00","close":"2026-11-28T03:00:00+09:00"}]},
                  {"date":"2026-12-25","status":"CLOSED","holidayName":"Christmas","sessions":[]}
                ]}""",
            ),
        )

        val days = client.getCalendar().calendar

        assertThat(days).hasSize(3)
        assertThat(days[0].open).isTrue()
        assertThat(days[0].timezone).isEqualTo("America/New_York")
        assertThat(days[0].sessions?.regular?.start).isEqualTo("09:30")
        assertThat(days[0].sessions?.regular?.end).isEqualTo("16:00")
        assertThat(days[0].sessions?.preMarket?.start).isEqualTo("04:00")
        assertThat(days[1].open).isTrue()
        assertThat(days[1].sessions?.regular?.end).isEqualTo("13:00") // 반장일 (EST)
        assertThat(days[1].holiday).isEqualTo("Day After Thanksgiving")
        assertThat(days[2].open).isFalse()
        assertThat(days[2].sessions).isNull()
    }

    @Test
    fun `instruments - tradable 상태 enum 을 Boolean 요약 + 원본으로`() {
        enqueueToken()
        server.enqueue(
            json("""{"symbol":"AAPL","name":"Apple Inc.","type":"COMMON_STOCK","exchange":"XNAS","currency":"USD","tradable":"SELL_ONLY","dayMarketTradable":true}"""),
        )

        val detail = client.getInstrument("AAPL")

        assertThat(detail.tradable).isFalse()
        assertThat(detail.tradableStatus).isEqualTo("SELL_ONLY")
        assertThat(detail.dayMarketTradable).isTrue()
        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/instruments/AAPL")
    }

    // ----------------------------------------------------------------- account

    @Test
    fun `account - cashAmount 를 cash 로, 총평가는 예수금 + 보유 평가금액 합`() {
        enqueueToken()
        server.enqueue(json("""{"accountId":"acc_main","currency":"USD","cashAmount":"1000.50","requestedAt":"2026-09-10T23:10:00+09:00"}"""))
        server.enqueue(
            json(
                """{"currency":"USD","holdings":[
                  {"symbol":"AAPL","name":"Apple Inc.","quantity":"2","sellableQuantity":"2","averageBuyPrice":"300.00",
                   "currentPrice":"310.00","purchaseAmount":"600.00","evaluationAmount":"620.00","evaluationPnl":"20.00","evaluationPnlRate":"3.3333"}
                ],"requestedAt":"2026-09-10T23:10:00+09:00"}""",
            ),
        )

        val account = client.getAccount()

        assertThat(account.cash).isEqualByComparingTo(BigDecimal("1000.50"))
        assertThat(account.portfolioValue).isEqualByComparingTo(BigDecimal("1620.50"))
        server.takeRequest() // token
        val accountRequest = server.takeRequest()
        assertThat(accountRequest.path).isEqualTo("/v1/account")
        assertThat(accountRequest.getHeader("X-Next-Account-Id")).isEqualTo("acc_main")
        assertThat(accountRequest.getHeader("X-Nextsecurities-Account")).isNull()
        assertThat(server.takeRequest().path).isEqualTo("/v1/account/holdings")
    }

    @Test
    fun `holdings - averageBuyPrice와 evaluation 계열을 공통 모델로, 손익률은 비율로`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"currency":"USD","holdings":[
                  {"symbol":"AAPL","name":"Apple Inc.","quantity":"2","sellableQuantity":"1","averageBuyPrice":"300.00",
                   "currentPrice":"310.00","purchaseAmount":"600.00","evaluationAmount":"620.00","evaluationPnl":"20.00","evaluationPnlRate":"3.3333"}
                ],"requestedAt":"2026-09-10T23:10:00+09:00"}""",
            ),
        )

        val holdings = client.getHoldings()

        val aapl = holdings.holdings[0]
        assertThat(aapl.avgEntryPrice).isEqualByComparingTo(BigDecimal("300.00"))
        assertThat(aapl.marketValue).isEqualByComparingTo(BigDecimal("620.00"))
        assertThat(aapl.unrealizedPnl).isEqualByComparingTo(BigDecimal("20.00"))
        assertThat(aapl.unrealizedPnlRate).isEqualByComparingTo(BigDecimal("0.033333"))
        assertThat(holdings.summary?.totalMarketValue).isEqualByComparingTo(BigDecimal("620.00"))
    }

    // ------------------------------------------------------------------ orders

    @Test
    fun `createOrder - v1_3 본문(market·clientOrderId 필수)을 보내고 접수 응답을 파싱한다`() {
        enqueueToken()
        server.enqueue(json("""{"orderId":"ord_8615a1f026","market":"US","status":"SUBMITTED","requestedAt":"2026-09-10T23:10:00+09:00"}"""))

        val order = client.createOrder(
            CreateOrderRequest(symbol = "AAPL", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal("1"), limitPrice = BigDecimal("200"), clientOrderId = "my-strategy-abcd1234"),
        )

        assertThat(order.orderId).isEqualTo("ord_8615a1f026")
        assertThat(order.status).isEqualTo(OrderStatus.SUBMITTED)
        assertThat(order.submittedAt).isEqualTo(Instant.parse("2026-09-10T14:10:00Z"))

        server.takeRequest() // token
        val request = server.takeRequest()
        assertThat(request.path).isEqualTo("/v1/orders")
        val body = objectMapper.readTree(request.body.readUtf8())
        assertThat(body["market"].asText()).isEqualTo("US")
        assertThat(body["clientOrderId"].asText()).isEqualTo("my-strategy-abcd1234")
        assertThat(body["orderType"].asText()).isEqualTo("LIMIT")
        assertThat(body["quantity"].asText()).isEqualTo("1")
        assertThat(body["limitPrice"].asText()).isEqualTo("200")
    }

    @Test
    fun `getOrder - 상세 응답의 requestId 를 clientOrderId 로, amount 를 notional 로`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"orderId":"ord_1","requestId":"my-strategy-abcd1234","market":"US","symbol":"AAPL","side":"BUY","orderType":"LIMIT",
                    "timeInForce":"DAY","status":"PARTIALLY_FILLED","quantity":"10","filledQuantity":"4","limitPrice":"200","avgFillPrice":"199.5",
                    "settlementDate":"2026-09-14","requestedAt":"2026-09-10T23:10:00+09:00","updatedAt":"2026-09-10T23:12:00+09:00"}""",
            ),
        )

        val order = client.getOrder("ord_1")

        assertThat(order.clientOrderId).isEqualTo("my-strategy-abcd1234")
        assertThat(order.status).isEqualTo(OrderStatus.PARTIALLY_FILLED)
        assertThat(order.status.isOpen).isTrue()
        assertThat(order.orderType).isEqualTo(OrderType.LIMIT)
        assertThat(order.filledQuantity).isEqualByComparingTo(BigDecimal("4"))
        assertThat(order.notional).isNull()
    }

    @Test
    fun `cancelOrder - PENDING_CANCEL 은 미체결(open)로 분류한다`() {
        enqueueToken()
        server.enqueue(json("""{"orderId":"ord_1","status":"PENDING_CANCEL","requestedAt":"2026-09-10T23:10:00+09:00"}"""))

        val order = client.cancelOrder("ord_1")

        assertThat(order.status).isEqualTo(OrderStatus.PENDING_CANCEL)
        assertThat(order.status.isOpen).isTrue()
        server.takeRequest() // token
        val request = server.takeRequest()
        assertThat(request.method).isEqualTo("DELETE")
        assertThat(request.path).isEqualTo("/v1/orders/ord_1")
        assertThat(request.getHeader("X-Next-Account-Id")).isEqualTo("acc_main")
    }

    @Test
    fun `fills - fillId 없이 filledAt 을 timestamp 로`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"fills":[{"orderId":"ord_1","market":"US","symbol":"AAPL","side":"BUY","quantity":"1","price":"199.5","amount":"199.5","fee":"0.1","filledAt":"2026-09-10T23:11:00+09:00"}],"nextCursor":null}""",
            ),
        )

        val fills = client.getFills().fills

        assertThat(fills).hasSize(1)
        assertThat(fills[0].fillId).isNull()
        assertThat(fills[0].side).isEqualTo(OrderSide.BUY)
        assertThat(fills[0].amount).isEqualByComparingTo(BigDecimal("199.5"))
        assertThat(fills[0].timestamp).isEqualTo(Instant.parse("2026-09-10T14:11:00Z"))
    }

    // ------------------------------------------------------------- environment

    @Test
    fun `환경 설정과 키 프리픽스가 어긋나면 기동 실패`() {
        val live = NextApiProperties(environment = TradingEnvironment.LIVE, clientId = "pk_test_demo", clientSecret = "x")
        assertThatThrownBy { NextApiClient(live, TokenManager(live, objectMapper), objectMapper) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("pk_live_")

        val paper = NextApiProperties(environment = TradingEnvironment.PAPER, clientId = "pk_live_real", clientSecret = "x")
        assertThatThrownBy { NextApiClient(paper, TokenManager(paper, objectMapper), objectMapper) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("pk_test_")

        val ok = NextApiProperties(environment = TradingEnvironment.LIVE, clientId = "pk_live_real", clientSecret = "x")
        assertThat(NextApiClient(ok, TokenManager(ok, objectMapper), objectMapper).environment).isEqualTo(TradingEnvironment.LIVE)
        assertThat(client.capabilities.environments).containsExactlyInAnyOrder(TradingEnvironment.PAPER, TradingEnvironment.LIVE)
    }

    @Test
    fun `시장 접두 심볼은 코드만 보내고 응답은 요청 표기로 돌려준다`() {
        enqueueToken()
        server.enqueue(
            json(
                """{"quotes":[{"symbol":"AAPL","outcome":"OK","session":"REGULAR","requestedAt":"2026-09-10T23:10:00+09:00",
                    "price":"308.91","volume":"1","lastTradeAt":"2026-09-10T23:09:58+09:00"}]}""",
            ),
        )

        val response = client.getQuotes(listOf("US:AAPL"))

        assertThat(response.quotes[0].symbol).isEqualTo("US:AAPL")
        server.takeRequest() // token
        assertThat(server.takeRequest().path).isEqualTo("/v1/market/quotes?symbols=AAPL")
        assertThatThrownBy { client.getQuotes(listOf("KRX:005930")) }.isInstanceOf(IllegalArgumentException::class.java)
    }

    // ------------------------------------------------------------------ errors

    @Test
    fun `에러 엔벨로프를 타입화된 예외로 변환한다`() {
        enqueueToken()
        server.enqueue(json("""{"error":{"type":"not_found","code":"symbol-not-found","message":"종목 정보를 찾을 수 없습니다","param":null,"requestId":"req_1","data":null,"docUrl":"https://docs"}}""", 404))

        assertThatThrownBy { client.getQuotes(listOf("NOPE")) }
            .isInstanceOf(BrokerApiException::class.java)
            .hasMessageContaining("symbol-not-found")
    }

    @Test
    fun `insufficient-scope(권한)는 자금 부족으로 오인하지 않는다`() {
        enqueueToken()
        server.enqueue(json("""{"error":{"type":"permission","code":"insufficient-scope","message":"조회 전용 키","param":null,"requestId":"req_3","data":null,"docUrl":null}}""", 403))

        assertThatThrownBy { client.cancelOrder("ord_1") }
            .isInstanceOf(BrokerApiException::class.java)
            .isNotInstanceOf(InsufficientFundsError::class.java)
            .hasMessageContaining("insufficient-scope")
    }

    @Test
    fun `401이면 토큰을 재발급하고 1회 재시도한다`() {
        enqueueToken()
        server.enqueue(json("""{"error":{"type":"authentication","code":"invalid-token","message":"만료","param":null,"requestId":"req_2","data":null,"docUrl":null}}""", 401))
        enqueueToken()
        server.enqueue(json("""{"accountId":"acc_main","currency":"USD","buyingPower":"100","requestedAt":"2026-09-10T23:10:00+09:00"}"""))

        val buyingPower = client.getBuyingPower()

        assertThat(buyingPower.buyingPower).isEqualByComparingTo(BigDecimal("100"))
        assertThat(server.requestCount).isEqualTo(4)
    }

    @Test
    fun `토큰 발급 실패는 OAuth 표준 에러 형식을 AuthError 로 변환한다`() {
        server.enqueue(json("""{"error":"invalid_client","error_description":"클라이언트 인증 실패"}""", 401))

        assertThatThrownBy { client.getQuotes(listOf("AAPL")) }
            .isInstanceOf(AuthError::class.java)
            .hasMessageContaining("invalid_client")
            .hasMessageContaining("클라이언트 인증 실패")
        val request = server.takeRequest()
        assertThat(request.path).isEqualTo("/v1/oauth/token")
        assertThat(request.getHeader("Content-Type")).startsWith("application/x-www-form-urlencoded")
    }
}
