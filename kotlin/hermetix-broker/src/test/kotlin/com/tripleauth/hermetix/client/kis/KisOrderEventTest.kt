package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/** 주문 통보가 KIS 메모리 주문 추적에 반영되는지 — 모의 서버는 주문 조회가 없어 통보가 유일한 즉시 확정 경로 */
class KisOrderEventTest {

    private lateinit var server: MockWebServer
    private val objectMapper = ObjectMapper().registerModule(kotlinModule()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    /** 보유 조회(VTTC8434R) 호출 횟수 — 통보로 확정된 뒤에는 늘지 않아야 한다 */
    private val balanceCalls = AtomicInteger()

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        // 큐 방식은 순서가 어긋나면 요청이 무한 대기하므로 tr_id/path 로 라우팅한다
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse = when {
                request.path?.startsWith("/oauth2/tokenP") == true -> json("""{"access_token":"tok","expires_in":86400}""")
                request.getHeader("tr_id") == "VTTC0802U" -> json("""{"rt_cd":"0","msg_cd":"APBK0013","msg1":"주문 전송 완료","output":{"KRX_FWDG_ORD_ORGNO":"00950","ODNO":"0012345","ORD_TMD":"105530"}}""")
                request.getHeader("tr_id") == "VTTC8434R" -> { balanceCalls.incrementAndGet(); json("""{"rt_cd":"0","msg_cd":"MCA00000","msg1":"ok","output1":[],"output2":[{"dnca_tot_amt":"1000000","tot_evlu_amt":"1000000"}]}""") }
                else -> MockResponse().setResponseCode(599).setBody("""{"rt_cd":"1","msg1":"unexpected ${request.path} ${request.getHeader("tr_id")}"}""")
            }
        }
        server.start()
    }

    @AfterEach
    fun tearDown() = server.shutdown()

    private fun json(body: String) = MockResponse().setHeader("Content-Type", "application/json").setBody(body)

    private fun client() = KisApiClient(
        KisApiProperties(baseUrl = server.url("/").toString().removeSuffix("/"), appkey = "k", appsecret = "s", cano = "50199202", throttleMillis = 1),
        objectMapper,
    )

    private fun event(type: OrderEventType, quantity: String? = null, price: String? = null) =
        OrderEvent(orderId = "0000012345", type = type, timestamp = Instant.now(), symbol = "005930", side = OrderSide.BUY,
            quantity = quantity?.let { BigDecimal(it) }, price = price?.let { BigDecimal(it) })

    @Test
    fun `체결 통보가 누적되어 주문을 FILLED 로 만들고, 그 뒤 getOrder 는 보유 조회를 하지 않는다`() {
        val c = client()
        val order = c.createOrder(CreateOrderRequest(symbol = "005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal("2"), limitPrice = BigDecimal("250000")))
        assertThat(order.orderId).isEqualTo("0012345")
        val baselineCalls = balanceCalls.get() // createOrder 가 기준 보유를 1회 조회

        c.applyOrderEvent(event(OrderEventType.ACCEPTED, quantity = "2", price = "250000"))
        c.applyOrderEvent(event(OrderEventType.FILLED, quantity = "1", price = "250000"))
        assertThat(c.getOrder("0012345").status).isEqualTo(OrderStatus.PARTIALLY_FILLED) // 아직 open → refresh 1회

        c.applyOrderEvent(event(OrderEventType.FILLED, quantity = "1", price = "250500"))
        val filled = c.getOrder("0012345")
        assertThat(filled.status).isEqualTo(OrderStatus.FILLED)
        assertThat(filled.filledQuantity).isEqualByComparingTo("2")
        assertThat(filled.avgFillPrice).isEqualByComparingTo("250500")
        assertThat(balanceCalls.get()).isEqualTo(baselineCalls + 1)
        assertThat(c.getFills().fills).hasSize(1)
        assertThat(balanceCalls.get()).isEqualTo(baselineCalls + 1) // FILLED 뒤에는 조회 없음
    }

    @Test
    fun `취소 통보는 주문을 CANCELED 로, 모르는 주문번호는 무시`() {
        val c = client()
        c.createOrder(CreateOrderRequest(symbol = "005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal("2"), limitPrice = BigDecimal("250000")))
        c.applyOrderEvent(event(OrderEventType.CANCELED))
        assertThat(c.getOrder("0012345").status).isEqualTo(OrderStatus.CANCELED)
        c.applyOrderEvent(OrderEvent(orderId = "9999999", type = OrderEventType.FILLED, timestamp = Instant.now(), quantity = BigDecimal.ONE))
        assertThat(c.getOrders().orders).isEmpty()
    }
}
