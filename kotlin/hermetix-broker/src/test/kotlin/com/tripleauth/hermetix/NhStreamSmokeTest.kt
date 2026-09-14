package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.ReconnectingWebSocket
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.nh.NhApiProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.io.File
import java.math.BigDecimal
import java.time.DayOfWeek
import java.time.LocalTime
import java.time.ZonedDateTime
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * NH PLUG 모의투자 웹소켓 스모크 — NH_APP_KEY / NH_APP_SECRET / NH_ACCOUNT_NO (선택 NH_MARKET_CD) 가 있을 때만.
 * 접속·구독 ACK 까지는 언제든 확인하고, 체결·호가 수신은 정규장 중에만 검증한다.
 * 포털 가이드상 모의(17070)는 시세 채널 "미제공" 이라 틱이 없을 수 있다 — 그 경우 실패가 아니라 경고로 남긴다.
 * 주문 통보 흐름(체결 안 될 지정가 1주 매수 → 취소)은 NH_SMOKE_ORDER=1 일 때만 돌린다.
 */
@EnabledIfEnvironmentVariable(named = "NH_APP_KEY", matches = ".+")
class NhStreamSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = NhApiClient(
        NhApiProperties(
            appKey = System.getenv("NH_APP_KEY") ?: "",
            appSecret = System.getenv("NH_APP_SECRET") ?: "",
            accountNo = System.getenv("NH_ACCOUNT_NO") ?: "",
            marketCd = System.getenv("NH_MARKET_CD") ?: "KRX",
        ),
        objectMapper,
    )

    @Test
    fun `접속-구독-체결·호가-주문 통보 스모크`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val books = LinkedBlockingQueue<OrderBookTick>()
        val events = LinkedBlockingQueue<OrderEvent>()
        client.openStream().use { stream ->
            stream.subscribeTrades(listOf("005930", "000660")) { ticks.put(it) }
            stream.subscribeOrderBook(listOf("005930")) { books.put(it) }
            stream.subscribeOrderEvents { events.put(it) }
            // 픽스처 채집용 — 처음 8개 원시 프레임을 출력하고, HERMETIX_RAW_DUMP 가 있으면 그 파일에 전체를 적는다
            val dumped = AtomicInteger()
            val dumpFile = System.getenv("HERMETIX_RAW_DUMP")?.let { File(it) }
            (stream as ReconnectingWebSocket).rawFrameHook = { raw ->
                if (dumped.incrementAndGet() <= 60) {
                    if (dumped.get() <= 8) println("RAW[${dumped.get()}]: ${raw.take(300)}")
                    dumpFile?.appendText(raw + "\n")
                }
            }
            stream.connect()

            val deadline = System.currentTimeMillis() + 20_000
            while (!stream.isConnected && System.currentTimeMillis() < deadline) Thread.sleep(200)
            assertThat(stream.isConnected).withFailMessage("20s 안에 웹소켓이 열리지 않았다 (운영 서버라면 TLS 중간 CA 문제일 수 있다)").isTrue()
            println("웹소켓 연결 OK")

            if (!isRegularHours()) {
                println("정규장 외 — 연결·구독 전송까지만 확인 (틱 검증 생략)")
                Thread.sleep(3000)
                return
            }
            val tick = ticks.poll(90, TimeUnit.SECONDS)
            if (tick == null) {
                println("경고: 정규장인데 90s 동안 체결 틱이 없다 — 모의 서버 시세 채널 미제공(포털 가이드) 가능성. 프레임 덤프를 확인할 것")
            } else {
                println("첫 틱: $tick")
                repeat(5) { ticks.poll(10, TimeUnit.SECONDS)?.let { println("틱: ${it.symbol} ${it.price} x${it.quantity} @${it.timestamp}") } }
            }
            val book = books.poll(60, TimeUnit.SECONDS)
            println(if (book == null) "경고: 60s 동안 호가가 없다" else "호가: ask1=${book.bestAsk} bid1=${book.bestBid} asks=${book.asks.size} bids=${book.bids.size} totalAsk=${book.totalAskQuantity} totalBid=${book.totalBidQuantity}")

            if (System.getenv("NH_SMOKE_ORDER") == "1") {
                val reference = book?.bestBid?.price ?: tick?.price ?: client.getQuotes(listOf("005930")).quotes.first().price
                val far = KrxTick.round(reference.multiply(BigDecimal("0.95")))
                val order = client.createOrder(CreateOrderRequest(symbol = "005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal.ONE, limitPrice = far))
                println("테스트 주문 접수: ${order.orderId} @$far")
                val accepted = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 1: $accepted")
                client.cancelOrder(order.orderId)
                val canceled = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 2: $canceled")
                assertThat(accepted).withFailMessage("30s 동안 접수 통보(d3)가 없다").isNotNull()
                assertThat(accepted!!.orderIdMatches(order.orderId)).withFailMessage("통보 주문번호 ${accepted.orderId} ≠ ${order.orderId}").isTrue()
                assertThat(canceled).withFailMessage("30s 동안 취소 통보(d2 ucgb=2)가 없다").isNotNull()
            }
        }
    }

    private fun isRegularHours(): Boolean {
        val now = ZonedDateTime.now(KrxCalendar.KST)
        val t = now.toLocalTime()
        return now.dayOfWeek !in setOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) &&
            t.isAfter(LocalTime.of(9, 0)) && t.isBefore(LocalTime.of(15, 30))
    }
}
