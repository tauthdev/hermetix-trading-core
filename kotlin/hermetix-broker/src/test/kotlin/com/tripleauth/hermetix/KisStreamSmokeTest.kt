package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderType
import java.math.BigDecimal
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.time.DayOfWeek
import java.time.LocalTime
import java.time.ZonedDateTime
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * KIS 모의투자 웹소켓 스모크 — KIS_APPKEY / KIS_APPSECRET 가 있을 때만.
 * 접속키 발급 → 접속 → 구독까지는 언제든 확인하고, 체결 틱 수신은 정규장(평일 09:00–15:30 KST) 중에만 검증한다.
 */
@EnabledIfEnvironmentVariable(named = "KIS_APPKEY", matches = ".+")
class KisStreamSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = KisApiClient(
        KisApiProperties(
            appkey = System.getenv("KIS_APPKEY") ?: "",
            appsecret = System.getenv("KIS_APPSECRET") ?: "",
            cano = System.getenv("KIS_CANO") ?: "",
            htsId = System.getenv("KIS_HTS_ID") ?: "", // 비우면 주문 통보 구독은 생략된다
        ),
        objectMapper,
    )

    @Test
    fun `접속키-접속-구독-체결 틱 스모크`() {
        val key = client.approvalKey()
        assertThat(key).isNotBlank()
        println("approval_key 발급 OK (${key.length}자)")

        val ticks = LinkedBlockingQueue<TradeTick>()
        client.openStream().use { stream ->
            stream.subscribeTrades(listOf("005930", "000660")) { ticks.put(it) }
            val books = LinkedBlockingQueue<OrderBookTick>()
            stream.subscribeOrderBook(listOf("005930")) { books.put(it) }
            val events = LinkedBlockingQueue<OrderEvent>()
            val orderEventsOn = runCatching { stream.subscribeOrderEvents { events.put(it) }; true }
                .onFailure { println("주문 통보 구독 생략: ${it.message}") }.getOrDefault(false)
            // 픽스처 채집용 — 처음 8개 원시 프레임(제어 프레임 포함)을 출력하고, HERMETIX_RAW_DUMP 가 있으면 그 파일에 전체를 적는다
            val dumped = java.util.concurrent.atomic.AtomicInteger()
            val dumpFile = System.getenv("HERMETIX_RAW_DUMP")?.let { java.io.File(it) }
            (stream as com.tripleauth.hermetix.broker.ReconnectingWebSocket).rawFrameHook = { raw ->
                if (dumped.incrementAndGet() <= 60) {
                    if (dumped.get() <= 8) println("RAW[${dumped.get()}]: ${raw.take(300)}")
                    dumpFile?.appendText(raw + "\n")
                }
            }
            stream.connect()

            val deadline = System.currentTimeMillis() + 20_000
            while (!stream.isConnected && System.currentTimeMillis() < deadline) Thread.sleep(200)
            assertThat(stream.isConnected).withFailMessage("20s 안에 웹소켓이 열리지 않았다").isTrue()
            println("웹소켓 연결 OK")

            if (!isRegularHours()) {
                println("정규장 외 — 연결·구독 전송까지만 확인 (틱 검증 생략)")
                Thread.sleep(3000)
                return
            }
            val tick = ticks.poll(90, TimeUnit.SECONDS)
            assertThat(tick).withFailMessage("정규장인데 90s 동안 체결 틱이 없다 — 프레임 파싱/구독 확인").isNotNull()
            println("첫 틱: $tick")
            repeat(5) { ticks.poll(10, TimeUnit.SECONDS)?.let { println("틱: ${it.symbol} ${it.price} x${it.quantity} @${it.timestamp}") } }

            val book = books.poll(60, TimeUnit.SECONDS)
            assertThat(book).withFailMessage("정규장인데 60s 동안 호가가 없다 — 호가 파싱/구독 확인").isNotNull()
            println("호가: ask1=${book!!.bestAsk} bid1=${book.bestBid} asks=${book.asks.size} bids=${book.bids.size} totalAsk=${book.totalAskQuantity} totalBid=${book.totalBidQuantity}")

            if (orderEventsOn) {
                // 체결되지 않을 지정가(최우선 매수호가의 5% 아래, 호가단위 보정)로 1주 매수 → 접수 통보 → 취소 → 취소 통보
                val far = KrxTick.round((book.bestBid?.price ?: tick!!.price).multiply(BigDecimal("0.95")))
                val order = client.createOrder(CreateOrderRequest(symbol = "005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal.ONE, limitPrice = far))
                println("테스트 주문 접수: ${order.orderId} @${far}")
                val accepted = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 1: $accepted")
                client.cancelOrder(order.orderId)
                val canceled = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 2: $canceled")
                assertThat(accepted).withFailMessage("30s 동안 접수 통보가 없다").isNotNull()
                assertThat(accepted!!.orderIdMatches(order.orderId)).withFailMessage("통보 주문번호 ${accepted.orderId} ≠ ${order.orderId}").isTrue()
                assertThat(canceled).withFailMessage("30s 동안 취소 통보가 없다").isNotNull()
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
