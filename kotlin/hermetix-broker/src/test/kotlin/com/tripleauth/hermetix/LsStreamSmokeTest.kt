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
import com.tripleauth.hermetix.client.ls.LsApiClient
import com.tripleauth.hermetix.client.ls.LsApiProperties
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
 * LS증권 모의투자 웹소켓 스모크 — LS_APP_KEY / LS_APP_SECRET (법인은 LS_MAC_ADDRESS) 가 있을 때만.
 * 접속·등록(ACK)까지는 언제든 확인하고, 체결·호가 수신은 정규장(평일 09:00–15:30 KST) 중에만 검증한다.
 * 주문 통보 검증(체결되지 않을 지정가 1주 매수 → 취소)은 LS_SMOKE_ORDER=1 일 때만 한다.
 * HERMETIX_RAW_DUMP 에 파일 경로를 주면 원시 프레임을 그대로 적는다 (픽스처 채집용).
 */
@EnabledIfEnvironmentVariable(named = "LS_APP_KEY", matches = ".+")
class LsStreamSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = LsApiClient(
        LsApiProperties(
            appKey = System.getenv("LS_APP_KEY") ?: "",
            appSecret = System.getenv("LS_APP_SECRET") ?: "",
            macAddress = System.getenv("LS_MAC_ADDRESS") ?: "",
        ),
        objectMapper,
    )

    @Test
    fun `접속-등록-체결·호가 틱-주문 통보 스모크`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val books = LinkedBlockingQueue<OrderBookTick>()
        val events = LinkedBlockingQueue<OrderEvent>()
        client.openStream().use { stream ->
            stream.subscribeTrades(listOf("005930", "000660")) { ticks.put(it) }
            stream.subscribeOrderBook(listOf("005930")) { books.put(it) }
            stream.subscribeOrderEvents { events.put(it) }
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
            assertThat(stream.isConnected).withFailMessage("20s 안에 웹소켓이 열리지 않았다").isTrue()
            println("웹소켓 연결 OK")

            if (!isRegularHours()) {
                println("정규장 외 — 연결·등록 전송까지만 확인 (틱 검증 생략)")
                Thread.sleep(3000)
                return
            }
            val tick = ticks.poll(90, TimeUnit.SECONDS)
            assertThat(tick).withFailMessage("정규장인데 90s 동안 체결 틱이 없다 — S3_/K3_ 등록·프레임 파싱 확인").isNotNull()
            println("첫 틱: $tick")
            repeat(5) { ticks.poll(10, TimeUnit.SECONDS)?.let { println("틱: ${it.symbol} ${it.price} x${it.quantity} @${it.timestamp}") } }

            val book = books.poll(60, TimeUnit.SECONDS)
            assertThat(book).withFailMessage("정규장인데 60s 동안 호가가 없다 — H1_/HA_ 등록·프레임 파싱 확인").isNotNull()
            println("호가: ask1=${book!!.bestAsk} bid1=${book.bestBid} asks=${book.asks.size} bids=${book.bids.size} totalAsk=${book.totalAskQuantity} totalBid=${book.totalBidQuantity}")

            if (System.getenv("LS_SMOKE_ORDER") == "1") {
                // 체결되지 않을 지정가(최우선 매수호가의 5% 아래, 호가단위 보정)로 1주 매수 → 접수 통보 → 취소 → 취소 통보
                val far = KrxTick.round((book.bestBid?.price ?: tick!!.price).multiply(BigDecimal("0.95")))
                val order = client.createOrder(CreateOrderRequest(symbol = "005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal.ONE, limitPrice = far))
                println("테스트 주문 접수: ${order.orderId} @$far")
                val accepted = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 1: $accepted")
                client.cancelOrder(order.orderId)
                val canceled = events.poll(30, TimeUnit.SECONDS)
                println("주문 통보 2: $canceled")
                assertThat(accepted).withFailMessage("30s 동안 접수 통보(SC0)가 없다").isNotNull()
                assertThat(accepted!!.orderIdMatches(order.orderId)).withFailMessage("통보 주문번호 ${accepted.orderId} ≠ ${order.orderId}").isTrue()
                assertThat(canceled).withFailMessage("30s 동안 취소 통보(SC3)가 없다").isNotNull()
            } else {
                println("주문 통보 검증 생략 (LS_SMOKE_ORDER=1 로 켠다)")
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
