package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.ReconnectingWebSocket
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.toss.TossApiClient
import com.tripleauth.hermetix.client.toss.TossApiProperties
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import java.math.BigDecimal
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 토스증권 웹소켓 스모크 — TOSS_CLIENT_ID / TOSS_CLIENT_SECRET (선택 TOSS_ACCOUNT_SEQ) 가 있을 때만.
 *
 * ⚠️ **실전 계좌** 다. 토스는 모의투자가 없다. 기본은 접속·선언·수신 확인만 하고 주문은 내지 않는다.
 * `TOSS_SMOKE_ORDER=1` 일 때만 체결되지 않을 지정가(최우선 매수호가의 5% 아래, 호가단위 보정)로 삼성전자 1주 매수 후 즉시 취소해
 * personal:order 이벤트를 확인한다 — 실제 주문이므로 허용 IP·계좌 상태를 확인하고 실행할 것.
 */
@EnabledIfEnvironmentVariable(named = "TOSS_CLIENT_ID", matches = ".+")
class TossStreamSmokeTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val client = TossApiClient(
        TossApiProperties(
            clientId = System.getenv("TOSS_CLIENT_ID") ?: "",
            clientSecret = System.getenv("TOSS_CLIENT_SECRET") ?: "",
            accountSeq = System.getenv("TOSS_ACCOUNT_SEQ") ?: "",
        ),
        objectMapper,
    )

    @Test
    fun `접속-선언-체결·호가 수신 스모크 (주문은 TOSS_SMOKE_ORDER=1 일 때만)`() {
        val ticks = LinkedBlockingQueue<TradeTick>()
        val books = LinkedBlockingQueue<OrderBookTick>()
        val events = LinkedBlockingQueue<OrderEvent>()
        client.openStream().use { stream ->
            stream.subscribeTrades(listOf("KRX:005930", "US:AAPL")) { ticks.put(it) }
            stream.subscribeOrderBook(listOf("KRX:005930")) { books.put(it) }
            stream.subscribeOrderEvents { events.put(it) }
            val dumped = AtomicInteger()
            val dumpFile = System.getenv("HERMETIX_RAW_DUMP")?.let { java.io.File(it) }
            (stream as ReconnectingWebSocket).rawFrameHook = { raw ->
                if (dumped.incrementAndGet() <= 60) {
                    if (dumped.get() <= 8) println("RAW[${dumped.get()}]: ${raw.take(300)}")
                    dumpFile?.appendText(raw + "\n")
                }
            }
            stream.connect()

            val deadline = System.currentTimeMillis() + 20_000
            while (!stream.isConnected && System.currentTimeMillis() < deadline) Thread.sleep(200)
            assertThat(stream.isConnected).withFailMessage("20s 안에 웹소켓이 열리지 않았다 (401 토큰 / 403 허용 IP 확인)").isTrue()
            println("웹소켓 연결 OK")

            // 장 시간에 따라 안 올 수 있으므로 검증하지 않고 관찰만 한다 (KRX 정규장·NXT·미국 프리/정규/애프터)
            val tick = ticks.poll(60, TimeUnit.SECONDS)
            println(if (tick == null) "60s 동안 체결 없음 (장외?)" else "첫 틱: $tick")
            repeat(3) { ticks.poll(5, TimeUnit.SECONDS)?.let { println("틱: ${it.symbol} ${it.price} x${it.quantity} @${it.timestamp}") } }
            val book = books.poll(30, TimeUnit.SECONDS)
            println(if (book == null) "30s 동안 호가 없음" else "호가: ask1=${book.bestAsk} bid1=${book.bestBid} asks=${book.asks.size} bids=${book.bids.size}")

            if (System.getenv("TOSS_SMOKE_ORDER") == "1") {
                val ref = book?.bestBid?.price ?: tick?.takeIf { it.symbol.endsWith("005930") }?.price
                    ?: client.getQuotes(listOf("KRX:005930")).quotes.first().price
                val far = KrxTick.round(ref.multiply(BigDecimal("0.95")))
                val order = client.createOrder(CreateOrderRequest(symbol = "KRX:005930", side = OrderSide.BUY, orderType = OrderType.LIMIT, quantity = BigDecimal.ONE, limitPrice = far))
                println("테스트 주문 접수: ${order.orderId} @$far")
                val accepted = events.poll(30, TimeUnit.SECONDS)
                println("주문 이벤트 1: $accepted")
                val canceled = client.cancelOrder(order.orderId)
                println("취소 응답: ${canceled.orderId} ${canceled.status}")
                val canceledEvent = events.poll(30, TimeUnit.SECONDS)
                println("주문 이벤트 2: $canceledEvent")
                assertThat(accepted).withFailMessage("30s 동안 접수 이벤트가 없다").isNotNull()
                assertThat(canceledEvent).withFailMessage("30s 동안 취소 이벤트가 없다").isNotNull()
            }
        }
    }
}
