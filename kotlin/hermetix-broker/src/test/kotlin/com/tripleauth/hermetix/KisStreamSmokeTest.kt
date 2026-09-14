package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.TradeTick
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
            // 픽스처 채집용 — 처음 8개 원시 프레임(제어 프레임 포함)을 출력하고, HERMETIX_RAW_DUMP 가 있으면 그 파일에 전체를 적는다
            val dumped = java.util.concurrent.atomic.AtomicInteger()
            val dumpFile = System.getenv("HERMETIX_RAW_DUMP")?.let { java.io.File(it) }
            (stream as com.tripleauth.hermetix.broker.ReconnectingWebSocket).rawFrameHook = { raw ->
                if (dumped.incrementAndGet() <= 8) {
                    println("RAW[${dumped.get()}]: ${raw.take(300)}")
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
        }
    }

    private fun isRegularHours(): Boolean {
        val now = ZonedDateTime.now(KrxCalendar.KST)
        val t = now.toLocalTime()
        return now.dayOfWeek !in setOf(DayOfWeek.SATURDAY, DayOfWeek.SUNDAY) &&
            t.isAfter(LocalTime.of(9, 0)) && t.isBefore(LocalTime.of(15, 30))
    }
}
