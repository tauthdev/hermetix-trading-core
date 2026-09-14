package com.tripleauth.hermetix.conformance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.TradeTick
import com.tripleauth.hermetix.client.kis.KisMarketStream
import com.tripleauth.hermetix.client.kiwoom.KiwoomMarketStream
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.time.LocalDate

/**
 * 골든 픽스처의 `stream` 섹션 — 네 언어가 같은 프레임을 같은 [TradeTick] 으로 파싱하는지.
 * 픽스처의 `expected` 는 언어 중립 표기(문자열 숫자, `time` 은 KST HH:mm:ss)다.
 */
class StreamFixtureTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private val today: LocalDate = LocalDate.of(2026, 9, 14)

    private fun fixture(broker: String): JsonNode {
        val file = File("../../conformance/fixtures/$broker.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file).path("stream").also { check(!it.isMissingNode) { "$broker 픽스처에 stream 섹션이 없다" } }
    }

    private fun assertMatches(ticks: List<TradeTick>, expected: JsonNode) {
        assertThat(ticks).hasSize(expected.size())
        ticks.zip(expected.toList()).forEach { (tick, e) ->
            assertThat(tick.symbol).isEqualTo(e["symbol"].asText())
            assertThat(tick.price).isEqualByComparingTo(e["price"].asText())
            assertThat(tick.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(tick.askPrice).isEqualByComparingTo(e["askPrice"].asText())
            assertThat(tick.bidPrice).isEqualByComparingTo(e["bidPrice"].asText())
            assertThat(tick.cumulativeVolume).isEqualTo(e["cumulativeVolume"].asLong())
            assertThat(tick.change).isEqualByComparingTo(e["change"].asText())
            assertThat(tick.changeRate).isEqualByComparingTo(e["changeRate"].asText())
            assertThat(tick.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
        }
    }

    @Test
    fun `kis - H0STCNT0 프레임`() {
        val stream = fixture("kis")
        assertThat(stream["channel"].asText()).isEqualTo("TRADES")
        val ticks = stream["frames"].flatMap { KisMarketStream.parseFrame(it.asText(), today) }
        assertMatches(ticks, stream["expected"])
    }

    @Test
    fun `kiwoom - REAL 0B 프레임`() {
        val stream = fixture("kiwoom")
        assertThat(stream["channel"].asText()).isEqualTo("TRADES")
        val ticks = stream["frames"].flatMap { KiwoomMarketStream.parseReal(objectMapper.readTree(it.asText()), today) }
        assertMatches(ticks, stream["expected"])
    }
}
