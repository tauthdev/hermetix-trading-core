package com.tripleauth.hermetix.conformance

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.OrderBookTick
import com.tripleauth.hermetix.broker.OrderEvent
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

    private fun assertBooks(books: List<OrderBookTick>, expected: JsonNode) {
        assertThat(books).hasSize(expected.size())
        books.zip(expected.toList()).forEach { (book, e) ->
            assertThat(book.symbol).isEqualTo(e["symbol"].asText())
            assertThat(book.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            fun levels(node: JsonNode) = node.map { it["price"].asText() to it["quantity"].asText() }
            assertThat(book.asks.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["asks"]))
            assertThat(book.bids.map { it.price.toPlainString() to it.quantity.toPlainString() }).isEqualTo(levels(e["bids"]))
            assertThat(book.totalAskQuantity).isEqualByComparingTo(e["totalAskQuantity"].asText())
            assertThat(book.totalBidQuantity).isEqualByComparingTo(e["totalBidQuantity"].asText())
        }
    }

    private fun assertEvents(events: List<OrderEvent>, expected: JsonNode) {
        assertThat(events).hasSize(expected.size())
        events.zip(expected.toList()).forEach { (ev, e) ->
            assertThat(ev.orderId).isEqualTo(e["orderId"].asText())
            assertThat(ev.type.name).isEqualTo(e["type"].asText())
            assertThat(ev.timestamp.atZone(KrxCalendar.KST).toLocalTime().toString()).isEqualTo(e["time"].asText())
            assertThat(ev.symbol).isEqualTo(e["symbol"].asText())
            assertThat(ev.side?.name).isEqualTo(e["side"].asText())
            assertThat(ev.quantity).isEqualByComparingTo(e["quantity"].asText())
            assertThat(ev.price).isEqualByComparingTo(e["price"].asText())
            if (e.has("remainingQuantity")) assertThat(ev.remainingQuantity).isEqualByComparingTo(e["remainingQuantity"].asText())
            if (e.has("originalOrderId")) assertThat(ev.originalOrderId).isEqualTo(e["originalOrderId"].asText()) else assertThat(ev.originalOrderId).isNull()
        }
    }

    @Test
    fun `kis - H0STASP0 호가 프레임 (실측)`() {
        val section = fixture("kis")["orderBook"]
        assertThat(section["channel"].asText()).isEqualTo("ORDER_BOOK")
        assertBooks(section["frames"].flatMap { KisMarketStream.parseOrderBookFrame(it.asText(), today) }, section["expected"])
    }

    @Test
    fun `kis - H0STCNI9 주문 통보 프레임 (복호화 후 평문, 문서 기반)`() {
        val section = fixture("kis")["orderEvents"]
        assertThat(section["measured"].asBoolean()).isFalse()
        assertEvents(section["frames"].flatMap { KisMarketStream.parseOrderEventFrame(it.asText(), today) }, section["expected"])
    }

    @Test
    fun `kiwoom - 0D 호가 프레임 (실측)`() {
        val section = fixture("kiwoom")["orderBook"]
        assertBooks(section["frames"].flatMap { KiwoomMarketStream.parseOrderBook(objectMapper.readTree(it.asText()), today) }, section["expected"])
    }

    @Test
    fun `kiwoom - 00 주문체결 프레임 (문서 기반)`() {
        val section = fixture("kiwoom")["orderEvents"]
        assertThat(section["measured"].asBoolean()).isFalse()
        assertEvents(section["frames"].flatMap { KiwoomMarketStream.parseOrderEvents(objectMapper.readTree(it.asText()), today) }, section["expected"])
    }
}
