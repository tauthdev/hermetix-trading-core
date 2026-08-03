package com.tripleauth.hermetix.client.dto

import java.math.BigDecimal
import java.time.Instant

data class QuotesResponse(
    val quotes: List<Quote>,
)

data class Quote(
    val symbol: String,
    val price: BigDecimal,
    val bidPrice: BigDecimal?,
    val askPrice: BigDecimal?,
    val volume: Long,
    val change: BigDecimal?,
    val changeRate: BigDecimal?,
    val timestamp: Instant,
)

data class CandlesResponse(
    val symbol: String,
    val interval: String,
    val candles: List<Candle>,
)

data class Candle(
    val timestamp: Instant,
    val open: BigDecimal,
    val high: BigDecimal,
    val low: BigDecimal,
    val close: BigDecimal,
    val volume: Long,
)

enum class CandleInterval(val value: String) {
    MIN_1("1m"), MIN_5("5m"), HOUR_1("1h"), DAY_1("1d"),
}

data class CalendarResponse(
    val calendar: List<MarketDay>,
)

data class MarketDay(
    val date: String,
    val open: Boolean,
    val sessions: MarketSessions?,
    val timezone: String,
    val holiday: String?,
)

data class MarketSessions(
    val preMarket: SessionHours?,
    val regular: SessionHours?,
    val afterHours: SessionHours?,
)

data class SessionHours(
    val start: String,
    val end: String,
)

data class ExchangeRateResponse(
    val baseCurrency: String,
    val quoteCurrency: String,
    val rate: BigDecimal,
    val rateChangeType: String?,
    val validFrom: Instant?,
    val validUntil: Instant?,
)

data class InstrumentsResponse(
    val instruments: List<Instrument>,
    val nextCursor: String?,
)

data class Instrument(
    val symbol: String,
    val name: String,
    val type: String?,
    val exchange: String?,
    val currency: String?,
    val tradable: Boolean,
)

data class InstrumentDetailResponse(
    val symbol: String,
    val name: String,
    val type: String?,
    val exchange: String?,
    val currency: String?,
    val tradable: Boolean,
    val fractionable: Boolean?,
    val shortable: Boolean?,
    val easyToBorrow: Boolean?,
    val marginable: Boolean?,
    val minOrderSize: BigDecimal?,
    val status: String?,
    val restrictions: List<String> = emptyList(),
)
