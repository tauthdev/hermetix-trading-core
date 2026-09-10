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

/** 환율 (넥스트증권 v1.3 — 하나은행 고시). 넥스트 전용 확장 API */
data class ExchangeRateResponse(
    val baseCurrency: String,
    val quoteCurrency: String,
    /** 고객이 외화를 팔 때 적용되는 환율 */
    val bidRate: BigDecimal,
    /** 고객이 외화를 살 때 적용되는 환율 */
    val askRate: BigDecimal,
    val requestedAt: Instant?,
)

data class InstrumentsResponse(
    val instruments: List<Instrument>,
)

/**
 * 종목 (넥스트증권 v1.3). `tradable` 은 서버의 거래 가능 상태(TRADABLE · SELL_ONLY · BUY_ONLY · SUSPENDED)를
 * 편의상 Boolean 으로 요약한 값이고, 원본 상태는 [tradableStatus] 에 있다.
 */
data class Instrument(
    val symbol: String,
    val name: String,
    /** COMMON_STOCK · PREFERRED_STOCK · DR · ETF */
    val type: String?,
    val exchange: String?,
    val currency: String?,
    val tradable: Boolean,
    val tradableStatus: String?,
    /** 주간거래(DAY 세션) 가능 여부 */
    val dayMarketTradable: Boolean?,
)

data class InstrumentDetailResponse(
    val symbol: String,
    val name: String,
    val type: String?,
    val exchange: String?,
    val currency: String?,
    val tradable: Boolean,
    val tradableStatus: String?,
    val dayMarketTradable: Boolean?,
)
