package com.tripleauth.hermetix.client.dto

import com.fasterxml.jackson.annotation.JsonInclude
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/*
 * 넥스트증권 공개 스펙 v1.3 의 원시(raw) 응답 형태.
 *
 * 공통 모델(Quote/Candle/Holding/OrderResponse …)은 전략 SPI 가 보는 브로커 중립 모델이므로 건드리지 않고,
 * NextApiClient 가 이 원시 형태를 공통 모델로 정규화한다 (KIS/키움 어댑터와 같은 방식).
 * 필드는 스펙상 필수라도 대부분 nullable 로 받아 서버 측 부분 변경에 관대하게 동작한다.
 */

internal data class NextQuotesResponse(val quotes: List<NextQuote> = emptyList())

internal data class NextQuote(
    val symbol: String,
    /** OK · NOT_FOUND · NO_DATA — OK 일 때만 가격류 필드가 유효 */
    val outcome: String? = null,
    /** PRE · REGULAR · AFTER · DAY · CLOSED */
    val session: String? = null,
    val price: BigDecimal? = null,
    val previousClose: BigDecimal? = null,
    val change: BigDecimal? = null,
    /** 전일 대비 등락률 — % 단위 (0.95 = 0.95%) */
    val changeRate: BigDecimal? = null,
    val bidPrice: BigDecimal? = null,
    val bidSize: BigDecimal? = null,
    val askPrice: BigDecimal? = null,
    val askSize: BigDecimal? = null,
    /** 문자열로 내려오지만 Jackson 이 Long 으로 강제 변환한다 */
    val volume: Long? = null,
    val lastTradeAt: String? = null,
    val requestedAt: String? = null,
)

internal data class NextCandlesResponse(
    val symbol: String,
    val interval: String? = null,
    val candles: List<NextCandle> = emptyList(),
    val nextCursor: String? = null,
)

internal data class NextCandle(
    /** 봉 시작 시각 — ISO 8601 · KST */
    val time: String,
    val session: String? = null,
    val open: BigDecimal,
    val high: BigDecimal,
    val low: BigDecimal,
    val close: BigDecimal,
    val volume: Long? = null,
)

internal data class NextCalendarResponse(val calendar: List<NextCalendarDay> = emptyList())

internal data class NextCalendarDay(
    /** 거래소 현지 기준 일자 */
    val date: String,
    /** OPEN · HALF_DAY · CLOSED */
    val status: String? = null,
    val holidayName: String? = null,
    /** 휴장일은 빈 배열. 세션 시각은 ISO 8601 · KST */
    val sessions: List<NextCalendarSession> = emptyList(),
)

internal data class NextCalendarSession(
    /** PRE · REGULAR · AFTER · DAY */
    val type: String? = null,
    val open: String? = null,
    val close: String? = null,
)

internal data class NextInstrumentsResponse(val instruments: List<NextInstrument> = emptyList())

internal data class NextInstrument(
    val symbol: String,
    val name: String,
    /** COMMON_STOCK · PREFERRED_STOCK · DR · ETF */
    val type: String? = null,
    val exchange: String? = null,
    val currency: String? = null,
    /** TRADABLE · SELL_ONLY · BUY_ONLY · SUSPENDED */
    val tradable: String? = null,
    val dayMarketTradable: Boolean? = null,
)

internal data class NextAccountResponse(
    val accountId: String,
    val currency: String? = null,
    val cashAmount: BigDecimal,
    val requestedAt: String? = null,
)

internal data class NextHoldingsResponse(
    val currency: String? = null,
    val holdings: List<NextHolding> = emptyList(),
    val requestedAt: String? = null,
)

internal data class NextHolding(
    val symbol: String,
    val name: String? = null,
    val quantity: BigDecimal,
    val sellableQuantity: BigDecimal? = null,
    val averageBuyPrice: BigDecimal,
    val currentPrice: BigDecimal? = null,
    val purchaseAmount: BigDecimal? = null,
    val evaluationAmount: BigDecimal? = null,
    val evaluationPnl: BigDecimal? = null,
    /** 평가 손익률 — % 단위 */
    val evaluationPnlRate: BigDecimal? = null,
)

internal data class NextBuyingPowerResponse(
    val accountId: String,
    val currency: String? = null,
    val buyingPower: BigDecimal,
    val requestedAt: String? = null,
)

/** 주문 생성/미리보기 요청 본문 (v1.3: clientOrderId · market 필수, timeInForce 는 DAY 만 허용) */
@JsonInclude(JsonInclude.Include.NON_NULL)
internal data class NextOrderRequest(
    val clientOrderId: String,
    val market: String,
    val symbol: String,
    val side: String,
    val orderType: String,
    val quantity: BigDecimal,
    val limitPrice: BigDecimal? = null,
    val timeInForce: String? = null,
)

/** 주문 생성/상세/취소 응답을 한 형태로 받는다 — 생성·취소 응답은 orderId/status/requestedAt 만 온다 */
internal data class NextOrderResponse(
    val orderId: String,
    /** 상세 조회에서 clientOrderId 에 해당하는 필드명 (스펙 v1.3 표기) */
    val requestId: String? = null,
    val market: String? = null,
    val symbol: String? = null,
    val side: String? = null,
    val orderType: String? = null,
    val timeInForce: String? = null,
    val status: String? = null,
    val quantity: BigDecimal? = null,
    /** 금액 주문의 주문 금액 — 다른 채널에서 접수된 주문에만 존재 */
    val amount: BigDecimal? = null,
    val filledQuantity: BigDecimal? = null,
    val limitPrice: BigDecimal? = null,
    val avgFillPrice: BigDecimal? = null,
    val settlementDate: String? = null,
    val rejectReason: String? = null,
    val requestedAt: String? = null,
    val updatedAt: String? = null,
)

internal data class NextOrdersResponse(
    val orders: List<NextOrderResponse> = emptyList(),
    val nextCursor: String? = null,
)

internal data class NextFillsResponse(
    val fills: List<NextFill> = emptyList(),
    val nextCursor: String? = null,
)

internal data class NextFill(
    val orderId: String? = null,
    val market: String? = null,
    val symbol: String? = null,
    val side: String? = null,
    val quantity: BigDecimal? = null,
    val price: BigDecimal? = null,
    val amount: BigDecimal? = null,
    val fee: BigDecimal? = null,
    val tax: BigDecimal? = null,
    val filledAt: String? = null,
)

/** v1.3 시각 규약: ISO 8601, 오프셋 생략 시 KST */
internal object NextTime {

    val KST: ZoneId = ZoneId.of("Asia/Seoul")
    val NEW_YORK: ZoneId = ZoneId.of("America/New_York")

    fun parseInstant(value: String?): Instant? {
        val text = value?.trim()?.takeIf { it.isNotEmpty() } ?: return null
        val parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(text, ZonedDateTime::from, LocalDateTime::from)
        return when (parsed) {
            is ZonedDateTime -> parsed.toInstant()
            is LocalDateTime -> parsed.atZone(KST).toInstant()
            else -> null
        }
    }
}
