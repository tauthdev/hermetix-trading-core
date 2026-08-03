package com.tripleauth.nexttrading.strategy

import com.tripleauth.nexttrading.client.dto.AccountResponse
import com.tripleauth.nexttrading.client.dto.Candle
import com.tripleauth.nexttrading.client.dto.Holding
import com.tripleauth.nexttrading.client.dto.OrderResponse
import com.tripleauth.nexttrading.client.dto.Quote
import java.math.BigDecimal
import java.time.ZonedDateTime

/**
 * 전략 호출 시점의 시장/계좌 스냅샷.
 *
 * 엔진이 매 틱마다 만들어 전달하며, 전략은 이 데이터만으로 의사결정한다.
 * (전략 코드에서 API 를 직접 호출하지 않는 것이 규약이다)
 */
data class StrategyContext(
    /** 전략 호출 시각 (엔진 기준) */
    val now: ZonedDateTime,
    /** 심볼별 현재가 스냅샷 */
    val quotes: Map<String, Quote>,
    /** 심볼별 캔들 (과거 → 최신 순서, spec.candleLimit 개) */
    val candles: Map<String, List<Candle>>,
    /** 계좌 정보 */
    val account: AccountResponse,
    /** 심볼별 보유 포지션 */
    val holdings: Map<String, Holding>,
    /** 미체결 주문 목록 */
    val openOrders: List<OrderResponse>,
    /** 주문 가능 현금 */
    val buyingPower: BigDecimal,
) {

    fun quote(symbol: String): Quote? = quotes[symbol]

    fun candles(symbol: String): List<Candle> = candles[symbol] ?: emptyList()

    fun holding(symbol: String): Holding? = holdings[symbol]

    fun hasPosition(symbol: String): Boolean =
        (holdings[symbol]?.quantity ?: BigDecimal.ZERO) > BigDecimal.ZERO

    fun openOrders(symbol: String): List<OrderResponse> =
        openOrders.filter { it.symbol == symbol }

    fun hasOpenOrder(symbol: String): Boolean = openOrders(symbol).isNotEmpty()
}
