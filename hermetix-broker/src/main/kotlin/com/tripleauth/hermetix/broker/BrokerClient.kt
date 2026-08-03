package com.tripleauth.hermetix.broker

import com.tripleauth.hermetix.client.dto.AccountResponse
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.FillsResponse
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.QuotesResponse

/**
 * 증권사(브로커) 추상화 인터페이스.
 *
 * 엔진/브라켓/가드/PnL 등 코어의 모든 컴포넌트는 이 인터페이스에만 의존한다.
 * 새 증권사를 지원하려면 이 인터페이스의 구현체(어댑터)를 추가하면 되고,
 * 전략 코드는 한 줄도 바꿀 필요가 없다.
 *
 * 현재 어댑터:
 * - `next` — 넥스트증권 모의투자 ([com.tripleauth.hermetix.client.NextApiClient])
 *
 * 어댑터 구현 규약:
 * - 시세/캔들/계좌/보유/주문의 의미는 [com.tripleauth.hermetix.client.dto] 의 공통 모델을 따른다
 * - 인증(토큰 갱신 포함)은 어댑터 내부에서 처리한다 — 호출자는 인증을 모른다
 * - 실패는 [com.tripleauth.hermetix.client.NextApiException] 또는 그에 준하는 RuntimeException 으로 던진다
 * - 장 운영시간 정보는 [getCalendar] 로 제공한다 (거래소 타임존 포함)
 */
interface BrokerClient {

    /** 이 브로커가 지원하는 기능 선언 — 엔진이 이를 보고 적응한다 */
    val capabilities: BrokerCapabilities

    fun getQuotes(symbols: List<String>): QuotesResponse

    fun getCandles(symbol: String, interval: CandleInterval, limit: Int? = null): CandlesResponse

    fun getCalendar(): CalendarResponse

    fun getAccount(): AccountResponse

    fun getHoldings(): HoldingsResponse

    fun getBuyingPower(): BuyingPowerResponse

    fun createOrder(request: CreateOrderRequest): OrderResponse

    fun getOrders(): OrdersResponse

    fun getOrder(orderId: String): OrderResponse

    fun cancelOrder(orderId: String): OrderResponse

    fun getFills(): FillsResponse
}
