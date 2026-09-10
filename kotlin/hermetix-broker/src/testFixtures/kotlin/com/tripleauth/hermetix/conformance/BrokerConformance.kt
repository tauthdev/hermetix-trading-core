package com.tripleauth.hermetix.conformance

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.MarketSymbol
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

/**
 * 어댑터 컨포먼스 검증 — 모든 [BrokerClient] 구현이 지켜야 하는 공통 모델 규약을 한 시나리오로 확인한다.
 *
 * 시세 → 캔들 → 캘린더 → 계좌 → 보유 → 매수가능 → 주문 → 조회 → 취소 → 체결 순으로 호출하고 위반을 모은다.
 * 상태 전이(취소 후 재조회)는 검사하지 않는다 — 정적 골든 픽스처로 재생 가능해야 하기 때문이다.
 * 새 어댑터 기여 조건은 `verify(...).violations` 가 비어 있는 것이다 (`conformance/README.md`).
 *
 * `hermetix-broker` 의 test-fixtures 아티팩트로 배포되어 외부 어댑터 프로젝트도 `testImplementation(testFixtures(...))` 로 쓸 수 있다.
 */
object BrokerConformance {

    data class Scenario(
        val symbol: String,
        val quantity: BigDecimal = BigDecimal.ONE,
        val limitPrice: BigDecimal,
    )

    data class Report(val steps: List<String>, val violations: List<String>) {
        val passed: Boolean get() = violations.isEmpty()
        override fun toString(): String =
            if (passed) "conformance OK (${steps.size} steps)" else "conformance FAILED:\n - " + violations.joinToString("\n - ")
    }

    fun verify(client: BrokerClient, scenario: Scenario): Report {
        val steps = mutableListOf<String>()
        val violations = mutableListOf<String>()
        fun check(condition: Boolean, message: () -> String) { if (!condition) violations += message() }
        fun step(name: String, block: () -> Unit) {
            steps += name
            runCatching(block).onFailure { violations += "$name: 예외 ${it::class.simpleName}: ${it.message}" }
        }

        val caps = client.capabilities
        step("capabilities") {
            check(caps.brokerId.isNotBlank()) { "capabilities.brokerId 가 비어 있다" }
            check(caps.currency.isNotBlank()) { "capabilities.currency 가 비어 있다" }
            check(caps.market in caps.markets) { "capabilities.market(${caps.market}) 이 markets(${caps.markets}) 에 없다" }
            check(client.environment in caps.environments) { "environment(${client.environment}) 이 선언된 environments(${caps.environments}) 에 없다" }
            check(caps.candleIntervals.isNotEmpty()) { "candleIntervals 가 비어 있다" }
        }

        step("quotes") {
            val quotes = client.getQuotes(listOf(scenario.symbol)).quotes
            check(quotes.size == 1) { "quotes: 1건을 기대했는데 ${quotes.size}건" }
            quotes.firstOrNull()?.let { q ->
                check(q.symbol == scenario.symbol) { "quotes: 심볼은 요청 표기 그대로여야 한다 (요청=${scenario.symbol}, 응답=${q.symbol})" }
                check(q.price > BigDecimal.ZERO) { "quotes: price 는 양수여야 한다 (${q.price})" }
                check(q.timestamp != Instant.EPOCH) { "quotes: timestamp 가 EPOCH 이다" }
                check(q.volume >= 0) { "quotes: volume 음수 (${q.volume})" }
                q.changeRate?.let { check(it.abs() <= BigDecimal.TEN) { "quotes: changeRate 는 비율이어야 한다 (% 로 보임: $it)" } }
            }
        }

        step("candles") {
            val interval = caps.candleIntervals.first()
            val candles = client.getCandles(scenario.symbol, interval, 3).candles
            check(candles.isNotEmpty()) { "candles: 비어 있다" }
            check(candles.size <= 3) { "candles: limit=3 을 넘겼다 (${candles.size})" }
            check(candles.zipWithNext().all { (a, b) -> a.timestamp < b.timestamp }) { "candles: 시각이 오름차순이 아니다" }
            candles.forEach { c ->
                check(c.low <= c.high && c.open in c.low..c.high && c.close in c.low..c.high) { "candles: OHLC 범위 위반 ${c.timestamp} o=${c.open} h=${c.high} l=${c.low} c=${c.close}" }
                check(c.volume >= 0) { "candles: volume 음수" }
                check(c.timestamp != Instant.EPOCH) { "candles: timestamp 가 EPOCH 이다" }
            }
        }

        step("calendar") {
            val days = client.getCalendar().calendar
            check(days.isNotEmpty()) { "calendar: 비어 있다" }
            days.forEach { d ->
                check(runCatching { LocalDate.parse(d.date) }.isSuccess) { "calendar: 날짜 형식 위반 ${d.date}" }
                check(runCatching { ZoneId.of(d.timezone) }.isSuccess) { "calendar: 타임존 위반 ${d.timezone}" }
                if (d.open) {
                    val regular = d.sessions?.regular
                    check(regular != null) { "calendar: 개장일 ${d.date} 에 정규장 세션이 없다" }
                    regular?.let {
                        check(runCatching { LocalTime.parse(it.start); LocalTime.parse(it.end) }.isSuccess) { "calendar: 세션 시각은 HH:mm 이어야 한다 (${it.start}~${it.end})" }
                    }
                }
            }
        }

        step("account") {
            val account = client.getAccount()
            check(account.accountId.isNotBlank()) { "account: accountId 비어 있음" }
            check(account.currency == caps.currency) { "account: currency(${account.currency}) 가 capabilities.currency(${caps.currency}) 와 다르다" }
            check(account.cash >= BigDecimal.ZERO) { "account: cash 음수" }
            check(account.portfolioValue >= BigDecimal.ZERO) { "account: portfolioValue 음수" }
        }

        step("holdings") {
            client.getHoldings().holdings.forEach { h ->
                check(h.symbol.isNotBlank()) { "holdings: 심볼 비어 있음" }
                check(MarketSymbol.parse(h.symbol).market.let { it == null || it in caps.markets }) { "holdings: 미지원 시장 접두 ${h.symbol}" }
                check(h.quantity > BigDecimal.ZERO) { "holdings: quantity 는 양수여야 한다 (${h.symbol}=${h.quantity})" }
                check(h.avgEntryPrice >= BigDecimal.ZERO) { "holdings: avgEntryPrice 음수 (${h.symbol})" }
                h.unrealizedPnlRate?.let { check(it.abs() <= BigDecimal.TEN) { "holdings: unrealizedPnlRate 는 비율이어야 한다 (% 로 보임: ${h.symbol}=$it)" } }
            }
        }

        step("buyingPower") {
            val bp = client.getBuyingPower()
            check(bp.buyingPower >= BigDecimal.ZERO) { "buyingPower 음수" }
            check(bp.currency == caps.currency) { "buyingPower: currency 불일치 (${bp.currency})" }
        }

        var orderId: String? = null
        step("createOrder") {
            val order = client.createOrder(
                CreateOrderRequest(
                    symbol = scenario.symbol, side = OrderSide.BUY, orderType = OrderType.LIMIT,
                    quantity = scenario.quantity, limitPrice = scenario.limitPrice, clientOrderId = "conformance-1",
                ),
            )
            check(order.orderId.isNotBlank()) { "createOrder: orderId 비어 있음" }
            check(order.status.isOpen) { "createOrder: 접수 직후 상태는 미체결(open)이어야 한다 (${order.status})" }
            order.symbol?.let { check(MarketSymbol.matches(it, scenario.symbol)) { "createOrder: 심볼 불일치 ($it)" } }
            orderId = order.orderId
        }

        val id = orderId
        if (id != null) {
            step("getOrder") {
                val order = client.getOrder(id)
                check(order.orderId == id) { "getOrder: orderId 불일치 (${order.orderId})" }
                check(order.status != OrderStatus.UNKNOWN) { "getOrder: status UNKNOWN" }
            }
            step("getOrders") {
                val orders = client.getOrders().orders
                check(orders.any { it.orderId == id }) { "getOrders: 방금 낸 주문 $id 가 목록에 없다" }
                orders.forEach { check(it.status != OrderStatus.UNKNOWN) { "getOrders: status UNKNOWN (${it.orderId})" } }
            }
            step("cancelOrder") {
                val canceled = client.cancelOrder(id)
                check(canceled.orderId == id) { "cancelOrder: orderId 불일치 (${canceled.orderId})" }
                check(canceled.status == OrderStatus.PENDING_CANCEL || canceled.status == OrderStatus.CANCELED) { "cancelOrder: status 는 PENDING_CANCEL/CANCELED 이어야 한다 (${canceled.status})" }
            }
        }

        step("fills") {
            client.getFills().fills.forEach { f ->
                f.quantity?.let { check(it > BigDecimal.ZERO) { "fills: quantity 는 양수 (${f.orderId})" } }
                f.price?.let { check(it > BigDecimal.ZERO) { "fills: price 는 양수 (${f.orderId})" } }
            }
        }

        return Report(steps, violations)
    }
}
