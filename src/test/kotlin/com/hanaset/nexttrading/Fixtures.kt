package com.hanaset.nexttrading

import com.hanaset.nexttrading.client.dto.AccountResponse
import com.hanaset.nexttrading.client.dto.Candle
import com.hanaset.nexttrading.client.dto.Holding
import com.hanaset.nexttrading.client.dto.OrderResponse
import com.hanaset.nexttrading.client.dto.Quote
import com.hanaset.nexttrading.strategy.StrategyContext
import java.math.BigDecimal
import java.time.Instant
import java.time.ZonedDateTime

object Fixtures {

    fun quote(symbol: String = "AAPL", price: String = "300") = Quote(
        symbol = symbol,
        price = BigDecimal(price),
        bidPrice = null,
        askPrice = null,
        volume = 1_000L,
        change = null,
        changeRate = null,
        timestamp = Instant.now(),
    )

    fun holding(symbol: String = "AAPL", quantity: String = "10") = Holding(
        symbol = symbol,
        quantity = BigDecimal(quantity),
        avgEntryPrice = BigDecimal("280"),
        currentPrice = BigDecimal("300"),
        marketValue = null,
        unrealizedPnl = null,
        unrealizedPnlRate = null,
    )

    fun account() = AccountResponse(
        accountId = "acc_main",
        name = "주력 계좌",
        currency = "USD",
        cash = BigDecimal("10000"),
        portfolioValue = BigDecimal("20000"),
        status = "ACTIVE",
    )

    fun context(
        quotes: Map<String, Quote> = mapOf("AAPL" to quote()),
        candles: Map<String, List<Candle>> = emptyMap(),
        holdings: Map<String, Holding> = emptyMap(),
        openOrders: List<OrderResponse> = emptyList(),
    ) = StrategyContext(
        now = ZonedDateTime.now(),
        quotes = quotes,
        candles = candles,
        account = account(),
        holdings = holdings,
        openOrders = openOrders,
        buyingPower = BigDecimal("10000"),
    )
}
