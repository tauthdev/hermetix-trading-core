package com.tripleauth.nexttrading.client.dto

import java.math.BigDecimal

data class AccountResponse(
    val accountId: String,
    val name: String?,
    val currency: String,
    val cash: BigDecimal,
    val portfolioValue: BigDecimal,
    val status: String,
)

data class HoldingsResponse(
    val holdings: List<Holding>,
    val summary: HoldingsSummary? = null,
)

data class Holding(
    val symbol: String,
    val quantity: BigDecimal,
    val avgEntryPrice: BigDecimal,
    val currentPrice: BigDecimal?,
    val marketValue: BigDecimal?,
    val unrealizedPnl: BigDecimal?,
    val unrealizedPnlRate: BigDecimal?,
)

data class HoldingsSummary(
    val totalMarketValue: BigDecimal?,
    val totalUnrealizedPnl: BigDecimal?,
)

data class BuyingPowerResponse(
    val accountId: String,
    val currency: String,
    val buyingPower: BigDecimal,
)
