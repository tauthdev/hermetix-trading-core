package com.tripleauth.nexttrading.client.dto

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue
import com.fasterxml.jackson.annotation.JsonInclude
import java.math.BigDecimal
import java.time.Instant

enum class OrderSide { BUY, SELL }

enum class OrderType { MARKET, LIMIT, MOO, LOO, MOC, LOC }

enum class TimeInForce { DAY, GTC }

enum class OrderStatus {
    SUBMITTED, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED,

    @JsonEnumDefaultValue
    UNKNOWN;

    val isOpen: Boolean
        get() = this == SUBMITTED || this == PARTIALLY_FILLED
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CreateOrderRequest(
    val symbol: String,
    val side: OrderSide,
    val orderType: OrderType,
    val quantity: BigDecimal,
    val limitPrice: BigDecimal? = null,
    val timeInForce: TimeInForce = TimeInForce.DAY,
    val clientOrderId: String? = null,
)

data class OrderResponse(
    val orderId: String,
    val clientOrderId: String? = null,
    val status: OrderStatus,
    val symbol: String? = null,
    val side: OrderSide? = null,
    val orderType: OrderType? = null,
    val quantity: BigDecimal? = null,
    val notional: BigDecimal? = null,
    val limitPrice: BigDecimal? = null,
    val filledQuantity: BigDecimal? = null,
    val avgFillPrice: BigDecimal? = null,
    val submittedAt: Instant? = null,
    val filledAt: Instant? = null,
    val canceledAt: Instant? = null,
    val rejectReason: String? = null,
)

data class OrdersResponse(
    val orders: List<OrderResponse>,
)

data class FillsResponse(
    val fills: List<Fill>,
)

data class Fill(
    val fillId: String? = null,
    val orderId: String? = null,
    val symbol: String? = null,
    val side: OrderSide? = null,
    val quantity: BigDecimal? = null,
    val price: BigDecimal? = null,
    val amount: BigDecimal? = null,
    val timestamp: Instant? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class PreviewOrderRequest(
    val symbol: String,
    val side: OrderSide,
    val orderType: OrderType,
    val quantity: BigDecimal,
    val limitPrice: BigDecimal? = null,
    val timeInForce: TimeInForce = TimeInForce.DAY,
)

data class PreviewOrderResponse(
    val valid: Boolean,
    val estimated: PreviewEstimated? = null,
    val warnings: List<Any> = emptyList(),
    val errors: List<Any> = emptyList(),
)

data class PreviewEstimated(
    val orderValue: BigDecimal?,
    val commission: BigDecimal?,
    val totalCost: BigDecimal?,
    val buyingPowerAfter: BigDecimal?,
)
