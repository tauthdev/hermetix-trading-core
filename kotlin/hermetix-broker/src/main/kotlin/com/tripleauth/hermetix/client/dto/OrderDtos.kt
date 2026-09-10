package com.tripleauth.hermetix.client.dto

import com.fasterxml.jackson.annotation.JsonEnumDefaultValue
import com.fasterxml.jackson.annotation.JsonInclude
import java.math.BigDecimal
import java.time.Instant

enum class OrderSide { BUY, SELL }

enum class OrderType { MARKET, LIMIT, MOO, LOO, MOC, LOC }

enum class TimeInForce { DAY, GTC }

/**
 * 주문 상태 (넥스트증권 공개 스펙 v1.3 부록 D 의 7종 + 미지 값 폴백).
 *
 * [PENDING_CANCEL] 은 취소 요청이 접수됐지만 아직 확정되지 않은 상태 — 원주문이 체결될 수 있으므로
 * OPEN 으로 분류한다 (재주문 금지). 취소 API 응답의 status 로 내려온다.
 */
enum class OrderStatus {
    SUBMITTED, PARTIALLY_FILLED, PENDING_CANCEL, FILLED, CANCELED, REJECTED, EXPIRED,

    @JsonEnumDefaultValue
    UNKNOWN;

    val isOpen: Boolean
        get() = this == SUBMITTED || this == PARTIALLY_FILLED || this == PENDING_CANCEL
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

/** 주문 미리보기 요청 (넥스트증권 v1.3). 본문은 주문 생성과 같고 clientOrderId 는 무시된다 */
@JsonInclude(JsonInclude.Include.NON_NULL)
data class PreviewOrderRequest(
    val symbol: String,
    val side: OrderSide,
    val orderType: OrderType,
    val quantity: BigDecimal,
    val limitPrice: BigDecimal? = null,
    val timeInForce: TimeInForce = TimeInForce.DAY,
)

/** 주문 미리보기 응답 (넥스트증권 v1.3) — 접수 없이 검증·추정만 */
data class PreviewOrderResponse(
    val valid: Boolean,
    val market: String? = null,
    val currency: String? = null,
    val estimatedQuantity: BigDecimal? = null,
    val estimatedPrice: BigDecimal? = null,
    /** 수량 × 단가 */
    val orderAmount: BigDecimal? = null,
    val estimatedFee: BigDecimal? = null,
    val estimatedTax: BigDecimal? = null,
    /** 매수: 주문금액+수수료+세금, 매도: 예상 수령액 */
    val estimatedTotal: BigDecimal? = null,
    /** valid=false 일 때의 거부 사유 */
    val rejectReason: String? = null,
    val rejectCode: String? = null,
)
