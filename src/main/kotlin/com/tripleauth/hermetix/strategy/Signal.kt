package com.tripleauth.hermetix.strategy

import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.dto.TimeInForce
import java.math.BigDecimal

/**
 * 전략이 엔진에게 전달하는 매매 의사결정.
 *
 * 전략은 "무엇을 하고 싶은지"만 표현하고, 주문 제출/체결 추적/익절·손절 관리는 엔진이 담당한다.
 */
sealed class Signal {

    /**
     * 매수 진입.
     *
     * [takeProfitPrice]/[stopLossPrice] 를 지정하면 엔진이 체결 이후 해당 가격 도달 시
     * 자동으로 청산 주문을 낸다 (소프트웨어 브라켓 — 모의투자 서버가 네이티브
     * BRACKET 주문을 지원하지 않는 동안의 기본 동작).
     */
    data class Buy(
        val symbol: String,
        val quantity: BigDecimal,
        val orderType: OrderType = OrderType.MARKET,
        val limitPrice: BigDecimal? = null,
        val timeInForce: TimeInForce = TimeInForce.DAY,
        val takeProfitPrice: BigDecimal? = null,
        val stopLossPrice: BigDecimal? = null,
    ) : Signal()

    /** 매도 청산. 보유 수량 내에서만 실행된다 (공매도 불가). */
    data class Sell(
        val symbol: String,
        val quantity: BigDecimal,
        val orderType: OrderType = OrderType.MARKET,
        val limitPrice: BigDecimal? = null,
        val timeInForce: TimeInForce = TimeInForce.DAY,
    ) : Signal()

    /** 미체결 주문 취소 */
    data class Cancel(
        val orderId: String,
    ) : Signal()
}
