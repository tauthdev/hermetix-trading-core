package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.strategy.Signal
import com.tripleauth.hermetix.strategy.StrategyContext
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap

/**
 * 소프트웨어 브라켓(익절/손절) 관리자.
 *
 * 모의투자 서버가 네이티브 BRACKET/STOP 주문을 지원하지 않으므로,
 * 진입 주문 체결을 추적하다가 현재가가 익절/손절 가격에 닿으면 시장가 청산 시그널을 만든다.
 * 상태는 메모리에만 유지된다 — 앱 재시작 시 브라켓은 사라지므로 전략에서 보유 포지션을
 * 다시 점검하는 로직을 두는 것을 권장한다.
 */
class BracketMonitor(
    private val brokerClient: BrokerClient,
) {

    private val logger = KotlinLogging.logger { }

    private val brackets = ConcurrentHashMap<String, Bracket>()

    fun register(entryOrderId: String, symbol: String, quantity: BigDecimal, takeProfitPrice: BigDecimal?, stopLossPrice: BigDecimal?) {
        if (takeProfitPrice == null && stopLossPrice == null) return
        brackets[entryOrderId] = Bracket(
            entryOrderId = entryOrderId,
            symbol = symbol,
            quantity = quantity,
            takeProfitPrice = takeProfitPrice,
            stopLossPrice = stopLossPrice,
        )
        logger.info { "bracket registered / order=$entryOrderId $symbol qty=$quantity tp=$takeProfitPrice sl=$stopLossPrice" }
    }

    /** 매 틱마다 호출된다. 청산이 필요한 브라켓을 Sell 시그널로 반환한다. */
    fun check(context: StrategyContext): List<Signal.Sell> {
        val signals = mutableListOf<Signal.Sell>()

        for (bracket in brackets.values) {
            if (!bracket.active) {
                resolveEntry(bracket)
                if (!bracket.active) continue
            }

            val price = context.quote(bracket.symbol)?.price ?: continue
            val takeProfitHit = bracket.takeProfitPrice != null && price >= bracket.takeProfitPrice
            val stopLossHit = bracket.stopLossPrice != null && price <= bracket.stopLossPrice

            if (takeProfitHit || stopLossHit) {
                val held = context.holding(bracket.symbol)?.quantity ?: BigDecimal.ZERO
                val quantity = bracket.quantity.min(held)

                brackets.remove(bracket.entryOrderId)

                if (quantity <= BigDecimal.ZERO) {
                    logger.warn { "bracket hit but no holdings / ${bracket.symbol} (수동 매도된 것으로 추정)" }
                    continue
                }

                logger.info { "bracket ${if (takeProfitHit) "TAKE-PROFIT" else "STOP-LOSS"} / ${bracket.symbol} price=$price" }
                signals += Signal.Sell(symbol = bracket.symbol, quantity = quantity)
            }
        }

        return signals
    }

    /** 진입 주문의 체결 여부를 확인한다. 취소/거부되었으면 브라켓을 폐기한다. */
    private fun resolveEntry(bracket: Bracket) {
        val order = runCatching { brokerClient.getOrder(bracket.entryOrderId) }
            .onFailure { logger.warn { "bracket entry lookup failed / ${bracket.entryOrderId}: ${it.message}" } }
            .getOrNull() ?: return

        when {
            order.status == OrderStatus.FILLED -> {
                bracket.active = true
                logger.info { "bracket activated / entry filled ${bracket.entryOrderId}" }
            }

            !order.status.isOpen -> {
                brackets.remove(bracket.entryOrderId)
                logger.info { "bracket dropped / entry ${order.status} ${bracket.entryOrderId}" }
            }
        }
    }

    fun activeCount(): Int = brackets.size

    private class Bracket(
        val entryOrderId: String,
        val symbol: String,
        val quantity: BigDecimal,
        val takeProfitPrice: BigDecimal?,
        val stopLossPrice: BigDecimal?,
        @Volatile var active: Boolean = false,
    )
}
