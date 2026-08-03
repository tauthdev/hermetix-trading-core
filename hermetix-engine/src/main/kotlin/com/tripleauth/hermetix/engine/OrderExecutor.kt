package com.tripleauth.hermetix.engine

import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.InsufficientFundsError
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.strategy.Signal
import com.tripleauth.hermetix.strategy.StrategyContext
import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal
import java.util.UUID

/**
 * 시그널을 실제 주문으로 변환/실행한다.
 *
 * - 매도는 보유 수량으로 클램프한다 (공매도 방지)
 * - 모든 주문에 `{전략이름}-{uuid}` 형식의 clientOrderId 를 부여한다 (24h 멱등)
 * - Buy 에 익절/손절 가격이 있으면 BracketMonitor 에 등록한다
 */
class OrderExecutor(
    private val brokerClient: BrokerClient,
    private val bracketMonitor: BracketMonitor,
    private val tradingGuard: TradingGuard,
) {

    private val logger = KotlinLogging.logger { }

    fun execute(strategyName: String, signals: List<Signal>, context: StrategyContext) {
        for (signal in signals) {
            if (tradingGuard.isHalted) {
                logger.warn { "[$strategyName] halted - signal skipped: $signal" }
                continue
            }

            try {
                when (signal) {
                    is Signal.Buy -> buy(strategyName, signal)
                    is Signal.Sell -> sell(strategyName, signal, context)
                    is Signal.Cancel -> cancel(strategyName, signal)
                }
            } catch (e: InsufficientFundsError) {
                logger.warn { "[$strategyName] 주문가능금액 부족으로 시그널 스킵: $signal" }
            } catch (e: Exception) {
                logger.error(e) { "[$strategyName] signal 실행 실패: $signal" }
            }
        }
    }

    private fun buy(strategyName: String, signal: Signal.Buy) {
        val order = brokerClient.createOrder(
            CreateOrderRequest(
                symbol = signal.symbol,
                side = OrderSide.BUY,
                orderType = signal.orderType,
                quantity = signal.quantity,
                limitPrice = signal.limitPrice,
                timeInForce = signal.timeInForce,
                clientOrderId = clientOrderId(strategyName),
            ),
        )

        logger.info { "[$strategyName] BUY 접수 / ${signal.symbol} qty=${signal.quantity} type=${signal.orderType} orderId=${order.orderId}" }

        bracketMonitor.register(
            entryOrderId = order.orderId,
            symbol = signal.symbol,
            quantity = signal.quantity,
            takeProfitPrice = signal.takeProfitPrice,
            stopLossPrice = signal.stopLossPrice,
        )
    }

    private fun sell(strategyName: String, signal: Signal.Sell, context: StrategyContext) {
        val held = context.holding(signal.symbol)?.quantity ?: BigDecimal.ZERO
        val quantity = signal.quantity.min(held)

        if (quantity <= BigDecimal.ZERO) {
            logger.warn { "[$strategyName] SELL 스킵 / ${signal.symbol} 보유 수량 없음 (요청=${signal.quantity})" }
            return
        }

        val order = brokerClient.createOrder(
            CreateOrderRequest(
                symbol = signal.symbol,
                side = OrderSide.SELL,
                orderType = signal.orderType,
                quantity = quantity,
                limitPrice = signal.limitPrice,
                timeInForce = signal.timeInForce,
                clientOrderId = clientOrderId(strategyName),
            ),
        )

        logger.info { "[$strategyName] SELL 접수 / ${signal.symbol} qty=$quantity type=${signal.orderType} orderId=${order.orderId}" }
    }

    private fun cancel(strategyName: String, signal: Signal.Cancel) {
        val order = brokerClient.cancelOrder(signal.orderId)
        logger.info { "[$strategyName] CANCEL / orderId=${signal.orderId} status=${order.status}" }
    }

    private fun clientOrderId(strategyName: String): String? =
        if (brokerClient.capabilities.clientOrderId) "$strategyName-${UUID.randomUUID().toString().substring(0, 8)}" else null
}
