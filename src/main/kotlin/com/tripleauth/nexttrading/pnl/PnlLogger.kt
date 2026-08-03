package com.tripleauth.nexttrading.pnl

import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import java.time.Duration

/**
 * 주기적으로 계좌 수익률을 로그로 남긴다.
 *
 * ```
 * PNL / portfolio=21363.45 cash=17322.83 unrealized=+271.73 return=+6.82% | AAPL +54.32(+9.64%) MSFT +184.80(+24.82%)
 * ```
 */
class PnlLogger(
    private val pnlService: PnlService,
    private val interval: Duration,
) : DisposableBean {

    private val logger = KotlinLogging.logger { }

    private val scheduler = ThreadPoolTaskScheduler().apply {
        poolSize = 1
        setThreadNamePrefix("pnl-logger-")
        initialize()
    }

    @EventListener(ApplicationReadyEvent::class)
    fun start() {
        scheduler.scheduleWithFixedDelay({ logReport() }, interval)
        logger.info { "PnL logger started / interval=$interval" }
    }

    internal fun logReport() {
        runCatching {
            val report = pnlService.report()

            val returnPart = report.totalReturnRate
                ?.let { " return=${"%+.2f".format(it.toDouble() * 100)}%" }
                ?: ""

            val holdingsPart = report.holdings.joinToString(" ") {
                val pnl = it.unrealizedPnl?.toDouble() ?: 0.0
                val rate = it.unrealizedPnlRate?.toDouble()?.times(100) ?: 0.0
                "${it.symbol} ${"%+.2f".format(pnl)}(${"%+.2f".format(rate)}%)"
            }

            logger.info {
                "PNL / portfolio=${report.portfolioValue} cash=${report.cash} " +
                    "unrealized=${"%+.2f".format(report.totalUnrealizedPnl.toDouble())}$returnPart" +
                    if (holdingsPart.isNotBlank()) " | $holdingsPart" else ""
            }
        }.onFailure { logger.warn { "PnL 리포트 실패: ${it.message}" } }
    }

    override fun destroy() {
        scheduler.shutdown()
    }
}
