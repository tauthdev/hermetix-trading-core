package com.tripleauth.nexttrading.autoconfigure

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.nexttrading.client.NextApiClient
import com.tripleauth.nexttrading.client.NextApiProperties
import com.tripleauth.nexttrading.client.TokenManager
import com.tripleauth.nexttrading.engine.BracketMonitor
import com.tripleauth.nexttrading.engine.OrderExecutor
import com.tripleauth.nexttrading.engine.StrategyEngine
import com.tripleauth.nexttrading.engine.TradingGuard
import com.tripleauth.nexttrading.market.MarketCalendarService
import com.tripleauth.nexttrading.pnl.PnlController
import com.tripleauth.nexttrading.pnl.PnlLogger
import com.tripleauth.nexttrading.pnl.PnlService
import com.tripleauth.nexttrading.strategy.TradingStrategy
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean

@AutoConfiguration
@EnableConfigurationProperties(NextApiProperties::class, NextEngineProperties::class, NextPnlProperties::class)
class NextTradingAutoConfiguration {

    @Bean
    fun nextTradingObjectMapper(): ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    @Bean
    @ConditionalOnMissingBean
    fun tokenManager(
        properties: NextApiProperties,
        @Qualifier("nextTradingObjectMapper") objectMapper: ObjectMapper,
    ): TokenManager = TokenManager(properties, objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun nextApiClient(
        properties: NextApiProperties,
        tokenManager: TokenManager,
        @Qualifier("nextTradingObjectMapper") objectMapper: ObjectMapper,
    ): NextApiClient = NextApiClient(properties, tokenManager, objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun marketCalendarService(nextApiClient: NextApiClient): MarketCalendarService =
        MarketCalendarService(nextApiClient)

    @Bean
    @ConditionalOnMissingBean
    fun tradingGuard(nextApiClient: NextApiClient, engineProperties: NextEngineProperties): TradingGuard =
        TradingGuard(nextApiClient, engineProperties.maxConsecutiveFailures)

    @Bean
    @ConditionalOnMissingBean
    fun bracketMonitor(nextApiClient: NextApiClient): BracketMonitor =
        BracketMonitor(nextApiClient)

    @Bean
    @ConditionalOnMissingBean
    fun orderExecutor(nextApiClient: NextApiClient, bracketMonitor: BracketMonitor, tradingGuard: TradingGuard): OrderExecutor =
        OrderExecutor(nextApiClient, bracketMonitor, tradingGuard)

    @Bean
    @ConditionalOnMissingBean
    fun pnlService(nextApiClient: NextApiClient, pnlProperties: NextPnlProperties): PnlService =
        PnlService(nextApiClient, pnlProperties.initialCapital)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "next.pnl", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun pnlLogger(pnlService: PnlService, pnlProperties: NextPnlProperties): PnlLogger =
        PnlLogger(pnlService, java.time.Duration.ofMinutes(pnlProperties.logIntervalMinutes))

    @Bean
    @ConditionalOnMissingBean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
    fun pnlController(pnlService: PnlService): PnlController = PnlController(pnlService)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "next.engine", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun strategyEngine(
        strategies: List<TradingStrategy>,
        nextApiClient: NextApiClient,
        marketCalendarService: MarketCalendarService,
        orderExecutor: OrderExecutor,
        bracketMonitor: BracketMonitor,
        tradingGuard: TradingGuard,
    ): StrategyEngine =
        StrategyEngine(strategies, nextApiClient, marketCalendarService, orderExecutor, bracketMonitor, tradingGuard)
}
