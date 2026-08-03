package com.tripleauth.hermetix.autoconfigure

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.NextApiClient
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import com.tripleauth.hermetix.client.NextApiProperties
import com.tripleauth.hermetix.client.TokenManager
import com.tripleauth.hermetix.engine.BracketMonitor
import com.tripleauth.hermetix.engine.OrderExecutor
import com.tripleauth.hermetix.engine.StrategyEngine
import com.tripleauth.hermetix.engine.TradingGuard
import com.tripleauth.hermetix.market.MarketCalendarService
import com.tripleauth.hermetix.pnl.PnlController
import com.tripleauth.hermetix.pnl.PnlLogger
import com.tripleauth.hermetix.pnl.PnlService
import com.tripleauth.hermetix.strategy.TradingStrategy
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@EnableConfigurationProperties(NextApiProperties::class, KisApiProperties::class, KiwoomApiProperties::class, NextEngineProperties::class, NextPnlProperties::class)
class NextTradingAutoConfiguration {

    // 앱의 전역 Jackson 설정을 건드리지 않도록 빈으로 노출하지 않는다 (API 통신 전용)
    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    @Bean
    @ConditionalOnMissingBean
    fun tokenManager(properties: NextApiProperties): TokenManager =
        TokenManager(properties, objectMapper)

    @Bean
    @ConditionalOnMissingBean(BrokerClient::class)
    @ConditionalOnProperty(prefix = "hermetix", name = ["broker"], havingValue = "next", matchIfMissing = true)
    fun nextApiClient(properties: NextApiProperties, tokenManager: TokenManager): NextApiClient =
        NextApiClient(properties, tokenManager, objectMapper)

    @Bean
    @ConditionalOnMissingBean(BrokerClient::class)
    @ConditionalOnProperty(prefix = "hermetix", name = ["broker"], havingValue = "kis")
    fun kisApiClient(kisProperties: KisApiProperties): KisApiClient =
        KisApiClient(kisProperties, objectMapper)

    @Bean
    @ConditionalOnMissingBean(BrokerClient::class)
    @ConditionalOnProperty(prefix = "hermetix", name = ["broker"], havingValue = "kiwoom")
    fun kiwoomApiClient(kiwoomProperties: KiwoomApiProperties): KiwoomApiClient =
        KiwoomApiClient(kiwoomProperties, objectMapper)

    @Bean
    @ConditionalOnMissingBean
    fun marketCalendarService(brokerClient: BrokerClient): MarketCalendarService =
        MarketCalendarService(brokerClient)

    @Bean
    @ConditionalOnMissingBean
    fun tradingGuard(brokerClient: BrokerClient, engineProperties: NextEngineProperties): TradingGuard =
        TradingGuard(brokerClient, engineProperties.maxConsecutiveFailures)

    @Bean
    @ConditionalOnMissingBean
    fun bracketMonitor(brokerClient: BrokerClient): BracketMonitor =
        BracketMonitor(brokerClient)

    @Bean
    @ConditionalOnMissingBean
    fun orderExecutor(brokerClient: BrokerClient, bracketMonitor: BracketMonitor, tradingGuard: TradingGuard): OrderExecutor =
        OrderExecutor(brokerClient, bracketMonitor, tradingGuard)

    @Bean
    @ConditionalOnMissingBean
    fun pnlService(brokerClient: BrokerClient, pnlProperties: NextPnlProperties): PnlService =
        PnlService(brokerClient, pnlProperties.initialCapital)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "hermetix.pnl", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun pnlLogger(pnlService: PnlService, pnlProperties: NextPnlProperties): PnlLogger =
        PnlLogger(pnlService, java.time.Duration.ofMinutes(pnlProperties.logIntervalMinutes))

    @Bean
    @ConditionalOnMissingBean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication
    fun pnlController(pnlService: PnlService): PnlController = PnlController(pnlService)

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "hermetix.engine", name = ["enabled"], havingValue = "true", matchIfMissing = true)
    fun strategyEngine(
        strategies: List<TradingStrategy>,
        brokerClient: BrokerClient,
        marketCalendarService: MarketCalendarService,
        orderExecutor: OrderExecutor,
        bracketMonitor: BracketMonitor,
        tradingGuard: TradingGuard,
    ): StrategyEngine =
        StrategyEngine(strategies, brokerClient, marketCalendarService, orderExecutor, bracketMonitor, tradingGuard)
}
