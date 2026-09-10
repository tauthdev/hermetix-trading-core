package com.tripleauth.hermetix.autoconfigure

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.NextApiClient
import com.tripleauth.hermetix.client.db.DbApiClient
import com.tripleauth.hermetix.client.db.DbApiProperties
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.nh.NhApiProperties
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import com.tripleauth.hermetix.client.NextApiProperties
import com.tripleauth.hermetix.client.TokenManager
import com.tripleauth.hermetix.engine.BracketMonitor
import com.tripleauth.hermetix.engine.OrderExecutor
import com.tripleauth.hermetix.engine.RiskGuard
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
@EnableConfigurationProperties(
    NextApiProperties::class, KisApiProperties::class, KiwoomApiProperties::class, NhApiProperties::class, DbApiProperties::class,
    NextEngineProperties::class, NextPnlProperties::class, HermetixLiveProperties::class, HermetixRiskProperties::class,
)
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

    /** NH투자증권 NH PLUG — 문서 기반 구현(실측 전), 상태 미검증 */
    @Bean
    @ConditionalOnMissingBean(BrokerClient::class)
    @ConditionalOnProperty(prefix = "hermetix", name = ["broker"], havingValue = "nh")
    fun nhApiClient(nhProperties: NhApiProperties): NhApiClient =
        NhApiClient(nhProperties, objectMapper)

    /** DB증권 — 문서 기반 구현(실측 전), 상태 미검증 */
    @Bean
    @ConditionalOnMissingBean(BrokerClient::class)
    @ConditionalOnProperty(prefix = "hermetix", name = ["broker"], havingValue = "db")
    fun dbApiClient(dbProperties: DbApiProperties): DbApiClient =
        DbApiClient(dbProperties, objectMapper)

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
    fun riskGuard(riskProperties: HermetixRiskProperties): RiskGuard =
        RiskGuard(riskProperties.maxOrderValue, riskProperties.maxDailyOrderValue)

    @Bean
    @ConditionalOnMissingBean
    fun orderExecutor(brokerClient: BrokerClient, bracketMonitor: BracketMonitor, tradingGuard: TradingGuard, riskGuard: RiskGuard): OrderExecutor =
        OrderExecutor(brokerClient, bracketMonitor, tradingGuard, riskGuard)

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
        liveProperties: HermetixLiveProperties,
    ): StrategyEngine =
        StrategyEngine(strategies, brokerClient, marketCalendarService, orderExecutor, bracketMonitor, tradingGuard, liveProperties.enabled)
}
