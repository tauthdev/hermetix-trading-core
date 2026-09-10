package com.tripleauth.hermetix.client.kb

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * KB증권 Open API(개인 오픈베타) 설정. `hermetix.broker: kb` 로 활성화한다.
 *
 * **실전 전용** — 모의투자는 "추후 제공 예정" 이라 운영 단일 환경이다. 기본 환경이 LIVE 이고 엔진은 `hermetix.live.enabled=true` 없이는
 * 기동하지 않는다. 오픈베타(2026-07~) 라 정식 오픈 전까지 스펙이 바뀔 수 있다.
 */
@ConfigurationProperties(prefix = "hermetix.kb")
data class KbApiProperties(
    val environment: TradingEnvironment = TradingEnvironment.LIVE,
    val baseUrl: String = "https://developer.kbsec.com:32484",
    val appKey: String = "",
    val appSecret: String = "",
    /** 시세 거래소구분(`excg_clsf`): 0 통합 / 1 KRX / 2 NXT */
    val excgClsf: String = "1",
    /** 주문 SOR 구분(`sor_ordr_ccd`): K KRX / N NXT / S SOR */
    val sorOrderCcd: String = "K",
    /**
     * 통합차트(IVS11560) 시장구분(`mkt_clsf`): 0 KOSPI / 1 KOSDAQ. 차트 TR 이 종목의 시장을 요구하는데 어댑터는 종목만 알므로
     * 기본 KOSPI 로 조회한다 — KOSDAQ 종목 전략은 `1` 로 바꿔야 한다 (실측 후 종목기본정보 TR 로 자동화 예정)
     */
    val chartMarketClsf: String = "0",
    /** 호출 간 최소 간격(ms). 게이트웨이 한도 5초당 200건(추정) → 100ms */
    val throttleMillis: Long = 100,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
)
