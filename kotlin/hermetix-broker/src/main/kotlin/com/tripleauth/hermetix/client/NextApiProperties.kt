package com.tripleauth.hermetix.client

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.next")
data class NextApiProperties(
    val baseUrl: String = "https://openapi.nextsecurities.dev",
    /**
     * 거래 환경. 넥스트증권은 키 프리픽스로 환경이 정해진다 (`pk_test_`=모의, `pk_live_`=실전) —
     * `pk_` 로 시작하는 키가 설정과 어긋나면 기동 시 실패한다 (실전 키를 모의로 착각하는 사고 방지).
     */
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    val clientId: String = "",
    val clientSecret: String = "",
    /** X-Next-Account-Id 헤더 값 (v1.3). 계좌·자산·주문 API 필수 — 비우면 401, 토큰의 계좌와 다르면 403 account-mismatch. */
    val accountId: String = "",
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 60,
)
