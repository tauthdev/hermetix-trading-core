package com.tripleauth.hermetix.client.kis

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "hermetix.kis")
data class KisApiProperties(
    /** 모의투자 도메인. 실전은 https://openapi.koreainvestment.com:9443 */
    val baseUrl: String = "https://openapivts.koreainvestment.com:29443",
    val appkey: String = "",
    val appsecret: String = "",
    /** 종합계좌번호 (8자리) */
    val cano: String = "",
    /** 계좌상품코드 (2자리, 보통 01) */
    val acntPrdtCd: String = "01",
    /** 고객타입 (P: 개인) */
    val custtype: String = "P",
    /** 모의투자 서버 초당 요청 제한 회피용 최소 호출 간격(ms) */
    val throttleMillis: Long = 600,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초) */
    val tokenRefreshMarginSeconds: Long = 300,
)
