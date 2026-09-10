package com.tripleauth.hermetix.client.db

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * DB증권 REST OpenAPI 설정. `hermetix.broker: db` 로 활성화한다.
 *
 * 운영/모의는 **같은 호스트·경로**를 쓰고 모의투자용 APP_KEY/SECRET 으로만 분기된다 — 서버가 키 속성으로 내부 분기한다.
 * 따라서 [environment] 는 엔진의 실전 게이트용 선언일 뿐이며, 어댑터가 키와 환경의 일치를 검증할 방법은 없다
 * (응답 `rsp_msg` 에 "모의투자" 가 붙는 것으로 사후 확인만 가능).
 */
@ConfigurationProperties(prefix = "hermetix.db")
data class DbApiProperties(
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    val baseUrl: String = "https://openapi.dbsec.co.kr:8443",
    val appKey: String = "",
    val appSecret: String = "",
    /** 법인 계정은 필수(없으면 IGW00132). 개인은 비워 둔다 */
    val macAddress: String = "",
    /** 시세 시장구분: `J` 주식/ETF, `NJ` NXT, `UJ` 통합 */
    val marketDivCode: String = "J",
    /**
     * 호출 간 최소 간격(ms). 서버는 앱 20 TPS 이지만 엔드포인트별로 잔고·체결 2 TPS, 예수금 1 TPS 라
     * 가장 빡빡한 조회 기준 500ms 를 기본으로 둔다
     */
    val throttleMillis: Long = 500,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초). 발급은 1분 1건 제한이라 넉넉히 잡는다 */
    val tokenRefreshMarginSeconds: Long = 600,
)
