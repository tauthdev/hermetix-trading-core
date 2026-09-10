package com.tripleauth.hermetix.client.ls

import com.tripleauth.hermetix.broker.TradingEnvironment
import org.springframework.boot.context.properties.ConfigurationProperties

/**
 * LS증권(구 이베스트) OPEN API 설정. `hermetix.broker: ls` 로 활성화한다.
 *
 * 실전/모의는 **같은 호스트**를 쓰고 발급받은 appkey 로 서버가 라우팅한다. 따라서 [environment] 는 엔진의 실전 게이트용 선언이며
 * 어댑터가 키와 환경의 일치를 검증할 방법은 없다. 모의투자 주문은 종목코드에 `A` 접두가 필수라 어댑터는 항상 `A`+코드로 보낸다.
 */
@ConfigurationProperties(prefix = "hermetix.ls")
data class LsApiProperties(
    val environment: TradingEnvironment = TradingEnvironment.PAPER,
    val baseUrl: String = "https://openapi.ls-sec.co.kr:8080",
    val appKey: String = "",
    val appSecret: String = "",
    /** 법인 계정 필수. 개인은 비워 둔다 */
    val macAddress: String = "",
    /** 시세 거래소구분(t1102 `exchgubun`): 빈값(기본) | K(KRX) | N(NXT) | U(통합) — 실측 전이라 기본은 빈값 */
    val exchGubun: String = "",
    /** 호출 간 최소 간격(ms). TR 별 TPS 가 1~10 으로 제각각 — 계좌 TR(2 TPS) 기준 500ms */
    val throttleMillis: Long = 500,
    /** 차트 TR(t8410) 전용 최소 간격(ms) — 포털 명시 초당 1건 */
    val chartThrottleMillis: Long = 1100,
    /** 토큰 만료 전 미리 갱신할 여유 시간(초). 토큰은 발급일 익일 07시까지 유효 */
    val tokenRefreshMarginSeconds: Long = 600,
)
