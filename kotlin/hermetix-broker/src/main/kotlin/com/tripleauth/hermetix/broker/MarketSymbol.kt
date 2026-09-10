package com.tripleauth.hermetix.broker

/**
 * 시장 접두 심볼 표기 — `MARKET:CODE` (예: `KRX:005930`, `US:AAPL`).
 *
 * 한 계좌로 여러 시장을 다루는 브로커(예: 국내+미국)를 위해 심볼에 시장을 붙일 수 있다.
 * 접두가 없는 심볼(`005930`, `AAPL`)은 브로커의 기본 시장으로 해석되므로 기존 전략은 그대로 동작한다.
 *
 * 규칙:
 * - 시장 접두는 대문자 영문 2~6자 + `:`. 그 외 콜론은 심볼의 일부로 본다
 * - 어댑터는 API 호출 시 [code] 만 보내고, 응답 심볼은 요청받은 표기 그대로 돌려준다
 * - 보유/주문처럼 서버가 주는 심볼은 어댑터가 자기 표기(단일 시장이면 접두 없음)로 돌려주며,
 *   전략 컨텍스트 조회([matches])는 접두 유무를 무시하고 코드로 비교한다
 */
data class MarketSymbol(val market: String?, val code: String) {

    override fun toString(): String = if (market == null) code else "$market:$code"

    companion object {
        private val MARKET_PREFIX = Regex("^([A-Z]{2,6}):(.+)$")

        fun parse(symbol: String): MarketSymbol {
            val match = MARKET_PREFIX.find(symbol) ?: return MarketSymbol(null, symbol)
            return MarketSymbol(match.groupValues[1], match.groupValues[2])
        }

        /** 접두를 뗀 브로커 심볼 코드 */
        fun code(symbol: String): String = parse(symbol).code

        /** 접두가 없으면 기본 시장을 붙인 정규 표기 */
        fun canonical(symbol: String, defaultMarket: String): String =
            parse(symbol).let { "${it.market ?: defaultMarket}:${it.code}" }

        /** 코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다 */
        fun matches(a: String, b: String): Boolean {
            val x = parse(a)
            val y = parse(b)
            return x.code == y.code && (x.market == null || y.market == null || x.market == y.market)
        }
    }
}

/**
 * 심볼의 시장 접두가 이 브로커가 지원하는 시장인지 확인하고 브로커 코드를 돌려준다.
 * 미지원 시장이면 [IllegalArgumentException] — 엔진은 틱 실패로 기록한다.
 */
fun BrokerCapabilities.symbolCode(symbol: String): String {
    val parsed = MarketSymbol.parse(symbol)
    val market = parsed.market
    require(market == null || market in markets) {
        "브로커 '$brokerId' 는 시장 '$market' 을 지원하지 않습니다 (지원: $markets): $symbol"
    }
    return parsed.code
}
