package com.tripleauth.hermetix

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.client.NextApiClient
import com.tripleauth.hermetix.client.NextApiProperties
import com.tripleauth.hermetix.client.TokenManager
import com.tripleauth.hermetix.client.db.DbApiClient
import com.tripleauth.hermetix.client.db.DbApiProperties
import com.tripleauth.hermetix.client.kb.KbApiClient
import com.tripleauth.hermetix.client.kb.KbApiProperties
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import com.tripleauth.hermetix.client.ls.LsApiClient
import com.tripleauth.hermetix.client.ls.LsApiProperties
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.nh.NhApiProperties
import com.tripleauth.hermetix.client.toss.TossApiClient
import com.tripleauth.hermetix.client.toss.TossApiProperties

/**
 * 모든 브로커가 같은 모양으로 받는 자격 증명 (docs/broker-factory.md).
 *
 * - [apiKey]/[apiSecret]: 브로커의 키·시크릿 (next client_id/secret, kis appkey/appsecret, kiwoom appkey/secretkey, 나머지 app_key/app_secret)
 * - [account]: 계좌 식별자 — next account_id, kis cano, nh account_no, toss account_seq. kiwoom·db·ls·kb 는 쓰지 않는다
 * - [environment]: null 이면 브로커 기본값 (next/kis/kiwoom/nh/db/ls = PAPER, toss/kb = LIVE)
 * - [extra]: 브로커별 선택 항목. 키는 기존 생성자 인자의 snake_case — 모르는 키는 [IllegalArgumentException]
 */
data class Credentials(
    val apiKey: String,
    val apiSecret: String,
    val account: String = "",
    val environment: TradingEnvironment? = null,
    val extra: Map<String, String> = emptyMap(),
)

/**
 * 브로커 팩토리 — `Hermetix.next(Credentials(...))` 처럼 브로커 ID 한 토큰만 바꾸면 증권사가 바뀐다.
 * 자격 증명을 각 `XxxApiProperties` 로 매핑만 하고, 인증·환경 결정·쓰로틀 기본값은 기존 생성자 로직 그대로다.
 */
object Hermetix {

    /** 브로커 ID 목록 — `BrokerCapabilities.brokerId` 와 같고, 이 순서가 문서·페이지의 표시 순서다 */
    val brokers: List<String> = listOf("next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb")

    /** 기본 ObjectMapper — 어댑터가 기대하는 설정 (kotlin 모듈, java.time, 모르는 필드 무시) */
    val defaultObjectMapper: ObjectMapper by lazy {
        ObjectMapper()
            .registerModule(kotlinModule())
            .registerModule(JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    fun next(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): NextApiClient =
        nextProperties(credentials).let { NextApiClient(it, TokenManager(it, objectMapper), objectMapper) }

    fun kis(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): KisApiClient =
        KisApiClient(kisProperties(credentials), objectMapper)

    fun kiwoom(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): KiwoomApiClient =
        KiwoomApiClient(kiwoomProperties(credentials), objectMapper)

    fun nh(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): NhApiClient =
        NhApiClient(nhProperties(credentials), objectMapper)

    fun ls(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): LsApiClient =
        LsApiClient(lsProperties(credentials), objectMapper)

    fun db(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): DbApiClient =
        DbApiClient(dbProperties(credentials), objectMapper)

    fun toss(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): TossApiClient =
        TossApiClient(tossProperties(credentials), objectMapper)

    fun kb(credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): KbApiClient =
        KbApiClient(kbProperties(credentials), objectMapper)

    /** 브로커 ID 로 만든다 — 설정 파일의 문자열 하나로 증권사를 고를 때. 모르는 ID 는 [IllegalArgumentException] */
    fun client(id: String, credentials: Credentials, objectMapper: ObjectMapper = defaultObjectMapper): BrokerClient =
        when (id.lowercase()) {
            "next" -> next(credentials, objectMapper)
            "kis" -> kis(credentials, objectMapper)
            "kiwoom" -> kiwoom(credentials, objectMapper)
            "nh" -> nh(credentials, objectMapper)
            "ls" -> ls(credentials, objectMapper)
            "db" -> db(credentials, objectMapper)
            "toss" -> toss(credentials, objectMapper)
            "kb" -> kb(credentials, objectMapper)
            else -> throw IllegalArgumentException("알 수 없는 브로커 ID '$id' — 가능한 값: ${brokers.joinToString()}")
        }

    // ---- 자격 증명 → Properties 매핑 (테스트에서 직접 확인한다)

    internal fun nextProperties(c: Credentials): NextApiProperties {
        val x = Extra("next", c, setOf("base_url"))
        val d = NextApiProperties()
        return NextApiProperties(
            baseUrl = x.string("base_url", d.baseUrl),
            environment = c.environment ?: d.environment,
            clientId = c.apiKey,
            clientSecret = c.apiSecret,
            accountId = c.account.ifBlank { "acc_main" },
        )
    }

    internal fun kisProperties(c: Credentials): KisApiProperties {
        require(c.account.isNotBlank()) { "kis 는 account(cano, 계좌번호 앞 8자리)가 필요합니다" }
        val x = Extra("kis", c, setOf("base_url", "ws_url", "throttle_seconds", "acnt_prdt_cd", "custtype", "hts_id"))
        val d = KisApiProperties()
        return KisApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            appkey = c.apiKey,
            appsecret = c.apiSecret,
            cano = c.account,
            acntPrdtCd = x.string("acnt_prdt_cd", d.acntPrdtCd),
            custtype = x.string("custtype", d.custtype),
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            htsId = x.string("hts_id", d.htsId),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun kiwoomProperties(c: Credentials): KiwoomApiProperties {
        val x = Extra("kiwoom", c, setOf("base_url", "ws_url", "throttle_seconds"))
        val d = KiwoomApiProperties()
        return KiwoomApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            appkey = c.apiKey,
            secretkey = c.apiSecret,
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun nhProperties(c: Credentials): NhApiProperties {
        val x = Extra("nh", c, setOf("base_url", "ws_url", "throttle_seconds", "auth_url", "market_cd", "order_market_cd"))
        val d = NhApiProperties()
        return NhApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            authUrl = x.string("auth_url", d.authUrl),
            appKey = c.apiKey,
            appSecret = c.apiSecret,
            accountNo = c.account,
            marketCd = x.string("market_cd", d.marketCd),
            orderMarketCd = x.string("order_market_cd", d.orderMarketCd),
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun lsProperties(c: Credentials): LsApiProperties {
        val x = Extra("ls", c, setOf("base_url", "ws_url", "throttle_seconds", "mac_address", "exch_gubun", "chart_throttle_seconds"))
        val d = LsApiProperties()
        return LsApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            appKey = c.apiKey,
            appSecret = c.apiSecret,
            macAddress = x.string("mac_address", d.macAddress),
            exchGubun = x.string("exch_gubun", d.exchGubun),
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            chartThrottleMillis = x.millis("chart_throttle_seconds", d.chartThrottleMillis),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun dbProperties(c: Credentials): DbApiProperties {
        val x = Extra("db", c, setOf("base_url", "ws_url", "throttle_seconds", "mac_address", "market_div_code"))
        val d = DbApiProperties()
        return DbApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            appKey = c.apiKey,
            appSecret = c.apiSecret,
            macAddress = x.string("mac_address", d.macAddress),
            marketDivCode = x.string("market_div_code", d.marketDivCode),
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun tossProperties(c: Credentials): TossApiProperties {
        val x = Extra("toss", c, setOf("base_url", "ws_url", "throttle_seconds"))
        val d = TossApiProperties()
        return TossApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            clientId = c.apiKey,
            clientSecret = c.apiSecret,
            accountSeq = c.account,
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
            wsUrl = x.string("ws_url", d.wsUrl),
        )
    }

    internal fun kbProperties(c: Credentials): KbApiProperties {
        val x = Extra("kb", c, setOf("base_url", "throttle_seconds", "excg_clsf", "sor_order_ccd", "chart_market_clsf"))
        val d = KbApiProperties()
        return KbApiProperties(
            environment = c.environment ?: d.environment,
            baseUrl = x.string("base_url", d.baseUrl),
            appKey = c.apiKey,
            appSecret = c.apiSecret,
            excgClsf = x.string("excg_clsf", d.excgClsf),
            sorOrderCcd = x.string("sor_order_ccd", d.sorOrderCcd),
            chartMarketClsf = x.string("chart_market_clsf", d.chartMarketClsf),
            throttleMillis = x.millis("throttle_seconds", d.throttleMillis),
        )
    }

    /** extra 검증 + 꺼내기. 필수 키·모르는 키를 여기서 한 번에 거른다 */
    private class Extra(broker: String, c: Credentials, allowed: Set<String>) {
        private val map = c.extra

        init {
            require(c.apiKey.isNotBlank()) { "$broker: api_key 가 비어 있습니다" }
            require(c.apiSecret.isNotBlank()) { "$broker: api_secret 이 비어 있습니다" }
            val unknown = map.keys - allowed
            require(unknown.isEmpty()) { "$broker: 알 수 없는 extra 키 ${unknown.sorted()} — 가능한 값: ${allowed.sorted()}" }
        }

        fun string(key: String, default: String): String = map[key]?.takeIf { it.isNotBlank() } ?: default

        /** 초 단위 문자열(소수 허용) → 밀리초 */
        fun millis(key: String, default: Long): Long {
            val raw = map[key]?.takeIf { it.isNotBlank() } ?: return default
            val seconds = raw.toDoubleOrNull() ?: throw IllegalArgumentException("$key 는 초 단위 숫자여야 합니다: '$raw'")
            return Math.round(seconds * 1000)
        }
    }
}
