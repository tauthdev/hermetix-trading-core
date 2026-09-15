package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.UsageTelemetry
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
import com.tripleauth.hermetix.conformance.BrokerConformance
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.math.BigDecimal
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * 모든 어댑터가 사용량 텔레메트리(docs/telemetry.md)를 남기는지 — REST 는 컨포먼스 시나리오를 돌린 뒤 버킷을,
 * 스트림은 구독 1건 + 프레임 1건 전달 뒤 버킷을 확인한다. 전송은 가로채서 아무 데도 보내지 않는다.
 */
class UsageInstrumentationTest {

    private lateinit var server: MockWebServer
    private lateinit var originalTransport: (String) -> Unit

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
        originalTransport = UsageTelemetry.transport
        UsageTelemetry.transport = { }
        UsageTelemetry.drain()
    }

    @AfterEach
    fun tearDown() {
        UsageTelemetry.drain()
        UsageTelemetry.transport = originalTransport
        server.shutdown()
    }

    private fun baseUrl() = server.url("/").toString().removeSuffix("/")

    private fun fixture(broker: String): JsonNode {
        val file = File("../../conformance/fixtures/$broker.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file)
    }

    /** BrokerConformanceTest 의 라우터와 같은 규칙 — method / path / header 중 지정된 것만 검사 */
    private class FixtureDispatcher(private val routes: JsonNode, private val objectMapper: ObjectMapper) : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val path = request.path?.substringBefore('?') ?: ""
            val route = routes.firstOrNull { r ->
                (!r.has("method") || r["method"].asText() == request.method) &&
                    (!r.has("path") || r["path"].asText() == path) &&
                    (!r.has("header") || request.getHeader(r["header"][0].asText()) == r["header"][1].asText())
            } ?: return MockResponse().setResponseCode(599).setBody("""{"error":"no fixture route for ${request.method} $path"}""")
            return MockResponse()
                .setResponseCode(route.path("status").asInt(200))
                .setHeader("Content-Type", "application/json")
                .setBody(objectMapper.writeValueAsString(route["body"]))
        }
    }

    private fun client(broker: String): BrokerClient = when (broker) {
        "next" -> NextApiProperties(baseUrl = baseUrl(), clientId = "pk_test_conf", clientSecret = "sk_test_conf", accountId = "acc_main")
            .let { NextApiClient(it, TokenManager(it, objectMapper), objectMapper) }
        "kis" -> KisApiClient(KisApiProperties(baseUrl = baseUrl(), appkey = "k", appsecret = "s", cano = "50199202", throttleMillis = 1), objectMapper)
        "kiwoom" -> KiwoomApiClient(KiwoomApiProperties(baseUrl = baseUrl(), appkey = "k", secretkey = "s", throttleMillis = 1), objectMapper)
        "nh" -> NhApiClient(NhApiProperties(baseUrl = baseUrl(), authUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
        "db" -> DbApiClient(DbApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
        "ls" -> LsApiClient(LsApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1, chartThrottleMillis = 1), objectMapper)
        "toss" -> TossApiClient(TossApiProperties(baseUrl = baseUrl(), clientId = "c_conf", clientSecret = "s_conf", throttleMillis = 1), objectMapper)
        "kb" -> KbApiClient(KbApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
        else -> error(broker)
    }

    private val brokers = listOf("next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb")

    @Test
    fun `여덟 어댑터 모두 REST 호출이 op 별로 세어지고 토큰 발급은 auth 로 세어진다`() {
        brokers.forEach { broker ->
            val fx = fixture(broker)
            server.dispatcher = FixtureDispatcher(fx["routes"], objectMapper)
            val scenario = BrokerConformance.Scenario(
                symbol = fx["scenario"]["symbol"].asText(),
                quantity = BigDecimal(fx["scenario"]["quantity"].asText()),
                limitPrice = BigDecimal(fx["scenario"]["limitPrice"].asText()),
            )
            val report = BrokerConformance.verify(client(broker), scenario)
            assertThat(report.violations).describedAs("$broker: $report").isEmpty()
        }

        val payload = objectMapper.readTree(UsageTelemetry.drain())
        val buckets = payload["buckets"].associateBy { it["broker"].asText() }
        assertThat(buckets.keys).containsExactlyInAnyOrderElementsOf(brokers)

        val required = listOf("quotes", "candles", "calendar", "account", "holdings", "buying_power", "create_order", "get_order", "get_orders", "cancel_order", "fills", "auth")
        brokers.forEach { broker ->
            val bucket = buckets[broker]!!
            val ops = bucket["ops"].associateBy { it["op"].asText() }
            required.forEach { op ->
                assertThat(ops[op]).describedAs("$broker: op '$op' 누락").isNotNull()
                assertThat(ops[op]!!["ok"].asLong()).describedAs("$broker.$op ok").isGreaterThanOrEqualTo(1)
                assertThat(ops[op]!!["latencyMs"]["count"].asLong()).describedAs("$broker.$op latency count").isGreaterThanOrEqualTo(1)
            }
            assertThat(bucket["environment"].asText()).isEqualTo(if (broker == "toss" || broker == "kb") "LIVE" else "PAPER")
        }
    }

    // ------------------------------------------------------------------ streams

    private class ServerConnection(val socket: WebSocket) {
        val received = LinkedBlockingQueue<String>()
    }

    private fun webSocketServer(connections: LinkedBlockingQueue<ServerConnection>, all: MutableList<ServerConnection>) {
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                // 토스는 핸드셰이크 헤더(Bearer)에 REST 토큰이 필요하다 — 토큰 발급 경로만 JSON 으로 응답
                if (request.method == "POST" && (request.path ?: "").contains("token")) {
                    return MockResponse().setHeader("Content-Type", "application/json")
                        .setBody("""{"access_token":"tok","token_type":"Bearer","expires_in":86400,"token":"tok","expires_dt":"20991231235959"}""")
                }
                return MockResponse().withWebSocketUpgrade(object : WebSocketListener() {
                    lateinit var conn: ServerConnection
                    override fun onOpen(webSocket: WebSocket, response: Response) {
                        conn = ServerConnection(webSocket).also { all += it; connections.put(it) }
                    }
                    override fun onMessage(webSocket: WebSocket, text: String) { conn.received.put(text) }
                    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) { webSocket.close(code, reason) }
                })
            }
        }
    }

    /** 브로커별 (스트림 팩토리, 구독 심볼) — 픽스처의 체결 프레임 첫 건이 그 심볼로 전달되어야 한다 */
    private fun streamCases(): List<Triple<String, () -> MarketStream, String>> {
        val ws = "ws://${server.hostName}:${server.port}"
        return listOf(
            Triple("kis", { (KisApiClient(KisApiProperties(baseUrl = baseUrl(), appkey = "k", appsecret = "s", cano = "1", throttleMillis = 1, wsUrl = "$ws/"), objectMapper) as StreamingBrokerClient).openStream() }, "005930"),
            Triple("kiwoom", { (KiwoomApiClient(KiwoomApiProperties(baseUrl = baseUrl(), appkey = "k", secretkey = "s", throttleMillis = 1, wsUrl = "$ws/api/dostk/websocket"), objectMapper) as StreamingBrokerClient).openStream() }, "000660"),
            Triple("nh", { (NhApiClient(NhApiProperties(baseUrl = baseUrl(), authUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1, wsUrl = "$ws/websocket"), objectMapper) as StreamingBrokerClient).openStream() }, "005940"),
            Triple("db", { (DbApiClient(DbApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1, wsUrl = "$ws/websocket"), objectMapper) as StreamingBrokerClient).openStream() }, "005930"),
            Triple("ls", { (LsApiClient(LsApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1, chartThrottleMillis = 1, wsUrl = "$ws/websocket"), objectMapper) as StreamingBrokerClient).openStream() }, "005930"),
            Triple("toss", { (TossApiClient(TossApiProperties(baseUrl = baseUrl(), clientId = "c", clientSecret = "s", accountSeq = "3", throttleMillis = 1, wsUrl = "$ws/ws/v1"), objectMapper) as StreamingBrokerClient).openStream() }, "US:AAPL"),
        )
    }

    @Test
    fun `여섯 스트림 모두 구독과 전달 메시지가 채널별로 세어진다`() {
        streamCases().forEach { (broker, factory, symbol) ->
            UsageTelemetry.drain()
            val connections = LinkedBlockingQueue<ServerConnection>()
            val all = mutableListOf<ServerConnection>()
            webSocketServer(connections, all)

            val frame = fixture(broker)["stream"]["frames"][0].asText()
            val delivered = CountDownLatch(1)
            val deliveredCount = AtomicInteger() // 픽스처 프레임에 레코드가 여러 건(KIS 002)이면 그만큼 전달된다
            val stream = factory()
            try {
                stream.subscribeTrades(listOf(symbol)) { deliveredCount.incrementAndGet(); delivered.countDown() }
                stream.connect()
                val conn = connections.poll(5, TimeUnit.SECONDS) ?: error("$broker: 5s 안에 접속하지 않음")
                // 토큰 발급이 필요한 스트림(kiwoom LOGIN 등)은 REST 라우트가 없어도 프레임 전달은 파서만 거치므로 그대로 진행한다
                Thread.sleep(300)
                conn.socket.send(frame)
                assertThat(delivered.await(5, TimeUnit.SECONDS)).describedAs("$broker: 체결 프레임이 리스너에 전달되지 않음").isTrue()
                Thread.sleep(100)
            } finally {
                stream.close()
                all.forEach { runCatching { it.socket.close(1000, null) } }
            }

            val bucket = objectMapper.readTree(UsageTelemetry.drain())["buckets"].first { it["broker"].asText() == broker }
            val trades = bucket["streams"].first { it["channel"].asText() == "TRADES" }
            assertThat(trades["subscriptions"].asLong()).describedAs("$broker TRADES subscriptions").isGreaterThanOrEqualTo(1)
            assertThat(trades["messages"].asLong()).describedAs("$broker TRADES messages").isEqualTo(deliveredCount.get().toLong()).isGreaterThanOrEqualTo(1)
        }
    }
}
