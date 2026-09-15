"""브로커 웹소켓 어댑터의 공용 부품 (Kotlin ReconnectingWebSocket 과 동일 의미).

`websockets` 패키지(선택 의존성, `pip install hermetix[stream]`) 위에 재연결·유휴 감시·직렬 전송을 얹는다.
코어의 의존성 0 을 지키기 위해 websockets 는 connect() 안에서만 import 한다.

하위 클래스가 정하는 것: 접속 URI(uri()), 연결 직후 보낼 것(on_open() - 로그인·구독), 텍스트 프레임 처리(on_message()).

동작:
- connect() 는 즉시 반환하고 전용 데몬 스레드의 asyncio 루프에서 접속한다. 실패하면 1s -> 2s -> 4s ... max_backoff 로 재시도
- 소켓이 닫히거나 오류가 나면 같은 백오프로 재접속한다. close() 뒤에는 재접속하지 않는다
- idle_timeout 동안 프레임이 하나도 없으면 죽은 연결로 보고 끊고 재접속한다
  (KIS·키움처럼 서버가 주기적으로 PING 류 프레임을 보내는 브로커용. 0 이면 끈다 - NH 처럼 조용한 게 정상인 브로커)
- heartbeat_seconds > 0 이면 그 주기로 on_heartbeat() 를 부른다 (토스처럼 클라이언트가 먼저 PING 을 보내야 하는 브로커)
- headers() 가 돌려주는 헤더를 핸드셰이크에 싣는다 (토스: Authorization Bearer). 매 (재)접속마다 호출된다
- send() 는 스트림 스레드로 넘겨 직렬 실행된다
"""
from __future__ import annotations

import asyncio
import logging
import threading
from abc import ABC, abstractmethod
from typing import Callable

logger = logging.getLogger("hermetix")


class ReconnectingWebSocket(ABC):

    def __init__(self, name: str, max_backoff_seconds: float = 30.0, idle_timeout_seconds: float = 90.0,
                 connect_timeout_seconds: float = 10.0, heartbeat_seconds: float = 0.0, usage=None):
        self._name = name
        # 사용량 텔레메트리 핸들(BrokerUsage) - 재접속 횟수를 센다 (docs/telemetry.md). None 이면 세지 않는다
        self._usage = usage
        self._max_backoff = max_backoff_seconds
        self._idle_timeout = idle_timeout_seconds
        self._heartbeat = heartbeat_seconds
        self._connect_timeout = connect_timeout_seconds
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._socket = None
        self._closed = False
        self._attempt = 0
        self._socket_open = False
        self._send_lock: asyncio.Lock | None = None
        # 원시 텍스트 프레임 관찰용 훅 (프로토콜 실측·픽스처 채집). 파싱 전에 호출되며 예외는 무시된다
        self.raw_frame_hook: Callable[[str], None] | None = None

    # ------------------------------------------------------------------ 하위 클래스 계약

    @abstractmethod
    def uri(self) -> str:
        """매 (재)접속마다 호출된다 - 토큰·승인키 갱신은 여기서"""

    def headers(self) -> dict[str, str]:
        """접속 핸드셰이크에 실을 HTTP 헤더 - 매 (재)접속마다 호출된다 (토스: Authorization Bearer)"""
        return {}

    @abstractmethod
    def on_open(self) -> None:
        """소켓이 열린 직후 (로그인/구독 전송). 스트림 스레드에서 호출된다"""

    @abstractmethod
    def on_message(self, text: str) -> None:
        """완성된 텍스트 프레임 1건. 스트림 스레드에서 호출된다 - 예외는 로그만 남긴다"""

    def on_disconnected(self) -> None:
        """연결이 끊긴 직후 (재접속 예약 전). 하위 클래스가 로그인 상태 등을 초기화한다"""

    def on_heartbeat(self) -> None:
        """heartbeat_seconds 주기로, 소켓이 열려 있을 때만 호출된다 (스트림 스레드)"""

    # ------------------------------------------------------------------ 공개 API

    @property
    def is_socket_open(self) -> bool:
        """소켓이 열려 있는지 - 로그인 필요 브로커는 하위 클래스가 별도 상태를 둔다"""
        return self._socket_open

    def connect(self) -> None:
        if self._closed:
            raise RuntimeError(f"{self._name} stream: 닫힌 스트림은 다시 열 수 없다")
        try:
            import websockets  # noqa: F401 - 선택 의존성 확인
        except ImportError as e:
            raise ImportError(
                "실시간 스트림에는 'websockets' 패키지가 필요합니다: pip install 'hermetix[stream]'") from e
        if self._thread is not None:
            return
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name=f"{self._name}-ws", daemon=True)
        self._thread.start()

    def send(self, text: str) -> bool:
        """텍스트 프레임 전송 (스트림 스레드에서 직렬 실행). 연결이 없으면 False.
        스트림 스레드 안에서 부르면 전송을 예약만 하고 True 를 돌려준다"""
        loop = self._loop
        if loop is None or self._socket is None or self._closed:
            return False
        if threading.current_thread() is self._thread:
            loop.create_task(self._send(text))
            return True
        future = asyncio.run_coroutine_threadsafe(self._send(text), loop)
        try:
            return future.result(timeout=10)
        except Exception as e:  # noqa: BLE001
            logger.warning("%s stream: 전송 실패 - %s", self._name, e)
            return False

    def close(self) -> None:
        self._closed = True
        self._socket_open = False
        loop = self._loop
        if loop is None:
            return
        try:
            future = asyncio.run_coroutine_threadsafe(self._close_socket(), loop)
            future.result(timeout=3)
        except Exception:  # noqa: BLE001
            pass
        loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None and threading.current_thread() is not self._thread:
            self._thread.join(timeout=3)

    # ------------------------------------------------------------------ 내부

    def _run_loop(self) -> None:
        loop = self._loop
        asyncio.set_event_loop(loop)
        self._send_lock = asyncio.Lock()
        loop.create_task(self._session_loop())
        try:
            loop.run_forever()
        finally:
            loop.close()

    async def _session_loop(self) -> None:
        """접속 -> 수신 루프 -> (끊기면) 백오프 대기 -> 재접속. close() 될 때까지 반복"""
        import websockets
        while not self._closed:
            target = None
            try:
                target = self.uri()
                logger.info("%s stream: connecting %s (attempt %d)", self._name, target, self._attempt + 1)
                async with websockets.connect(target, additional_headers=self.headers() or None,
                                              open_timeout=self._connect_timeout, ping_interval=None,
                                              max_size=None) as ws:
                    self._socket = ws
                    self._socket_open = True
                    self._attempt = 0
                    logger.info("%s stream: connected", self._name)
                    try:
                        self.on_open()
                    except Exception:  # noqa: BLE001
                        logger.exception("%s stream: on_open 실패", self._name)
                    heartbeat = asyncio.ensure_future(self._heartbeat_loop()) if self._heartbeat > 0 else None
                    try:
                        reason = await self._receive_until_closed(ws)
                    finally:
                        if heartbeat is not None:
                            heartbeat.cancel()
                    self._handle_disconnect(reason)
            except Exception as e:  # noqa: BLE001
                if self._socket_open:
                    self._handle_disconnect(f"error {e}")
                else:
                    logger.warning("%s stream: 접속 실패 - %s", self._name, e)
            if self._closed:
                break
            delay = min(1.0 * (2 ** min(self._attempt, 10)), self._max_backoff)
            if self._attempt > 0 and self._usage is not None:
                self._usage.reconnected()  # 첫 접속 실패 재시도부터 셈 (정상 첫 접속은 재접속이 아님)
            self._attempt += 1
            logger.info("%s stream: reconnect in %.0fms", self._name, delay * 1000)
            await asyncio.sleep(delay)

    async def _heartbeat_loop(self) -> None:
        """heartbeat_seconds 마다 on_heartbeat() - 소켓이 열려 있는 동안만"""
        while not self._closed and self._socket is not None:
            await asyncio.sleep(self._heartbeat)
            if self._closed or self._socket is None:
                return
            try:
                self.on_heartbeat()
            except Exception as e:  # noqa: BLE001
                logger.warning("%s stream: heartbeat 실패 - %s", self._name, e)

    async def _receive_until_closed(self, ws) -> str:
        """프레임을 받아 on_message 로 넘긴다. 유휴 시간 초과·종료·오류 시 사유를 돌려준다 (idle_timeout 0 이면 무기한 대기)"""
        while not self._closed:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=self._idle_timeout if self._idle_timeout > 0 else None)
            except asyncio.TimeoutError:
                logger.warning("%s stream: %.0fms 동안 프레임 없음 - 재접속", self._name, self._idle_timeout * 1000)
                try:
                    await ws.close()
                except Exception:  # noqa: BLE001
                    pass
                return "idle timeout"
            except Exception as e:  # noqa: BLE001 - ConnectionClosed 포함
                code = getattr(getattr(e, "rcvd", None), "code", None) or getattr(getattr(e, "sent", None), "code", None)
                return f"close {code}" if code is not None else f"error {e}"
            if isinstance(message, (bytes, bytearray)):
                continue
            self._dispatch(message)
        return "closed"

    def _handle_disconnect(self, reason: str) -> None:
        was_open = self._socket_open
        self._socket = None
        self._socket_open = False
        if self._closed:
            return
        if was_open:
            logger.warning("%s stream: disconnected - %s", self._name, reason)
        try:
            self.on_disconnected()
        except Exception:  # noqa: BLE001
            logger.exception("%s stream: on_disconnected 실패", self._name)

    def _dispatch(self, text: str) -> None:
        hook = self.raw_frame_hook
        if hook is not None:
            try:
                hook(text)
            except Exception:  # noqa: BLE001
                pass
        try:
            self.on_message(text)
        except Exception:  # noqa: BLE001
            logger.exception("%s stream: 메시지 처리 실패 - %s", self._name, text[:200])

    async def _send(self, text: str) -> bool:
        ws = self._socket
        if ws is None:
            return False
        try:
            async with self._send_lock:
                await ws.send(text)
            return True
        except Exception as e:  # noqa: BLE001
            logger.warning("%s stream: 전송 실패 - %s", self._name, e)
            return False

    async def _close_socket(self) -> None:
        ws = self._socket
        self._socket = None
        if ws is not None:
            try:
                await asyncio.wait_for(ws.close(), timeout=2)
            except Exception:  # noqa: BLE001
                pass
