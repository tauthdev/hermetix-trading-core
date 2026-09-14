/**
 * 브로커 웹소켓 어댑터의 공용 부품 — Node 22+ 내장 전역 WebSocket 위에 재연결·유휴 감시를 얹는다. 외부 의존성 없음.
 *
 * 하위 클래스가 정하는 것: 접속 URI(uri), 연결 직후 보낼 것(onOpen — 로그인·구독), 텍스트 프레임 처리(onMessage).
 *
 * 동작 (Kotlin ReconnectingWebSocket 과 동일):
 * - connect() 는 즉시 반환하고 접속한다. 실패하면 1s → 2s → 4s … maxBackoffMs 로 재시도
 * - 소켓이 닫히거나 오류가 나면 같은 백오프로 재접속. close() 뒤에는 재접속하지 않는다
 * - idleTimeoutMs 동안 프레임이 하나도 없으면 죽은 연결로 보고 끊고 재접속한다 (0 이면 끈다 — NH 처럼 조용한 게 정상인 브로커)
 * - heartbeatMs > 0 이면 그 주기로 onHeartbeat() 를 부른다 (토스처럼 클라이언트가 먼저 PING 을 보내야 하는 브로커)
 * - headers() 가 돌려주는 헤더를 업그레이드 요청에 싣는다 (Node 22 내장 WebSocket 의 undici `headers` 옵션 — 토스 Authorization: Bearer)
 * - 콜백은 이벤트 루프에서 실행된다. onMessage 의 예외는 로그만 남긴다
 */
const log = {
  info: (msg: string) => console.log(`INFO hermetix ${msg}`),
  warn: (msg: string) => console.warn(`WARN hermetix ${msg}`),
  error: (msg: string) => console.error(`ERROR hermetix ${msg}`),
};

export abstract class ReconnectingWebSocket {
  private socket: WebSocket | null = null;
  private closed = false;
  private attempt = 0;
  private lastFrameAt = Date.now();
  private reconnectTimer: NodeJS.Timeout | null = null;
  private idleTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  /** 원시 텍스트 프레임 관찰용 훅 (프로토콜 실측·픽스처 채집). 파싱 전에 호출되며 예외는 무시된다 */
  rawFrameHook: ((text: string) => void) | null = null;

  /** 소켓이 열려 있는지 — 로그인 필요 브로커는 하위 클래스가 별도 상태를 둔다 */
  isSocketOpen = false;

  constructor(
    private readonly name: string,
    private readonly maxBackoffMs = 30_000,
    private readonly idleTimeoutMs = 90_000,
    private readonly heartbeatMs = 0,
  ) {}

  /** 매 (재)접속마다 호출된다 — 토큰·승인키 갱신은 여기서 */
  protected abstract uri(): string;
  /** 소켓이 열린 직후 (로그인/구독 전송) */
  protected abstract onOpen(): void | Promise<void>;
  /** 완성된 텍스트 프레임 1건 — 예외는 로그만 남긴다 */
  protected abstract onMessage(text: string): void;
  /** 연결이 끊긴 직후 (재접속 예약 전). 하위 클래스가 로그인 상태 등을 초기화한다 */
  protected onDisconnected(): void {}
  /** heartbeatMs 주기로, 소켓이 열려 있을 때만 호출된다 */
  protected onHeartbeat(): void {}
  /** 접속 핸드셰이크에 실을 HTTP 헤더 — 매 (재)접속마다 호출된다 (토스: Authorization Bearer) */
  protected headers(): Record<string, string> | Promise<Record<string, string>> { return {}; }

  connect(): void {
    if (this.closed) throw new Error(`${this.name} stream: 닫힌 스트림은 다시 열 수 없다`);
    void this.doConnect();
    if (this.idleTimeoutMs > 0) {
      this.idleTimer = setInterval(() => this.checkIdle(), Math.max(1, Math.floor(this.idleTimeoutMs / 3)));
      this.idleTimer.unref?.();
    }
    if (this.heartbeatMs > 0) {
      this.heartbeatTimer = setInterval(() => {
        if (this.closed || !this.socket) return;
        try { this.onHeartbeat(); } catch (e) { log.warn(`${this.name} stream: heartbeat 실패 - ${e}`); }
      }, this.heartbeatMs);
      this.heartbeatTimer.unref?.();
    }
  }

  /** 텍스트 프레임 전송. 연결이 없으면 false */
  send(text: string): boolean {
    const ws = this.socket;
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(text);
      return true;
    } catch (e) {
      log.warn(`${this.name} stream: 전송 실패 - ${e}`);
      return false;
    }
  }

  close(): void {
    this.closed = true;
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.idleTimer) { clearInterval(this.idleTimer); this.idleTimer = null; }
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
    const ws = this.socket;
    this.socket = null;
    this.isSocketOpen = false;
    try { ws?.close(1000, "bye"); } catch { /* 이미 닫힘 */ }
  }

  // ------------------------------------------------------------------ internals

  private async doConnect(): Promise<void> {
    if (this.closed) return;
    const target = this.uri();
    log.info(`${this.name} stream: connecting ${target} (attempt ${this.attempt + 1})`);
    let ws: WebSocket;
    try {
      const headers = await this.headers();
      if (this.closed) return;
      // undici(Node 22 내장) WebSocket 은 init.headers 로 업그레이드 헤더를 실을 수 있다 — DOM 타입에는 없어 캐스팅
      ws = Object.keys(headers).length > 0
        ? new (WebSocket as unknown as new (url: string, init: { headers: Record<string, string> }) => WebSocket)(target, { headers })
        : new WebSocket(target);
    } catch (e) {
      log.warn(`${this.name} stream: 접속 실패 - ${e}`);
      this.scheduleReconnect();
      return;
    }
    let opened = false;
    ws.addEventListener("open", () => {
      if (this.closed) { ws.close(); return; }
      opened = true;
      this.socket = ws;
      this.isSocketOpen = true;
      this.lastFrameAt = Date.now();
      this.attempt = 0;
      log.info(`${this.name} stream: connected`);
      Promise.resolve()
        .then(() => this.onOpen())
        .catch((e) => log.error(`${this.name} stream: onOpen 실패 - ${e}`));
    });
    ws.addEventListener("message", (event: MessageEvent) => {
      this.lastFrameAt = Date.now();
      const data = event.data;
      if (typeof data === "string") this.dispatch(data);
    });
    ws.addEventListener("close", (event: CloseEvent) => {
      if (this.socket !== ws && opened) return; // 이미 대체된 소켓
      if (!opened) { // 접속 자체가 실패
        if (!this.closed) log.warn(`${this.name} stream: 접속 실패 - close ${event.code}`);
        this.scheduleReconnect();
        return;
      }
      this.handleDisconnect(`close ${event.code} ${event.reason ?? ""}`.trim());
    });
    ws.addEventListener("error", () => {
      // 오류 뒤에는 항상 close 이벤트가 따라오므로 재접속은 거기서 잡는다
    });
  }

  private handleDisconnect(reason: string): void {
    const wasOpen = this.isSocketOpen;
    this.socket = null;
    this.isSocketOpen = false;
    if (this.closed) return;
    if (wasOpen) log.warn(`${this.name} stream: disconnected - ${reason}`);
    try { this.onDisconnected(); } catch { /* 무시 */ }
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer) return;
    const n = this.attempt++;
    const delay = Math.min(1000 * 2 ** Math.min(n, 10), this.maxBackoffMs);
    log.info(`${this.name} stream: reconnect in ${delay}ms`);
    this.reconnectTimer = setTimeout(() => { this.reconnectTimer = null; void this.doConnect(); }, delay);
    this.reconnectTimer.unref?.();
  }

  private checkIdle(): void {
    const ws = this.socket;
    if (!ws || this.closed) return;
    if (Date.now() - this.lastFrameAt > this.idleTimeoutMs) {
      log.warn(`${this.name} stream: ${this.idleTimeoutMs}ms 동안 프레임 없음 - 재접속`);
      this.socket = null;
      try { ws.close(); } catch { /* 무시 */ }
      this.handleDisconnect("idle timeout");
    }
  }

  private dispatch(text: string): void {
    if (this.rawFrameHook) { try { this.rawFrameHook(text); } catch { /* 무시 */ } }
    try {
      this.onMessage(text);
    } catch (e) {
      log.error(`${this.name} stream: 메시지 처리 실패 - ${e} / ${text.slice(0, 200)}`);
    }
  }
}

/** 거래소 현지(KST) 날짜 + HHMMSS → Date */
export function kstDateTime(today: string, hhmmss: string): Date {
  const t = hhmmss.trim().padStart(6, "0");
  return new Date(`${today}T${t.slice(0, 2)}:${t.slice(2, 4)}:${t.slice(4, 6)}+09:00`);
}
