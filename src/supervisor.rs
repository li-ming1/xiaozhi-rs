//! `VoiceSupervisor`：连接生命周期状态机。
//! `SelectTransport -> Connect -> Streaming -> Backoff`，网络瞬断/热插拔/服务故障均在内部恢复。
//! 策略：decorrelated-jitter 退避（250ms~30s）；MQTT 熔断；UDP 黑洞检测；10s 窗口网络分级。

use std::time::{Duration, Instant};

use log::{debug, info, warn};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::audio::opus::NetworkGrade;
use crate::audio::{AudioManager, PlaybackMsg};
use crate::error::{Result, VoiceError};
use crate::identity::DeviceIdentity;
use crate::ota::{OtaConfig, OtaMqttConfig};
use crate::protocol::message::{ClientMessage, ListenState, ServerMessage, TtsState};
use crate::protocol::mqtt_udp::MqttUdpTransport;
use crate::protocol::ws::WsTransport;
use crate::protocol::{
    ConnectParams, IncomingEvent, MqttParams, TransportAdapter, TransportHandles,
};

/// 传输类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportKind {
    MqttUdp,
    WebSocket,
}

/// 建连整体超时（TCP/TLS 握手无响应时兜底，避免状态机卡死）。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// 状态机。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    SelectTransport,
    Connect,
    Streaming,
    Backoff,
}

/// 退避：decorrelated jitter，base 250ms，cap 30s。
struct Backoff {
    base: Duration,
    cap: Duration,
    attempt: u64,
    rng: u64,
}

impl Backoff {
    fn new() -> Self {
        Self {
            base: Duration::from_millis(250),
            cap: Duration::from_secs(30),
            attempt: 0,
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next(&mut self) -> Duration {
        self.rng = self.rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // shift 恒 ≤ 8，`1 << shift` 不会溢出。
        let exp = self.base.saturating_mul(1u32 << self.attempt.min(8));
        let upper = exp.min(self.cap);
        let range_ms = upper.as_millis().max(1) as u64;
        let jitter_ms = self.rng % range_ms;
        let delay = Duration::from_millis(jitter_ms.max(25)).min(self.cap);
        self.attempt += 1;
        delay
    }

    fn on_success(&mut self) {
        self.attempt = 0;
    }
}

/// MQTT 熔断器。
struct CircuitBreaker {
    failures: Vec<Instant>,
    cooldown_until: Option<Instant>,
    open_count: u32,
}

impl CircuitBreaker {
    const FAIL_WINDOW: Duration = Duration::from_secs(120);
    const OPEN_COOLDOWN: Duration = Duration::from_secs(300); // 5min 起步
    const OPEN_COOLDOWN_MAX: Duration = Duration::from_secs(1800); // 30min
    const STABLE_RESET: Duration = Duration::from_secs(120);

    fn new() -> Self {
        Self {
            failures: Vec::new(),
            cooldown_until: None,
            open_count: 0,
        }
    }

    fn is_open(&self) -> bool {
        if let Some(until) = self.cooldown_until
            && Instant::now() < until
        {
            return true;
        }
        false
    }

    /// 记录一次失败；返回是否本次打开熔断。
    fn record_failure(&mut self) -> bool {
        let now = Instant::now();
        self.failures.retain(|t| now.duration_since(*t) < Self::FAIL_WINDOW);
        self.failures.push(now);
        if self.failures.len() >= 2 {
            self.open_count += 1;
            let multiplier = 1u32.checked_shl(self.open_count.min(4)).unwrap_or(1);
            let cooldown = Self::OPEN_COOLDOWN.saturating_mul(multiplier).min(Self::OPEN_COOLDOWN_MAX);
            self.cooldown_until = Some(now + cooldown);
            self.failures.clear();
            info!("MQTT 熔断 {}s", cooldown.as_secs());
            true
        } else {
            false
        }
    }

    fn on_stable(&mut self) {
        self.failures.retain(|t| t.elapsed() < Self::STABLE_RESET);
        if self.failures.is_empty() {
            self.open_count = 0;
            self.cooldown_until = None;
        }
    }
}

/// 连接级丢失错误（传输断开或超时），用于 MQTT 熔断决策。
fn is_transport_loss(e: &VoiceError) -> bool {
    matches!(e, VoiceError::Transport(_) | VoiceError::Timeout(_))
}

/// 丢包估计器：按 60ms 帧到达间隙估算 10s 窗口丢包率。
struct LossEstimator {
    last: Option<Instant>,
    received: u64,
    lost: u64,
    window_start: Instant,
    grade: NetworkGrade,
}

impl LossEstimator {
    const FRAME_MS: u64 = 60;
    const WINDOW: Duration = Duration::from_secs(10);
    /// gap 超过此值视为服务器静默期（TTS 间隔/无下行），重置窗口而非计丢失。
    const RESET_GAP_MS: u64 = 1000;

    fn new() -> Self {
        Self {
            last: None,
            received: 0,
            lost: 0,
            window_start: Instant::now(),
            grade: NetworkGrade::Good,
        }
    }

    fn observe_frame(&mut self, now: Instant) {
        if let Some(prev) = self.last {
            let gap_ms = now.duration_since(prev).as_millis() as u64;
            // 服务器静默期无法区分"丢包"与"未发送"：>1s 重置窗口；
            // 否则按帧间隔折算丢失。
            if gap_ms > Self::RESET_GAP_MS {
                self.received = 0;
                self.lost = 0;
                self.window_start = now;
            } else if gap_ms > Self::FRAME_MS * 3 {
                self.lost += gap_ms / Self::FRAME_MS - 1;
            }
        }
        self.last = Some(now);
        self.received += 1;
        if now.duration_since(self.window_start) >= Self::WINDOW {
            let total = self.received + self.lost;
            let ratio = if total > 0 {
                self.lost as f64 / total as f64
            } else {
                0.0
            };
            self.grade = if ratio < 0.03 {
                NetworkGrade::Good
            } else if ratio < 0.10 {
                NetworkGrade::Fair
            } else {
                NetworkGrade::Poor
            };
            self.received = 0;
            self.lost = 0;
            self.window_start = now;
        }
    }
}

/// 监督器。
pub struct VoiceSupervisor {
    identity: DeviceIdentity,
    ota: OtaConfig,
    shutdown: CancellationToken,
    state: State,
    audio: Option<AudioManager>,
    backoff: Backoff,
    mqtt_circuit: CircuitBreaker,
    /// 当前会话的传输类型（每次建连重建，标记"纪元"）。
    epoch: Option<TransportKind>,
    /// 当前会话建连后的统一句柄（由 Connect 状态交给 Streaming 状态，禁止重复建连）。
    handles: Option<TransportHandles>,
}

impl VoiceSupervisor {
    pub fn new(identity: DeviceIdentity, ota: OtaConfig, shutdown: CancellationToken) -> Self {
        let audio = AudioManager::new().ok();
        Self {
            identity,
            ota,
            shutdown,
            state: State::SelectTransport,
            audio,
            backoff: Backoff::new(),
            mqtt_circuit: CircuitBreaker::new(),
            epoch: None,
            handles: None,
        }
    }

    /// 主循环。
    pub async fn run(&mut self) -> Result<()> {
        info!("监督器启动");
        let mut heartbeat = Instant::now();
        loop {
            if self.shutdown.is_cancelled() {
                info!("收到停机信号，退出监督循环");
                self.cleanup_session();
                return Ok(());
            }
            // 心跳：进程静默死亡时，最后一行日志能定位死亡瞬间的状态。
            if heartbeat.elapsed() >= Duration::from_secs(30) {
                info!("监督器心跳: state={:?}", self.state);
                heartbeat = Instant::now();
            }
            self.state = match self.state {
                State::SelectTransport => State::Connect,
                State::Connect => match self.connect().await {
                    Ok(()) => State::Streaming,
                    Err(e) => {
                        warn!("连接失败: {}", e);
                        self.on_connect_error(&e);
                        State::Backoff
                    }
                },
                State::Streaming => match self.stream().await {
                    Ok(()) => {
                        info!("会话正常结束，重新连接");
                        self.cleanup_session();
                        State::SelectTransport
                    }
                    Err(e) => {
                        warn!("流式会话结束: {}", e);
                        self.on_stream_error(&e);
                        State::Backoff
                    }
                },
                State::Backoff => {
                    let delay = self.backoff.next();
                    info!("{}s 后重试", delay.as_secs_f64());
                    tokio::select! {
                        _ = sleep(delay) => {}
                        _ = self.shutdown.cancelled() => {
                            self.cleanup_session();
                            return Ok(());
                        }
                    }
                    State::SelectTransport
                }
            };
        }
    }

    /// MQTT+UDP 链路可用（OTA 已下发且熔断器未打开；冷却期结束后自动恢复尝试）。
    fn mqtt_available(&self) -> bool {
        self.ota.mqtt.is_some() && !self.mqtt_circuit.is_open()
    }

    fn current_transport_kind(&self) -> TransportKind {
        if self.mqtt_available() {
            TransportKind::MqttUdp
        } else {
            TransportKind::WebSocket
        }
    }

    /// 构造连接参数。WebSocket URL 缺失时用空串（MQTT 主链路不受影响；
    /// 选择 WS 时 URI 解析自然失败）。
    fn build_params(&self) -> ConnectParams {
        let ws_url = self.ota.websocket.url.clone().unwrap_or_default();
        let token = self.ota.websocket.token.clone().unwrap_or_default();
        let mqtt = self
            .ota
            .mqtt
            .as_ref()
            .map(|m| derive_mqtt(m, &ws_url, &self.identity.client_id));
        ConnectParams {
            device_id: self.identity.device_id.clone(),
            client_id: self.identity.client_id.clone(),
            token,
            ws_url,
            mqtt,
        }
    }

    /// 建连 + 协商。句柄存入 `self.handles`，由 Streaming 状态取用。
    async fn connect(&mut self) -> Result<()> {
        let kind = self.current_transport_kind();
        info!(
            "选择传输: {}",
            match kind {
                TransportKind::MqttUdp => "MQTT+UDP（主链路）",
                TransportKind::WebSocket => "WebSocket（回退）",
            }
        );
        let params = self.build_params();
        if kind == TransportKind::MqttUdp
            && let Some(m) = &params.mqtt
        {
            info!(
                "MQTT 参数: {}:{}, TLS={}, topic pub={} sub={}",
                m.host, m.port, m.tls, m.publish_topic, m.subscribe_topic
            );
        }
        let adapter = match kind {
            TransportKind::MqttUdp => TransportAdapter::MqttUdp(MqttUdpTransport),
            TransportKind::WebSocket => TransportAdapter::WebSocket(WsTransport),
        };
        let handles = tokio::time::timeout(CONNECT_TIMEOUT, adapter.connect(&params))
            .await
            .map_err(|_| VoiceError::Timeout(format!("连接超时（{}s）", CONNECT_TIMEOUT.as_secs())))??;
        self.epoch = Some(kind);
        self.handles = Some(handles);
        self.backoff.on_success();
        self.mqtt_circuit.on_stable();
        Ok(())
    }

    /// 连接错误处理：MQTT 失败计入熔断。
    fn on_connect_error(&mut self, e: &VoiceError) {
        if is_transport_loss(e) && self.current_transport_kind() == TransportKind::MqttUdp {
            self.mqtt_circuit.record_failure();
        }
        if matches!(e, VoiceError::AuthenticationFailed(_)) {
            warn!("认证失败：请重新激活");
        }
    }

    /// 流式会话。句柄来自 Connect 状态（`self.handles`），此处不再建连。
    async fn stream(&mut self) -> Result<()> {
        let kind = self.epoch.unwrap_or(TransportKind::WebSocket);
        let mut handles = self.handles.take().ok_or(VoiceError::SessionClosed)?;

        self.ensure_audio(handles.server_audio.sample_rate, handles.audio_tx.clone())
            .await?;

        handles
            .control_tx
            .send(ClientMessage::Listen {
                session_id: handles.session_id.clone(),
                state: ListenState::Start,
                mode: Some("realtime".to_string()),
                text: None,
            })
            .await
            .map_err(|e| VoiceError::Transport(format!("发送 listen 失败: {}", e)))?;

        info!("会话开始（{}）", match kind {
            TransportKind::MqttUdp => "MQTT+UDP",
            _ => "WebSocket",
        });

        // 下行发送端固定于本会话（audio 会话内不变），避免每帧重复解包+clone。
        let playback_tx = self.audio.as_ref().and_then(|a| a.playback_sender());
        let mut s = StreamSession::new();
        let result = loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break Ok(()),
                _ = s.tick.tick() => {
                    // UDP 黑洞检测（仅 MQTT+UDP）：首包 10s 无媒体，或中途断流 3s。
                    if kind == TransportKind::MqttUdp && s.ctx.tts_active {
                        let now = Instant::now();
                        let blackhole = if s.ctx.sentence_has_media {
                            s.last_audio.elapsed() >= Duration::from_secs(3)
                        } else {
                            s.ctx
                                .tts_started_at
                                .map(|t| now.duration_since(t) >= Duration::from_secs(10))
                                .unwrap_or(false)
                        };
                        if blackhole {
                            warn!("UDP 黑洞检测：活跃句子无媒体（首包>10s 或中途断流>3s）");
                            break Err(VoiceError::Transport("UDP 黑洞".into()));
                        }
                    }
                    if s.downlink_diag.elapsed() >= Duration::from_secs(2) {
                        debug!("下行诊断: 收到服务器音频帧 {}", s.downlink_frames);
                        s.downlink_frames = 0;
                        s.downlink_diag = Instant::now();
                    }
                    // 网络分级（10s 窗口）：下发捕获侧（编码策略）与播放侧（FEC 解码）。
                    if s.last_grade_check.elapsed() >= Duration::from_secs(10) {
                        let grade = s.loss.grade;
                        if let Some(a) = self.audio.as_ref() {
                            if let Some(tx) = a.grade_sender() {
                                let _ = tx.send(grade).await;
                            }
                            if let Some(tx) = a.playback_grade_sender() {
                                let _ = tx.send(grade).await;
                            }
                        }
                        s.last_grade_check = Instant::now();
                    }
                }
                incoming = handles.incoming_rx.recv() => {
                    match incoming {
                        Some(IncomingEvent::Json(msg)) => {
                            match handle_json(msg, &mut s.ctx, self.audio.as_ref()).await {
                                Ok(Some(())) => break Ok(()),   // goodbye
                                Ok(None) => {}
                                Err(e) => break Err(e),
                            }
                        }
                        Some(IncomingEvent::Audio(data)) => {
                            s.last_audio = Instant::now();
                            s.ctx.sentence_has_media = true;
                            if !s.first_frame_logged {
                                s.first_frame_logged = true;
                                let hex: Vec<String> =
                                    data.iter().take(16).map(|b| format!("{:02x}", b)).collect();
                                debug!("下行首帧 hex (len={}): {}", data.len(), hex.join(" "));
                            }
                            s.downlink_frames += 1;
                            // 丢包估计仅对 UDP 有意义（TCP 可靠传输，间隙是服务器停顿而非丢包）。
                            if kind == TransportKind::MqttUdp {
                                s.loss.observe_frame(Instant::now());
                            }
                            if let Some(tx) = &playback_tx
                                && let Err(e) = tx.send(PlaybackMsg::Audio(data)).await
                            {
                                break Err(VoiceError::Audio(format!("下行队列失败: {}", e)));
                            }
                        }
                        Some(IncomingEvent::Closed) => {
                            break Err(VoiceError::Transport("传输层已断开".into()));
                        }
                        None => break Err(VoiceError::Transport("传输通道关闭".into())),
                    }
                }
            }
        };

        // 异常结束（Err）时尽力通知服务器中断会话（Abort）。
        // 否则服务器会停留在旧会话的 Listening/Speaking 状态，新会话的音频会被忽略约 60s。
        if result.is_err() {
            let _ = handles
                .control_tx
                .send(ClientMessage::Abort {
                    session_id: handles.session_id.clone(),
                    reason: "session_terminated".to_string(),
                })
                .await;
        }
        // 停止音频：下次会话重新建流，编解码器随纪元重建。
        if let Some(a) = self.audio.as_mut() {
            a.stop();
        }
        result
    }

    /// 确保音频管线可用；设备缺失时重建枚举。
    async fn ensure_audio(
        &mut self,
        server_rate: u32,
        encoded_tx: crate::protocol::LatestSlot<Vec<u8>>,
    ) -> Result<()> {
        if self.audio.is_none() {
            self.audio = Some(AudioManager::new().map_err(|e| {
                warn!("音频设备不可用: {}", e);
                VoiceError::Audio(format!("音频设备不可用: {}", e))
            })?);
        }
        let audio = self.audio.as_mut().expect("audio 已创建");
        audio.start(server_rate, encoded_tx).await
    }

    /// 流式错误处理：音频错误 → 停止并重新枚举；MQTT/UDP → 熔断。
    fn on_stream_error(&mut self, e: &VoiceError) {
        if matches!(e, VoiceError::Audio(_)) {
            if let Some(a) = self.audio.as_mut() {
                a.stop();
            }
            self.audio = None;
        }
        if is_transport_loss(e) && self.epoch == Some(TransportKind::MqttUdp) {
            self.mqtt_circuit.record_failure();
        }
        self.epoch = None;
    }

    /// 会话结束清理：停音频、清纪元与句柄（进程保持运行）。
    fn cleanup_session(&mut self) {
        if let Some(a) = self.audio.as_mut() {
            a.stop();
        }
        self.epoch = None;
        self.handles = None;
    }
}

/// topic 归一化：服务器可能下发 "null"/空 字符串，统一回退到缺省值。
fn effective_topic(t: &Option<String>, default: &str) -> String {
    t.as_deref()
        .filter(|s| !s.is_empty() && *s != "null")
        .map(str::to_string)
        .unwrap_or_else(|| default.to_string())
}

/// 由 OTA 配置推导 MQTT 参数。关键规则（与官方一致）：
/// client_id 用服务器下发值；端点无端口时默认 8883(TLS)；subscribe_topic 为 "null"/空 时回退为发布同主题。
fn derive_mqtt(m: &OtaMqttConfig, ws_url: &str, client_id: &str) -> MqttParams {
    let (host, port, tls) = if let Some(ep) = &m.endpoint {
        let ep = ep.trim();
        match ep.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                let p = p.parse().unwrap_or(8883);
                (h.to_string(), p, m.tls || p == 8883)
            }
            _ => (ep.to_string(), 8883, true),
        }
    } else {
        let host = ws_url
            .trim_start_matches("wss://")
            .trim_start_matches("ws://")
            .split('/')
            .next()
            .unwrap_or("")
            .to_string();
        (host, 1883, m.tls)
    };

    // 服务器可能下发 "null"/空 字符串：按缺省处理。
    let publish_topic = effective_topic(&m.publish_topic, "device-server");
    let subscribe_topic = effective_topic(&m.subscribe_topic, &publish_topic);

    MqttParams {
        host,
        port,
        tls,
        username: m.username.clone().unwrap_or_default(),
        password: m.password.clone().unwrap_or_default(),
        mqtt_client_id: m.client_id.clone().unwrap_or_else(|| format!("xz-{}", client_id)),
        publish_topic,
        subscribe_topic,
    }
}

/// 会话上下文：TTS 状态与媒体跟踪（每次流式会话重建）。
struct SessionCtx {
    tts_active: bool,
    sentence_has_media: bool,
    /// TTS 开始时间（UDP 黑洞首包超时判定基准）。
    tts_started_at: Option<Instant>,
}

impl SessionCtx {
    fn new() -> Self {
        Self {
            tts_active: false,
            sentence_has_media: false,
            tts_started_at: None,
        }
    }
}

/// 流式会话状态（每次建流重建）：TTS/媒体跟踪、丢包估计、诊断计时。
struct StreamSession {
    ctx: SessionCtx,
    loss: LossEstimator,
    last_audio: Instant,
    last_grade_check: Instant,
    downlink_frames: u64,
    downlink_diag: Instant,
    first_frame_logged: bool,
    tick: tokio::time::Interval,
}

impl StreamSession {
    fn new() -> Self {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        tick.reset(); // 首个 tick 延迟一个周期。
        Self {
            ctx: SessionCtx::new(),
            loss: LossEstimator::new(),
            last_audio: Instant::now(),
            last_grade_check: Instant::now(),
            downlink_frames: 0,
            downlink_diag: Instant::now(),
            first_frame_logged: false,
            tick,
        }
    }
}

/// 处理服务器 JSON 消息。返回 Some(()) 表示会话应结束（goodbye）。
async fn handle_json(
    msg: ServerMessage,
    ctx: &mut SessionCtx,
    audio: Option<&AudioManager>,
) -> Result<Option<()>> {
    match msg {
        ServerMessage::Tts { state, text } => match state {
            TtsState::Start => {
                debug!("TTS 开始");
                ctx.tts_active = true;
                ctx.sentence_has_media = false;
                ctx.tts_started_at = Some(Instant::now());
                if let Some(a) = audio
                    && let Some(tx) = a.playback_sender()
                {
                    tx.send(PlaybackMsg::Flush).await.map_err(|e| {
                        VoiceError::Audio(format!("TTS 清缓冲失败: {}", e))
                    })?;
                }
            }
            TtsState::Stop => {
                debug!("TTS 结束");
                ctx.tts_active = false;
                ctx.sentence_has_media = false;
                ctx.tts_started_at = None;
            }
            TtsState::SentenceStart => {
                if let Some(t) = text {
                    info!("AI: {}", t);
                }
            }
            TtsState::SentenceStop | TtsState::SentenceEnd => {}
        },
        ServerMessage::Stt { text } => info!("用户: {}", text),
        ServerMessage::Llm { text } => {
            if let Some(t) = text {
                info!("AI: {}", t);
            }
        }
        ServerMessage::Mcp { payload } => debug!("MCP: {:?}", payload),
        ServerMessage::System { command } => {
            info!("系统指令: {}", command);
        }
        ServerMessage::Hello(_) => {}
        ServerMessage::Listen { state } => debug!("监听状态: {}", state),
        ServerMessage::Goodbye { .. } => {
            info!("服务器发送 goodbye，会话结束");
            return Ok(Some(()));
        }
        ServerMessage::Unknown => {}
    }
    Ok(None)
}

/// 实时语音客户端入口：对外唯一接口，内部由 [`VoiceSupervisor`] 恢复各类故障。
pub struct RealtimeVoice {
    identity: DeviceIdentity,
    ota: OtaConfig,
}

impl RealtimeVoice {
    /// 由设备身份与 OTA 配置构造。
    pub fn from_ota(identity: DeviceIdentity, ota: OtaConfig) -> Result<Self> {
        Ok(Self { identity, ota })
    }

    /// 运行至 `shutdown` 取消或不可恢复错误。
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<()> {
        let mut supervisor = VoiceSupervisor::new(self.identity, self.ota, shutdown);
        supervisor.run().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 服务器静默期（gap >1s）应重置窗口，不计入丢失。
    #[test]
    fn loss_estimator_ignores_server_silence() {
        let mut e = LossEstimator::new();
        let t0 = Instant::now();
        for i in 0..10 {
            e.observe_frame(t0 + Duration::from_millis(i * 60));
        }
        e.observe_frame(t0 + Duration::from_millis(3000 + 600));
        assert_eq!(e.lost, 0);
    }

    /// 180ms–1s 内的间隙按帧间隔折算丢失。
    #[test]
    fn loss_estimator_counts_gaps_within_threshold() {
        let mut e = LossEstimator::new();
        let t0 = Instant::now();
        e.observe_frame(t0);
        e.observe_frame(t0 + Duration::from_millis(300));
        assert_eq!(e.lost, 4); // 5 帧间隔 → 丢 4 帧
    }

    #[test]
    fn circuit_breaker_opens_after_two_failures() {
        let mut cb = CircuitBreaker::new();
        assert!(!cb.record_failure());
        assert!(!cb.is_open());
        assert!(cb.record_failure());
        assert!(cb.is_open());
    }

    /// 服务器下发 "null"/空 topic 时应回退缺省值。
    #[test]
    fn derive_mqtt_null_topic_falls_back() {
        let m = OtaMqttConfig {
            endpoint: Some("mqtt.example.com:1883".into()),
            subscribe_topic: Some("null".into()),
            publish_topic: Some("device-server".into()),
            ..Default::default()
        };
        let p = derive_mqtt(&m, "wss://x", "client-1");
        assert_eq!(p.subscribe_topic, "device-server");
        assert_eq!(p.publish_topic, "device-server");
        assert!(!p.tls);
    }

    /// endpoint 无端口时默认 8883 且强制 TLS（与官方一致）。
    #[test]
    fn derive_mqtt_endpoint_without_port_defaults_8883_tls() {
        let m = OtaMqttConfig {
            endpoint: Some("mqtt.example.com".into()),
            ..Default::default()
        };
        let p = derive_mqtt(&m, "wss://x", "client-1");
        assert_eq!(p.host, "mqtt.example.com");
        assert_eq!(p.port, 8883);
        assert!(p.tls);
    }

    /// 退避应始终在 [25ms, 30s] 区间，长期运行不越界。
    #[test]
    fn backoff_stays_within_bounds() {
        let mut b = Backoff::new();
        for _ in 0..40 {
            let d = b.next();
            assert!(d >= Duration::from_millis(25), "delay 过小: {:?}", d);
            assert!(d <= Duration::from_secs(30), "delay 超上限: {:?}", d);
        }
    }

    /// 成功后重置，退避重新从 base 区间（≤250ms）起步。
    #[test]
    fn backoff_resets_after_success() {
        let mut b = Backoff::new();
        b.next();
        b.next();
        b.on_success();
        let first = b.next();
        assert!(
            first <= Duration::from_millis(250),
            "重置后 delay 仍过大: {:?}",
            first
        );
    }
}
