//! `VoiceSupervisor`：连接生命周期状态机。
//!
//! 状态流转固定为 `Bootstrap -> SelectTransport -> Connect -> Negotiate -> Streaming -> Backoff`。
//! 网络瞬断、设备热插拔与临时服务故障均在内部恢复；认证失败立即失效并刷新。
//!
//! 关键策略（重构方案）：
//! - 无限 decorrelated-jitter 退避，base=250ms、cap=30s。
//! - MQTT 熔断：120s 内失败 2 次 → 5min，后续 10/20/30min 递增；稳定 120s 清零。
//! - UDP 黑洞：活跃句子 3s 无媒体 → 废弃纪元并回退 WebSocket。
//! - 网络分级：10s 窗口丢包率映射 Good/Fair/Poor，驱动 Opus 编码策略。

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
use crate::session::{SessionEpoch, TransportKind};

/// 状态机。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Bootstrap,
    SelectTransport,
    Connect,
    Negotiate,
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
        // 简单 LCG。
        self.rng = self.rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let shift = self.attempt.min(8) as u32;
        let exp = self.base.saturating_mul(1u32.checked_shl(shift).unwrap_or(1));
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

    /// 稳定运行后清零。
    fn on_stable(&mut self) {
        self.failures.retain(|t| t.elapsed() < Self::STABLE_RESET);
        if self.failures.is_empty() {
            self.open_count = 0;
            self.cooldown_until = None;
        }
    }
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
            if gap_ms > Self::FRAME_MS * 3 {
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
    /// 主链路偏好（MQTT+UDP）。熔断或黑洞时暂时切换 WebSocket。
    prefer_mqtt: bool,
    epoch: Option<SessionEpoch>,
    /// 当前纪元建连后的统一句柄（由 Connect 状态交给 Streaming 状态，禁止重复建连）。
    handles: Option<TransportHandles>,
}

impl VoiceSupervisor {
    pub fn new(identity: DeviceIdentity, ota: OtaConfig, shutdown: CancellationToken) -> Self {
        let audio = AudioManager::new().ok();
        Self {
            identity,
            ota,
            shutdown,
            state: State::Bootstrap,
            audio,
            backoff: Backoff::new(),
            mqtt_circuit: CircuitBreaker::new(),
            prefer_mqtt: true,
            epoch: None,
            handles: None,
        }
    }

    /// 主循环。
    pub async fn run(&mut self) -> Result<()> {
        self.state = State::Bootstrap;
        let mut heartbeat = Instant::now();
        loop {
            if self.shutdown.is_cancelled() {
                info!("收到停机信号，退出监督循环");
                self.cleanup();
                return Ok(());
            }
            // 心跳：进程静默死亡时，最后一行日志能定位死亡瞬间的状态。
            if heartbeat.elapsed() >= Duration::from_secs(30) {
                info!("监督器心跳: state={:?}", self.state);
                heartbeat = Instant::now();
            }
            self.state = match self.state {
                State::Bootstrap => {
                    info!("监督器启动");
                    State::SelectTransport
                }
                State::SelectTransport => {
                    self.select_transport();
                    State::Connect
                }
                State::Connect => {
                    self.state = State::Negotiate;
                    match self.connect().await {
                        Ok(()) => State::Streaming,
                        Err(e) => {
                            warn!("连接失败: {}", e);
                            self.on_connect_error(&e);
                            State::Backoff
                        }
                    }
                }
                State::Negotiate => State::Streaming,
                State::Streaming => match self.stream().await {
                    // 会话正常结束（goodbye）也保持运行：回到传输选择，不退出进程。
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
                            self.cleanup();
                            return Ok(());
                        }
                    }
                    State::SelectTransport
                }
            };
        }
    }

    /// 选择传输：MQTT+UDP 优先（未熔断），否则 WebSocket。
    fn select_transport(&mut self) {
        let mqtt_ok = self.ota.mqtt.is_some() && !self.mqtt_circuit.is_open();
        if self.prefer_mqtt && mqtt_ok {
            info!("选择传输: MQTT+UDP（主链路）");
        } else {
            info!("选择传输: WebSocket（回退）");
        }
        self.state = State::SelectTransport;
    }

    fn current_transport_kind(&self) -> TransportKind {
        let mqtt_ok = self.ota.mqtt.is_some() && !self.mqtt_circuit.is_open();
        if self.prefer_mqtt && mqtt_ok {
            TransportKind::MqttUdp
        } else {
            TransportKind::WebSocket
        }
    }

    /// 构造连接参数。
    fn build_params(&self) -> Result<ConnectParams> {
        let ws_url = self
            .ota
            .websocket
            .url
            .clone()
            .ok_or_else(|| VoiceError::InvalidConfig("OTA 未提供 WebSocket URL".into()))?;
        let token = self.ota.websocket.token.clone().unwrap_or_default();
        let mqtt = self
            .ota
            .mqtt
            .as_ref()
            .map(|m| derive_mqtt(m, &ws_url, &self.identity.client_id));
        Ok(ConnectParams {
            device_id: self.identity.device_id.clone(),
            client_id: self.identity.client_id.clone(),
            token,
            ws_url,
            mqtt,
        })
    }

    /// 建连 + 协商。句柄存入 `self.handles`，由 Streaming 状态取用。
    async fn connect(&mut self) -> Result<()> {
        let params = self.build_params()?;
        let kind = self.current_transport_kind();
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
            TransportKind::WebSocket => TransportAdapter::WebSocket(WsTransport::default()),
        };
        let handles = adapter.connect(&params).await?;
        self.epoch = Some(SessionEpoch::new(handles.session_id.clone(), kind));
        self.handles = Some(handles);
        self.backoff.on_success();
        self.mqtt_circuit.on_stable();
        Ok(())
    }

    /// 连接错误处理：MQTT 失败计入熔断。
    fn on_connect_error(&mut self, e: &VoiceError) {
        if matches!(e, VoiceError::Transport(_) | VoiceError::Timeout(_))
            && self.current_transport_kind() == TransportKind::MqttUdp
            && self.mqtt_circuit.record_failure()
        {
            self.prefer_mqtt = false;
        }
        if matches!(e, VoiceError::AuthenticationFailed(_)) {
            warn!("认证失败：请重新激活");
        }
    }

    /// 流式会话。句柄来自 Connect 状态（`self.handles`），此处不再建连。
    async fn stream(&mut self) -> Result<()> {
        let kind = self
            .epoch
            .as_ref()
            .map(|e| e.transport)
            .unwrap_or(TransportKind::WebSocket);
        let mut handles = self
            .handles
            .take()
            .ok_or_else(|| VoiceError::SessionClosed)?;

        // 启动音频（设备缺失/错误时返回瞬态错误进入退避）。
        self.ensure_audio(handles.server_audio.sample_rate, handles.audio_tx.clone())
            .await?;

        // 发送 listen start。
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

        let mut tts_active = false;
        let mut last_audio = Instant::now();
        // 当前句子是否已收到过音频（区分"首包等待"与"中途断流"）。
        let mut sentence_has_media = false;
        let mut tts_started_at: Option<Instant> = None;
        let mut loss = LossEstimator::new();
        let mut last_grade_check = Instant::now();
        let mut downlink_frames: u64 = 0;
        let mut downlink_diag = Instant::now();
        let mut first_frame_logged = false;

        let result = loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => break Ok(()),
                _ = tokio::time::sleep(Duration::from_millis(250)) => {
                    // UDP 黑洞检测仅对 MQTT+UDP 生效（TCP 无黑洞概念）。
                    // - 首包等待：TTS start 后 10s 无媒体（TTS 生成延迟属正常）；
                    // - 中途断流：已收到媒体的句子 3s 无媒体。
                    if kind == TransportKind::MqttUdp && tts_active {
                        let now = Instant::now();
                        let blackhole = if sentence_has_media {
                            last_audio.elapsed() >= Duration::from_secs(3)
                        } else {
                            tts_started_at
                                .map(|t| now.duration_since(t) >= Duration::from_secs(10))
                                .unwrap_or(false)
                        };
                        if blackhole {
                            warn!("UDP 黑洞检测：活跃句子无媒体（首包>10s 或中途断流>3s）");
                            break Err(VoiceError::Transport("UDP 黑洞".into()));
                        }
                    }
                    // 下行诊断：每 2s 输出收到的服务器音频帧数（RUST_LOG=debug 时可见）。
                    if downlink_diag.elapsed() >= Duration::from_secs(2) {
                        debug!("下行诊断: 收到服务器音频帧 {}", downlink_frames);
                        downlink_frames = 0;
                        downlink_diag = Instant::now();
                    }
                    // 网络分级（10s 窗口）。
                    if last_grade_check.elapsed() >= Duration::from_secs(10) {
                        let grade = loss.grade;
                        if let Some(tx) = self.audio.as_ref().and_then(|a| a.grade_sender()) {
                            let _ = tx.send(grade).await;
                        }
                        last_grade_check = Instant::now();
                    }
                }
                incoming = handles.incoming_rx.recv() => {
                    match incoming {
                        Some(IncomingEvent::Json(msg)) => {
                            match handle_json(msg, &handles, &mut tts_active, &mut sentence_has_media, &mut tts_started_at, self.audio.as_ref()).await {
                                Ok(Some(())) => break Ok(()),   // goodbye
                                Ok(None) => {}
                                Err(e) => break Err(e),
                            }
                        }
                        Some(IncomingEvent::Audio(data)) => {
                            last_audio = Instant::now();
                            sentence_has_media = true;
                            if !first_frame_logged {
                                first_frame_logged = true;
                                let hex: Vec<String> =
                                    data.iter().take(16).map(|b| format!("{:02x}", b)).collect();
                                debug!("下行首帧 hex (len={}): {}", data.len(), hex.join(" "));
                            }
                            downlink_frames += 1;
                            loss.observe_frame(Instant::now());
                            if let Some(tx) = self.audio.as_ref().and_then(|a| a.playback_sender())
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

        // 停止音频（下次会话重新建流，编解码器随纪元重建）。
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
        if matches!(e, VoiceError::Transport(_) | VoiceError::Timeout(_))
            && self.epoch.as_ref().map(|e| e.transport) == Some(TransportKind::MqttUdp)
            && self.mqtt_circuit.record_failure()
        {
            self.prefer_mqtt = false;
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

    fn cleanup(&mut self) {
        self.cleanup_session();
    }
}

/// 由 OTA 配置推导 MQTT 参数。
///
/// 关键规则（与官方 ESP32 一致）：
/// - client_id 必须使用服务器下发的值（勿自造）。
/// - 端点未带端口时默认 1883（明文）；带端口按端口判断 TLS。
/// - 服务器 `subscribe_topic` 为 "null"/空 时回退为与发布同主题
///   （官方客户端回调不区分 topic，响应即达）。
fn derive_mqtt(m: &OtaMqttConfig, ws_url: &str, client_id: &str) -> MqttParams {
    let (host, port, tls) = if let Some(ep) = &m.endpoint {
        let ep = ep.trim();
        match ep.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                let p = p.parse().unwrap_or(8883);
                (h.to_string(), p, m.tls || p == 8883)
            }
            // 官方 MQTT 默认 TLS 8883（协议文档 §8.1）。
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

    let publish_topic = m
        .publish_topic
        .clone()
        .unwrap_or_else(|| "device-server".to_string());
    // "null" 字符串按缺省处理。
    let subscribe_topic = m
        .subscribe_topic
        .clone()
        .filter(|s| s != "null" && !s.is_empty())
        .unwrap_or_else(|| publish_topic.clone());

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

/// 处理服务器 JSON 消息。返回 Some(()) 表示会话应结束（goodbye）。
async fn handle_json(
    msg: ServerMessage,
    handles: &TransportHandles,
    tts_active: &mut bool,
    sentence_has_media: &mut bool,
    tts_started_at: &mut Option<Instant>,
    audio: Option<&AudioManager>,
) -> Result<Option<()>> {
    match msg {
        ServerMessage::Tts { state, text } => match state {
            TtsState::Start => {
                debug!("TTS 开始");
                *tts_active = true;
                *sentence_has_media = false;
                *tts_started_at = Some(Instant::now());
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
                *tts_active = false;
                *sentence_has_media = false;
                *tts_started_at = None;
            }
            TtsState::SentenceStart => {
                if let Some(t) = text {
                    info!("AI: {}", t);
                }
            }
            TtsState::SentenceStop | TtsState::SentenceEnd => {}
        },
        ServerMessage::Stt { text } => info!("用户: {}", text),
        ServerMessage::Llm { text, .. } => {
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
    let _ = handles;
    Ok(None)
}
