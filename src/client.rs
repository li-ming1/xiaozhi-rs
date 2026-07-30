//! 客户端核心逻辑
//!
//! 实现全双工音频对话

use anyhow::Result;
use log::{debug, info, warn};
use tokio::time::{interval, Duration};

use crate::audio::AudioManager;
use crate::message::{ListenState, Message, ServerMessage};
use crate::opus_codec::OpusCodec;
use crate::protocol::{ReceivedMessage, WebSocketProtocol};

/// 客户端状态
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum State {
    Idle,
    Connecting,
    Connected,
    Listening,
    Speaking,
}

/// 客户端
pub struct Client {
    protocol: WebSocketProtocol,
    audio: AudioManager,
    opus: OpusCodec,
    state: State,
}

impl Client {
    pub fn new(url: String, token: String, device_id: String, client_id: String) -> Self {
        Self {
            protocol: WebSocketProtocol::new(url, token, device_id, client_id),
            audio: AudioManager::new().expect("音频初始化失败"),
            opus: OpusCodec::new().expect("Opus初始化失败"),
            state: State::Idle,
        }
    }

    /// 连接服务器
    pub async fn connect(&mut self) -> Result<()> {
        self.state = State::Connecting;

        // 1. WebSocket 连接
        self.protocol.connect().await?;

        // 2. hello 握手
        self.protocol.send_hello().await?;

        self.state = State::Connected;
        info!("客户端已连接");
        Ok(())
    }

    /// 开始语音对话（全双工）
    pub async fn start_conversation(&mut self) -> Result<()> {
        const MAX_RETRIES: u32 = 5;
        // 分离两个计数器：对话失败与重连失败各自独立
        // 旧实现重连成功即重置对话计数，会导致"对话持续失败但重连一直成功"的死循环
        let mut conv_failures = 0u32;
        let mut reconnect_failures = 0u32;

        loop {
            match self.run_conversation().await {
                Ok(_) => break,
                Err(e) => {
                    conv_failures += 1;
                    if conv_failures >= MAX_RETRIES {
                        return Err(e);
                    }

                    let delay = std::cmp::min(conv_failures * 2, 10);
                    warn!("连接断开: {}，{}秒后重试 (对话失败 {}/{})", e, delay, conv_failures, MAX_RETRIES);

                    tokio::time::sleep(Duration::from_secs(delay as u64)).await;

                    // 重连：失败独立计数，不因重连成功而重置对话失败计数
                    if let Err(re) = self.reconnect().await {
                        reconnect_failures += 1;
                        warn!("重连失败: {} (重连失败 {}/{})", re, reconnect_failures, MAX_RETRIES);
                        if reconnect_failures >= MAX_RETRIES {
                            return Err(re);
                        }
                    } else {
                        reconnect_failures = 0;
                    }
                }
            }
        }

        Ok(())
    }

    /// 运行对话循环
    async fn run_conversation(&mut self) -> Result<()> {
        self.state = State::Listening;

        // 发送监听开始
        let session_id = self.protocol.session_id().await;
        let msg = Message::Listen {
            session_id,
            state: ListenState::Start,
            mode: Some("realtime".to_string()),
            text: None,
        };
        self.protocol.send_json(&msg).await?;

        // 启动音频
        self.audio.start_capture()?;
        self.audio.start_playback()?;

        info!("开始实时对话（按 Ctrl+C 退出）");

        // 发送定时器（20ms，匹配音频帧）
        let mut send_tick = interval(Duration::from_millis(20));

        loop {
            tokio::select! {
                // 接收 WebSocket 消息
                result = self.protocol.receive() => {
                    match result {
                        Ok(ReceivedMessage::Json(json_msg)) => {
                            self.handle_json_message(json_msg)?;
                        }
                        Ok(ReceivedMessage::Audio(audio_data)) => {
                            // 解码 Opus
                            match self.opus.decode(&audio_data) {
                                Ok(decoded) => {
                                    self.audio.write_frame(decoded);
                                }
                                Err(e) => {
                                    warn!("Opus 解码失败: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }

                // 定时发送音频（每 20ms 一帧）
                // 每次 tick 只取一帧发送，避免缓冲累积导致延迟线性增长
                _ = send_tick.tick() => {
                    if let Some(frame) = self.audio.read_frame() {
                        // 编码并发送
                        match self.opus.encode(&frame) {
                            Ok(encoded) => {
                                if let Err(e) = self.protocol.send_audio(&encoded).await {
                                    warn!("音频发送失败: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Opus 编码失败: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// 重连服务器
    async fn reconnect(&mut self) -> Result<()> {
        info!("正在重连...");
        self.connect().await
    }

    /// 处理 JSON 消息
    fn handle_json_message(&mut self, msg: ServerMessage) -> Result<()> {
        use crate::message::TtsState;
        match msg {
            ServerMessage::Tts { state: tts_state, text: _ } => {
                // TTS 只负责状态机切换，不显示文本
                // 文本统一由 LLM 消息显示，避免 Stt/Llm/Tts 三处重复
                match tts_state {
                    TtsState::Start => {
                        self.state = State::Speaking;
                        info!("AI 开始说话");
                    }
                    TtsState::Stop => {
                        self.state = State::Listening;
                        info!("AI 说完，继续监听");
                    }
                    TtsState::SentenceStart | TtsState::SentenceStop | TtsState::SentenceEnd => {
                        debug!("TTS 句子状态: {:?}", tts_state);
                    }
                }
            }
            ServerMessage::Listen { state: listen_state, text: _ } => {
                debug!("监听状态: {}", listen_state);
            }
            ServerMessage::Stt { text, state: _ } => {
                // 用户语音识别结果（用户说的话）
                info!("用户: {}", text);
            }
            ServerMessage::Llm { text, emotion } => {
                // AI 完整回复文本（一次显示，不与 TTS 分句重复）
                if let Some(text) = text {
                    info!("AI: {}", text);
                }
                if let Some(emotion) = emotion {
                    debug!("AI 表情: {}", emotion);
                }
            }
            ServerMessage::Mcp { name, payload } => {
                debug!("MCP 消息: name={:?}, payload={:?}", name, payload);
            }
            ServerMessage::Goodbye { session_id } => {
                info!("会话结束: {}", session_id);
                return Err(anyhow::anyhow!("会话已关闭"));
            }
            _ => {
                debug!("消息: {:?}", msg);
            }
        }
        Ok(())
    }
}