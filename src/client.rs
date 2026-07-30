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
        let mut retry_count = 0;

        loop {
            match self.run_conversation().await {
                Ok(_) => break,
                Err(e) => {
                    if retry_count >= MAX_RETRIES {
                        return Err(e);
                    }
                    
                    retry_count += 1;
                    let delay = std::cmp::min(retry_count * 2, 10);
                    warn!("连接断开: {}，{}秒后重试 ({}/{})", e, delay, retry_count, MAX_RETRIES);
                    
                    tokio::time::sleep(Duration::from_secs(delay as u64)).await;
                    
                    // 重连
                    if let Err(e) = self.reconnect().await {
                        warn!("重连失败: {}", e);
                    } else {
                        retry_count = 0; // 重置重试计数
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

        // 音频帧缓冲
        let frame_size = crate::audio::FRAME_SIZE;
        let mut audio_buffer: Vec<f32> = Vec::with_capacity(frame_size * 2);

        // 定时器
        let mut tick = interval(Duration::from_millis(10));

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

                // 定时发送音频
                _ = tick.tick() => {
                    // 尝试读取音频数据
                    while let Some(chunk) = self.audio.read_frame() {
                        audio_buffer.extend_from_slice(&chunk);
                    }
                    
                    // 累积到一帧后发送
                    while audio_buffer.len() >= frame_size {
                        let frame: Vec<f32> = audio_buffer.drain(..frame_size).collect();
                        
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
        match msg {
            ServerMessage::Tts { state: tts_state, text } => {
                match tts_state {
                    crate::message::TtsState::Start => {
                        self.state = State::Speaking;
                        info!("AI 开始说话");
                    }
                    crate::message::TtsState::Stop => {
                        self.state = State::Listening;
                        info!("AI 说完，继续监听");
                    }
                    crate::message::TtsState::SentenceStart => {
                        debug!("AI 开始句子");
                    }
                    crate::message::TtsState::SentenceStop | crate::message::TtsState::SentenceEnd => {
                        debug!("AI 句子结束");
                    }
                }
                // 只在 TTS stop 时显示文本（避免重复）
                if matches!(tts_state, crate::message::TtsState::SentenceStop | crate::message::TtsState::SentenceEnd) {
                    if let Some(text) = text {
                        info!("AI: {}", text);
                    }
                }
            }
            ServerMessage::Listen { state: listen_state, text } => {
                info!("监听状态: {}", listen_state);
                if let Some(text) = text {
                    info!("用户: {}", text);
                }
            }
            ServerMessage::Stt { text, state: _ } => {
                info!("语音识别: {}", text);
            }
            ServerMessage::Llm { text, emotion } => {
                if let Some(text) = text {
                    info!("AI 回复: {}", text);
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

    /// 关闭连接
    pub async fn close(&mut self) -> Result<()> {
        self.audio.stop_capture();
        self.audio.stop_playback();
        self.protocol.close().await?;
        self.state = State::Idle;
        Ok(())
    }

    /// 获取状态
    pub fn state(&self) -> State {
        self.state
    }
}