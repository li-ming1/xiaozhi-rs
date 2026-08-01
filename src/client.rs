//! 客户端核心：全双工对话。收发端点分离，select! 中凭 disjoint borrow 并行无锁。

use anyhow::Result;
use log::{debug, info, warn};
use tokio::time::{interval, interval_at, Duration, Instant};

use crate::audio::AudioManager;
use crate::message::{ListenState, Message, ServerMessage};
use crate::opus_codec::OpusCodec;
use crate::protocol::{connect_and_handshake, ReceivedMessage, WsReceiver, WsSender};

/// 客户端：持连接参数与收发端点，对话期独占音频与编解码器。
pub struct Client {
    // 重连所需连接参数
    url: String,
    token: String,
    device_id: String,
    client_id: String,
    // 收发端点分离，select! 内 disjoint borrow
    sender: Option<WsSender>,
    receiver: Option<WsReceiver>,
    session_id: String,
    audio: AudioManager,
    opus: OpusCodec,
}

impl Client {
    pub fn new(url: String, token: String, device_id: String, client_id: String) -> Self {
        Self {
            url,
            token,
            device_id,
            client_id,
            sender: None,
            receiver: None,
            session_id: String::new(),
            audio: AudioManager::new().expect("音频初始化失败"),
            opus: OpusCodec::new().expect("Opus初始化失败"),
        }
    }

    /// 握手建连。
    pub async fn connect(&mut self) -> Result<()> {
        let (sender, receiver, session_id) = connect_and_handshake(
            &self.url,
            &self.token,
            &self.device_id,
            &self.client_id,
        )
        .await?;

        self.sender = Some(sender);
        self.receiver = Some(receiver);
        self.session_id = session_id;
        info!("客户端已连接");
        Ok(())
    }

    /// 全双工对话，含指数退避重连。
    pub async fn start_conversation(&mut self) -> Result<()> {
        const MAX_RETRIES: u32 = 5;
        // 对话与重连失败各自计次，避免"对话持续失败却重连常成"的死循环。
        let mut conv_failures = 0u32;
        let mut reconnect_failures = 0u32;

        loop {
            match self.run_conversation().await {
                Ok(_) => break,
                Err(e) => {
                    // 立即停流，避免输入回调在重连窗口持续积压旧音频并占用设备/CPU。
                    self.audio.stop();
                    conv_failures += 1;
                    if conv_failures >= MAX_RETRIES {
                        return Err(e);
                    }

                    let delay = std::cmp::min(conv_failures * 2, 10);
                    warn!("连接断开: {}，{}秒后重试 (对话失败 {}/{})", e, delay, conv_failures, MAX_RETRIES);

                    tokio::time::sleep(Duration::from_secs(delay as u64)).await;

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

    /// 单轮对话循环：监听开启 → 启动音频 → select! 收发与心跳。
    async fn run_conversation(&mut self) -> Result<()> {
        let msg = Message::Listen {
            session_id: self.session_id.clone(),
            state: ListenState::Start,
            mode: Some("realtime".to_string()),
            text: None,
        };
        self.sender.as_mut().unwrap().send_json(&msg).await?;

        self.audio.start_capture()?;
        self.audio.start_playback()?;

        info!("开始实时对话（按 Ctrl+C 退出）");

        // 发送节拍与音频帧对齐（20ms）；心跳延后 30s 起，借 Sink flush 顺带排空排队 Pong。
        let mut send_tick = interval(Duration::from_millis(20));
        let mut heartbeat = interval_at(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );

        loop {
            tokio::select! {
                // 接收分支（借用 &mut self.receiver）。
                result = self.receiver.as_mut().unwrap().receive() => {
                    match result {
                        Ok(ReceivedMessage::Json(json_msg)) => {
                            self.handle_json_message(json_msg)?;
                        }
                        Ok(ReceivedMessage::Audio(audio_data)) => {
                            // 解码为栈数组，write_frame 零堆分配。
                            match self.opus.decode(&audio_data) {
                                Ok(decoded) => {
                                    self.audio.write_frame(decoded);
                                }
                                Err(_) => {
                                    // 解码失败（丢包/损坏）：PLC 重建当前帧维持连续性，仍失败才丢弃。
                                    match self.opus.decode(&[]) {
                                        Ok(plc) => self.audio.write_frame(plc),
                                        Err(e) => warn!("Opus 解码失败: {}", e),
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }

                // 发送分支（借用 &mut self.sender，与 receiver disjoint，无锁）。每 tick 仅发一帧，防止缓冲累积致延迟线性增长。
                _ = send_tick.tick() => {
                    if let Some(frame) = self.audio.read_frame() {
                        match self.opus.encode(&frame) {
                            Ok(encoded) => {
                                // encoded 直接 move 入 send_audio，省一次 to_vec。
                                if let Err(e) = self.sender.as_mut().unwrap().send_audio(encoded).await {
                                    warn!("音频发送失败: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Opus 编码失败: {}", e);
                            }
                        }
                    }
                }

                // 心跳保活，防空闲超时。
                _ = heartbeat.tick() => {
                    if let Err(e) = self.sender.as_mut().unwrap().send_ping(b"xz".to_vec()).await {
                        warn!("心跳发送失败: {}", e);
                    }
                }
            }
        }
    }

    /// 重新建连。
    async fn reconnect(&mut self) -> Result<()> {
        info!("正在重连...");
        self.connect().await
    }

    /// 处理服务器 JSON 消息。
    fn handle_json_message(&self, msg: ServerMessage) -> Result<()> {
        use crate::message::TtsState;
        match msg {
            ServerMessage::Tts { state: tts_state, text } => {
                // start/stop 切换状态机；sentence_start 携带分句正文，流式落日志。
                match tts_state {
                    TtsState::Start => {
                        info!("AI 开始说话");
                        // 清空上一句残留播放缓冲，避免尾帧串入新句开头。
                        self.audio.clear_playback();
                    }
                    TtsState::Stop => info!("AI 说完，继续监听"),
                    TtsState::SentenceStart => {
                        if let Some(text) = text {
                            info!("AI: {}", text);
                        }
                    }
                    TtsState::SentenceStop | TtsState::SentenceEnd => {
                        debug!("TTS 句子状态: {:?}", tts_state);
                    }
                }
            }
            ServerMessage::Listen { state: listen_state } => {
                debug!("监听状态: {}", listen_state);
            }
            ServerMessage::Stt { text } => {
                info!("用户: {}", text);
            }
            ServerMessage::Llm { text, emotion } => {
                // 完整回复一次展示，不与 TTS 分句重复。
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
            ServerMessage::Hello { .. } => {
                debug!("收到 hello");
            }
        }
        Ok(())
    }
}
