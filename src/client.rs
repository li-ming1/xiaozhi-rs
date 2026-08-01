//! 客户端核心：全双工对话。收发端点分离，select! 中凭 disjoint borrow 并行无锁。

use anyhow::Result;
use log::{debug, info, warn};
use tokio::sync::mpsc;
use tokio::time::{interval, interval_at, Duration, Instant, MissedTickBehavior};

use crate::audio::AudioManager;
use crate::message::{ListenState, Message, ServerMessage};
use crate::opus_codec::OpusCodec;
use crate::protocol::{connect_and_handshake, ReceivedMessage, WsReceiver, WsSender};

/// 发送任务消息：音频帧与心跳复用同一 mpsc 通道，由独立 send_task 串行消费。
enum OutMsg {
    Audio(Vec<u8>),
    Ping,
}

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
    // 独立发送任务：网络写阻塞隔离，主循环 tick 仅 try_send 非阻塞。
    out_tx: Option<mpsc::Sender<OutMsg>>,
    // TTS 播放状态 + 下行音频到达时间，用于断流时 PLC 补帧判定。
    tts_active: bool,
    last_audio_recv: Instant,
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
            out_tx: None,
            tts_active: false,
            last_audio_recv: Instant::now(),
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

    /// 单轮对话循环：监听开启 → 启动音频 → 独立发送任务 → select! 收发与心跳。
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

        self.tts_active = false;
        self.last_audio_recv = Instant::now();
        info!("开始实时对话（按 Ctrl+C 退出）");

        // 拆分 sender 给独立发送任务：网络写阻塞隔离在 send_task，
        // 主循环 tick 仅 try_send 非阻塞，避免 socket 写慢饿死 tick → 输入队列积压丢帧。
        let sender = self.sender.take().unwrap();
        let (out_tx, out_rx) = mpsc::channel::<OutMsg>(25);
        let send_handle = tokio::spawn(Self::run_send_task(out_rx, sender));
        self.out_tx = Some(out_tx);

        // 发送节拍与音频帧对齐（20ms）；Delay 行为避免调度抖动后突发补发多帧。
        // 心跳延后 30s 起，借 Sink flush 顺带排空排队 Pong。
        let mut send_tick = interval(Duration::from_millis(20));
        send_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut heartbeat = interval_at(
            Instant::now() + Duration::from_secs(30),
            Duration::from_secs(30),
        );

        let result = self.conversation_loop(&mut send_tick, &mut heartbeat).await;

        // 退出清理：关闭通道触发 send_task 结束，并中止任务句柄，避免泄漏。
        self.out_tx.take();
        send_handle.abort();
        result
    }

    /// select! 主循环：接收 / 上行编码发送 / 下行 PLC 补帧 / 心跳。
    async fn conversation_loop(
        &mut self,
        send_tick: &mut tokio::time::Interval,
        heartbeat: &mut tokio::time::Interval,
    ) -> Result<()> {
        loop {
            tokio::select! {
                // 接收分支（借用 &mut self.receiver）。
                result = self.receiver.as_mut().unwrap().receive() => {
                    match result {
                        Ok(ReceivedMessage::Json(json_msg)) => {
                            self.handle_json_message(json_msg)?;
                        }
                        Ok(ReceivedMessage::Audio(audio_data)) => {
                            // 记录到达时间，供 PLC 断流判定；解码为栈数组后入播放队列。
                            self.last_audio_recv = Instant::now();
                            match self.opus.decode(&audio_data) {
                                Ok(decoded) => self.audio.write_frame(decoded),
                                Err(e) => warn!("Opus 解码失败: {}", e),
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }

                // 上行分支：每 tick 仅取一帧编码后非阻塞入队（满则丢最新帧，
                // 在网络抖动层丢弃，而非采集层积压）。Delay 行为防突发补发。
                _ = send_tick.tick() => {
                    if let Some(frame) = self.audio.read_frame() {
                        match self.opus.encode(&frame) {
                            Ok(encoded) => {
                                if self.out_tx.as_ref().unwrap().try_send(OutMsg::Audio(encoded)).is_err() {
                                    warn!("[DROP] 上行发送队列满，丢最新帧");
                                }
                            }
                            Err(e) => warn!("Opus 编码失败: {}", e),
                        }
                    }
                    // 下行 PLC：TTS 播放中且超过 40ms 未收到音频帧，判定断流，
                    // 用解码器内部状态外推补偿帧填补，避免静音断裂。
                    if self.tts_active
                        && self.last_audio_recv.elapsed() > Duration::from_millis(40)
                    {
                        if let Ok(plc) = self.opus.decode_plc() {
                            self.audio.write_frame(plc);
                        }
                    }
                }

                // 心跳保活，防空闲超时；复用发送队列，与音频帧串行下发。
                _ = heartbeat.tick() => {
                    if self.out_tx.as_ref().unwrap().try_send(OutMsg::Ping).is_err() {
                        warn!("心跳入队失败（发送队列满）");
                    }
                }
            }
        }
    }

    /// 独立发送任务：串行消费 mpsc 通道，承担所有网络写阻塞。
    async fn run_send_task(mut receiver: mpsc::Receiver<OutMsg>, mut sender: WsSender) {
        while let Some(msg) = receiver.recv().await {
            match msg {
                OutMsg::Audio(data) => {
                    if let Err(e) = sender.send_audio(data).await {
                        warn!("音频发送失败: {}", e);
                        return;
                    }
                }
                OutMsg::Ping => {
                    if let Err(e) = sender.send_ping(b"xz".to_vec()).await {
                        warn!("心跳发送失败: {}", e);
                        return;
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
    fn handle_json_message(&mut self, msg: ServerMessage) -> Result<()> {
        use crate::message::TtsState;
        match msg {
            ServerMessage::Tts { state: tts_state } => {
                // TTS 仅切换状态机，正文由 LLM 消息统一展示，避免三处重复渲染。
                match tts_state {
                    TtsState::Start => {
                        // 进入播放：置位并重置到达时间，避免首帧前误触发 PLC。
                        self.tts_active = true;
                        self.last_audio_recv = Instant::now();
                        info!("AI 开始说话");
                    }
                    TtsState::Stop => {
                        self.tts_active = false;
                        info!("AI 说完，继续监听");
                    }
                    TtsState::SentenceStart | TtsState::SentenceStop | TtsState::SentenceEnd => {
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
