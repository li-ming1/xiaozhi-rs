//! WebSocket 传输：v2 二进制协议（带时间戳，供服务端 AEC）为主，v1 原始 Opus 回退。
//!
//! 协议要点（来自官方 websocket_zh.md）：
//! - hello `version` 字段同时声明二进制协议版本（1=原始 / 2=BinaryProtocol2 / 3=BinaryProtocol3）。
//! - 文本帧承载 JSON，二进制帧承载 Opus。
//! - BinaryProtocol2（16 字节小端头 + payload，与 ESP32 原生 packed struct 一致）：
//!   ```text
//!   u16 version        // 2
//!   u16 type           // 0=OPUS, 1=JSON（WS 下 JSON 走文本帧，故二进制 type 恒为 0）
//!   u32 reserved       // 0
//!   u32 timestamp_ms   // 毫秒，服务端 AEC 用
//!   u32 payload_size
//!   u8  payload[]      // Opus
//!   ```
//!
//! 端序假设：ESP32 为小端且采用 packed struct memcpy，故 BinaryProtocol2 字段为小端。
//! UDP 包头（见 crypto.rs）则协议文档明确为网络字节序（大端）。两者不同，切勿混用。

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_tungstenite::{connect_async_tls_with_config, MaybeTlsStream, WebSocketStream};

use crate::error::{Result, VoiceError};
use crate::protocol::message::{AudioParams, ClientMessage, ServerMessage};

use super::{ConnectParams, IncomingEvent, TransportHandles};

const V2_HEADER_SIZE: usize = 16;
const V2_TYPE_OPUS: u16 = 0;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const CONTROL_CHANNEL_CAP: usize = 16;
const INCOMING_CHANNEL_CAP: usize = 64;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// WebSocket 传输。
pub struct WsTransport {
    /// 二进制协议版本：2 = BinaryProtocol2（时间戳，AEC），1 = 原始 Opus。
    pub binary_version: u16,
}

impl Default for WsTransport {
    fn default() -> Self {
        Self { binary_version: 2 }
    }
}

impl WsTransport {
    pub async fn connect(self, params: &ConnectParams) -> Result<TransportHandles> {
        let binary_version = self.binary_version;
        let (sender, receiver, session_id, server_audio) =
            connect_and_handshake(params, binary_version).await?;

        let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAP);
        let (audio_tx_slot, audio_rx_slot) = super::LatestSlot::<Vec<u8>>::new().pipe();
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_CHANNEL_CAP);
        let (close_tx, close_rx) = mpsc::channel(1);

        // 发送任务：控制（文本 JSON）、音频（二进制 v2/v1）、心跳 Ping。
        let session_start = Instant::now();
        tokio::spawn(send_loop(
            sender,
            control_rx,
            audio_rx_slot,
            binary_version,
            session_start,
        ));

        // 接收任务。
        tokio::spawn(recv_loop(receiver, incoming_tx.clone(), close_rx));

        Ok(TransportHandles {
            session_id,
            server_audio,
            control_tx,
            audio_tx: audio_tx_slot,
            incoming_rx,
            close_tx,
        })
    }
}

/// 建连并完成 hello 协商，返回 (发送端, 接收端, session_id, 服务器音频参数)。
async fn connect_and_handshake(
    params: &ConnectParams,
    binary_version: u16,
) -> Result<(WsSender, WsReceiver, String, AudioParams)> {
    let url = &params.ws_url;
    info!("WebSocket 连接: {} (二进制协议 v{})", url, binary_version);

    let uri: http::Uri = url
        .parse()
        .map_err(|e| VoiceError::Transport(format!("URI 解析失败: {}", e)))?;
    let host = uri
        .host()
        .ok_or_else(|| VoiceError::Transport(format!("URL 缺少 host: {}", url)))?
        .to_string();

    let request = http::Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {}", params.token))
        .header("Protocol-Version", binary_version.to_string())
        .header("Device-Id", &params.device_id)
        .header("Client-Id", &params.client_id)
        .header("Host", host.as_str())
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .map_err(|e| VoiceError::Transport(format!("构建请求失败: {}", e)))?;

    let (ws_stream, _) = connect_async_tls_with_config(request, None, false, None)
        .await
        .map_err(|e| VoiceError::Transport(format!("WebSocket 连接失败: {}", e)))?;

    let (sink, stream) = ws_stream.split();
    let mut sender = WsSender { sink };
    let mut receiver = WsReceiver { stream };

    // hello version = 二进制协议版本。
    let hello = match binary_version {
        1 => ClientMessage::hello_websocket_v1(),
        2 => ClientMessage::hello_websocket_v2(),
        v => {
            return Err(VoiceError::Protocol(format!(
                "不支持的二进制协议版本: {}",
                v
            )))
        }
    };
    sender.send_json(&hello).await?;

    // 等待服务器 hello 响应。
    let session_id;
    let server_audio;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, receiver.receive())
            .await
            .map_err(|_| VoiceError::Timeout("hello 响应超时".into()))??;
        match msg {
            ReceivedMessage::Json(ServerMessage::Hello(h)) => {
                session_id = h.session_id;
                server_audio = h.audio_params.unwrap_or_default();
                info!(
                    "WebSocket 握手成功，session_id={}，下行 {}Hz/{}ms",
                    session_id, server_audio.sample_rate, server_audio.frame_duration
                );
                break;
            }
            ReceivedMessage::Json(other) => {
                return Err(VoiceError::Protocol(format!(
                    "期望 hello 响应，收到: {:?}",
                    other
                )))
            }
            ReceivedMessage::Audio(data) => {
                debug!("握手阶段收到音频 {} 字节，忽略", data.len());
            }
        }
    }

    Ok((sender, receiver, session_id, server_audio))
}

/// WebSocket 发送端：独占 SplitSink。
struct WsSender {
    sink: futures_util::stream::SplitSink<WsStream, WsMessage>,
}

impl WsSender {
    async fn send_json(&mut self, msg: &ClientMessage) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        debug!("WS 发送 JSON: {}", json);
        self.sink
            .send(WsMessage::Text(json.into()))
            .await
            .map_err(|e| VoiceError::Transport(format!("发送失败: {}", e)))?;
        Ok(())
    }

    async fn send_audio_v2(&mut self, opus: &[u8], timestamp_ms: u32) -> Result<()> {
        // 注意：BinaryProtocol2 为网络字节序（大端），官方 ESP32 发送端
        // 对 version/timestamp/payload_size 调用 htons/htonl，接收端 ntohs/ntohl。
        let mut frame = Vec::with_capacity(V2_HEADER_SIZE + opus.len());
        frame.extend_from_slice(&2u16.to_be_bytes()); // version
        frame.extend_from_slice(&V2_TYPE_OPUS.to_be_bytes()); // type
        frame.extend_from_slice(&0u32.to_be_bytes()); // reserved
        frame.extend_from_slice(&timestamp_ms.to_be_bytes());
        frame.extend_from_slice(&(opus.len() as u32).to_be_bytes());
        frame.extend_from_slice(opus);
        self.sink
            .send(WsMessage::Binary(frame.into()))
            .await
            .map_err(|e| VoiceError::Transport(format!("音频发送失败: {}", e)))?;
        Ok(())
    }

    async fn send_audio_raw(&mut self, opus: &[u8]) -> Result<()> {
        self.sink
            .send(WsMessage::Binary(opus.to_vec().into()))
            .await
            .map_err(|e| VoiceError::Transport(format!("音频发送失败: {}", e)))?;
        Ok(())
    }

    async fn send_ping(&mut self) -> Result<()> {
        self.sink
            .send(WsMessage::Ping(b"xz".to_vec().into()))
            .await
            .map_err(|e| VoiceError::Transport(format!("Ping 失败: {}", e)))?;
        Ok(())
    }
}

/// WebSocket 接收端：独占 SplitStream。
struct WsReceiver {
    stream: futures_util::stream::SplitStream<WsStream>,
}

enum ReceivedMessage {
    Json(ServerMessage),
    Audio(Vec<u8>),
}

impl WsReceiver {
    async fn receive(&mut self) -> Result<ReceivedMessage> {
        loop {
            match self.stream.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    let msg: ServerMessage = serde_json::from_str(&text)?;
                    return Ok(ReceivedMessage::Json(msg));
                }
                Some(Ok(WsMessage::Binary(data))) => {
                    return Ok(ReceivedMessage::Audio(data.to_vec()));
                }
                Some(Ok(WsMessage::Ping(_))) | Some(Ok(WsMessage::Pong(_))) => continue,
                Some(Ok(WsMessage::Close(f))) => {
                    return Err(VoiceError::Transport(format!("连接已关闭: {:?}", f)));
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(VoiceError::Transport(format!("WebSocket 错误: {}", e))),
                None => return Err(VoiceError::Transport("连接已断开".into())),
            }
        }
    }
}

/// 发送循环：控制 / 音频 / 心跳三路 select。
async fn send_loop(
    mut sender: WsSender,
    mut control_rx: mpsc::Receiver<ClientMessage>,
    audio_slot: super::LatestSlot<Vec<u8>>,
    binary_version: u16,
    session_start: Instant,
) {
    let mut ping = interval(PING_INTERVAL);
    // 首次 tick 立即触发，但心跳应延后；重置为错过首 tick。
    ping.reset();

    let mut sent: u64 = 0;
    let mut diag = Instant::now();
    loop {
        tokio::select! {
            biased;
            // 控制消息优先，不静默丢失。
            ctrl = control_rx.recv() => {
                let Some(msg) = ctrl else { break };
                if let Err(e) = sender.send_json(&msg).await {
                    warn!("WS 控制发送失败: {}", e);
                    break;
                }
            }
            // 音频 latest-slot：取最新帧，过期帧已丢弃。
            opus = audio_slot.take() => {
                let res = if binary_version == 2 {
                    let ts = session_start.elapsed().as_millis() as u32;
                    sender.send_audio_v2(&opus, ts).await
                } else {
                    sender.send_audio_raw(&opus).await
                };
                if let Err(e) = res {
                    warn!("WS 音频发送失败: {}", e);
                    break;
                }
                sent += 1;
            }
            _ = ping.tick() => {
                if let Err(e) = sender.send_ping().await {
                    warn!("WS 心跳失败: {}", e);
                    break;
                }
            }
        }
        // 每 2s 输出上行音频发送诊断。
        if diag.elapsed() >= Duration::from_secs(2) {
            info!("WS 上行诊断: 音频帧已发送 {}", sent);
            sent = 0;
            diag = Instant::now();
        }
    }
}

/// 接收循环：文本→JSON，二进制→音频（v2 剥头 / v1 原始），断开→Closed。
async fn recv_loop(
    mut receiver: WsReceiver,
    incoming_tx: mpsc::Sender<IncomingEvent>,
    mut close_rx: mpsc::Receiver<()>,
) {
    loop {
        tokio::select! {
            biased;
            _ = close_rx.recv() => break,
            msg = receiver.receive() => {
                match msg {
                    Ok(ReceivedMessage::Json(srv)) => {
                        if incoming_tx.send(IncomingEvent::Json(srv)).await.is_err() {
                            break;
                        }
                    }
                    Ok(ReceivedMessage::Audio(data)) => {
                        let payload = strip_binary_header(&data);
                        if incoming_tx.send(IncomingEvent::Audio(payload)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!("WS 接收错误: {}", e);
                        let _ = incoming_tx.send(IncomingEvent::Closed).await;
                        break;
                    }
                }
            }
        }
    }
}

/// 剥离二进制帧头：v2（16 字节大端头，version==2 且 payload_size 匹配）则剥头；
/// 否则视为 v1 原始 Opus。
fn strip_binary_header(data: &[u8]) -> Vec<u8> {
    if data.len() >= V2_HEADER_SIZE {
        let version = u16::from_be_bytes([data[0], data[1]]);
        let payload_size = u32::from_be_bytes([
            data[12], data[13], data[14], data[15],
        ]) as usize;
        if version == 2 && V2_HEADER_SIZE + payload_size == data.len() {
            return data[V2_HEADER_SIZE..].to_vec();
        }
    }
    data.to_vec()
}


