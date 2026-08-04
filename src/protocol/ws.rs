//! WebSocket 传输：固定 BinaryProtocol2（16 字节头，时间戳供服务端 AEC）。
//! hello `version` 声明二进制协议版本（2）。端序大端（与官方 ESP32 一致）。

use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;
use tokio_tungstenite::{connect_async_tls_with_config, MaybeTlsStream, WebSocketStream};

use crate::error::{Result, VoiceError};
use crate::protocol::message::{AudioParams, ClientMessage, ServerMessage};

use super::{ConnectParams, IncomingEvent, TransportHandles, CONTROL_CHANNEL_CAP, INCOMING_CHANNEL_CAP};

const V2_HEADER_SIZE: usize = 16;
const V3_HEADER_SIZE: usize = 4;
const V2_TYPE_OPUS: u16 = 0;
const PING_INTERVAL: Duration = Duration::from_secs(15);

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// WebSocket 传输（固定二进制协议 v2）。
pub struct WsTransport;

impl WsTransport {
    pub async fn connect(self, params: &ConnectParams) -> Result<TransportHandles> {
        let (sender, receiver, session_id, server_audio) = connect_and_handshake(params).await?;

        let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAP);
        let (audio_tx_slot, audio_rx_slot) = super::LatestSlot::<Vec<u8>>::new().pipe();
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_CHANNEL_CAP);
        // 会话关闭信号：handles drop（Sender 失效）→ 后台任务 `changed()` 返回 Err 退出。
        let (close_tx, close_rx) = watch::channel(());

        tokio::spawn(send_loop(sender, control_rx, audio_rx_slot, close_rx.clone()));
        tokio::spawn(recv_loop(receiver, incoming_tx.clone(), close_rx));

        Ok(TransportHandles {
            session_id,
            server_audio,
            control_tx,
            audio_tx: audio_tx_slot,
            incoming_rx,
            _close: close_tx,
        })
    }
}

/// 生成 WebSocket 握手 `Sec-WebSocket-Key`（RFC 6455：16 随机字节 → Base64，24 字符）。
fn ws_sec_key() -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut out = String::with_capacity(24);
    let mut i = 0;
    while i < 16 {
        let b0 = bytes[i];
        let b1 = bytes.get(i + 1).copied().unwrap_or(0);
        let b2 = bytes.get(i + 2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if i + 1 < 16 {
            out.push(ALPHABET[(((b1 & 0x0F) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if i + 2 < 16 {
            out.push(ALPHABET[(b2 & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        i += 3;
    }
    out
}

/// 建连并完成 hello 协商，返回 (发送端, 接收端, session_id, 服务器音频参数)。
async fn connect_and_handshake(
    params: &ConnectParams,
) -> Result<(WsSender, WsReceiver, String, AudioParams)> {
    let url = &params.ws_url;
    info!("WebSocket 连接: {} (二进制协议 v2)", url);

    let uri: http::Uri = url
        .parse()
        .map_err(|e| VoiceError::Transport(format!("URI 解析失败: {}", e)))?;
    // Host 头必须由调用方提供（tungstenite 不会自动填充）。
    let host = uri
        .authority()
        .map(|a| a.as_str().to_string())
        .ok_or_else(|| VoiceError::Transport(format!("URL 缺少 host: {}", url)))?;

    // 注意：tokio-tungstenite 客户端握手要求请求头完整——
    // Host/Connection/Upgrade/Sec-WebSocket-Version/Sec-WebSocket-Key 必须由调用方设置，
    // 缺失会报 "Missing, duplicated or incorrect header sec-websocket-key"。
    let request = http::Request::builder()
        .uri(uri)
        .header("Host", &host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", ws_sec_key())
        .header("Authorization", format!("Bearer {}", params.token))
        .header("Protocol-Version", "2")
        .header("Device-Id", &params.device_id)
        .header("Client-Id", &params.client_id)
        .body(())
        .map_err(|e| VoiceError::Transport(format!("构建请求失败: {}", e)))?;

    // disable_nagle=true：禁用 TCP Nagle 算法，降低小音频包的发送延迟（与 MQTT 侧一致）。
    let (ws_stream, _) = connect_async_tls_with_config(request, None, true, None)
        .await
        .map_err(|e| VoiceError::Transport(format!("WebSocket 连接失败: {}", e)))?;

    let (sink, stream) = ws_stream.split();
    let mut sender = WsSender { sink };
    let mut receiver = WsReceiver { stream };

    sender.send_json(&ClientMessage::hello_websocket_v2()).await?;

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

/// WebSocket 发送端：独占 SplitSink，按 BinaryProtocol2 构造音频帧。
struct WsSender {
    sink: futures_util::stream::SplitSink<WsStream, WsMessage>,
}

impl WsSender {
    /// 发送音频帧（v2 时间戳为 Unix 毫秒，与 MQTT 一致，供服务器端 AEC 对齐时钟）。
    async fn send_audio(&mut self, opus: &[u8]) -> Result<()> {
        self.send_audio_v2(opus, super::now_ms()).await
    }

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
        // BinaryProtocol2 为网络字节序（大端），version/type/payload_size 均 htons/htonl。
        let mut frame = Vec::with_capacity(V2_HEADER_SIZE + opus.len());
        frame.extend_from_slice(&2u16.to_be_bytes());
        frame.extend_from_slice(&V2_TYPE_OPUS.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&timestamp_ms.to_be_bytes());
        frame.extend_from_slice(&(opus.len() as u32).to_be_bytes());
        frame.extend_from_slice(opus);
        self.sink
            .send(WsMessage::Binary(frame.into()))
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
    mut close_rx: watch::Receiver<()>,
) {
    let mut ping = interval(PING_INTERVAL);
    // 重置为错过首 tick，使首个心跳延后一个周期。
    ping.reset();

    let mut sent: u64 = 0;
    let mut diag = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = close_rx.changed() => break,
            ctrl = control_rx.recv() => {
                let Some(msg) = ctrl else { break };
                if let Err(e) = sender.send_json(&msg).await {
                    warn!("WS 控制发送失败: {}", e);
                    break;
                }
            }
            opus = audio_slot.take() => {
                if let Err(e) = sender.send_audio(&opus).await {
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
        if diag.elapsed() >= Duration::from_secs(2) {
            debug!("WS 上行诊断: 音频帧已发送 {}", sent);
            sent = 0;
            diag = Instant::now();
        }
    }
}

/// 接收循环：文本→JSON，二进制→音频（剥 v2 头），断开→Closed。
async fn recv_loop(
    mut receiver: WsReceiver,
    incoming_tx: mpsc::Sender<IncomingEvent>,
    mut close_rx: watch::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = close_rx.changed() => break,
            msg = receiver.receive() => match msg {
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
            },
        }
    }
}

/// 剥离 BinaryProtocol2 帧头：头字段校验匹配则剥头，否则原样返回（裸 Opus 兜底）。
fn strip_binary_header(data: &[u8]) -> Vec<u8> {
    // 先按 v2（16B 头）识别。实测服务器下行头 version 字段为 0（非 2），
    // 故仅校验 type=OPUS 与精确长度；裸 Opus 帧前 2 字节为 TOC（非 0），误判概率极低。
    if data.len() >= V2_HEADER_SIZE {
        let ty = u16::from_be_bytes([data[2], data[3]]);
        let payload_size = u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
        if ty == V2_TYPE_OPUS && V2_HEADER_SIZE + payload_size == data.len() {
            return data[V2_HEADER_SIZE..].to_vec();
        }
    }
    // 再按 v3（4B 头）识别：type=0, reserved=0, payload_size（大端）。
    // reserved 必须为 0，避免把裸 Opus 帧（首字节恰为 0 的 SILK 8k 帧）误剥 4 字节。
    if data.len() >= V3_HEADER_SIZE {
        let ty = data[0];
        let payload_size = u16::from_be_bytes([data[2], data[3]]) as usize;
        if ty as u16 == V2_TYPE_OPUS
            && data[1] == 0
            && V3_HEADER_SIZE + payload_size == data.len()
        {
            return data[V3_HEADER_SIZE..].to_vec();
        }
    }
    // 均未识别：按裸 Opus 处理，记录前 16 字节便于核对服务器实际格式。
    if data.len() >= V3_HEADER_SIZE {
        let hex: String = data.iter().take(16).map(|b| format!("{:02x}", b)).collect();
        debug!("WS 下行帧头未识别，按裸 Opus 处理 (len={}): {}", data.len(), hex);
    }
    data.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_v2_is_unchanged() {
        // 服务器下行 v2 头：version=0（实测值）, type=0(OPUS), reserved, timestamp, payload_size。
        let payload = b"v2-opus";
        let mut frame = Vec::with_capacity(V2_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&12345u32.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        let out = strip_binary_header(&frame);
        assert_eq!(out, payload);
    }

    #[test]
    fn strip_v2_falls_back_to_raw_when_header_mismatch() {
        // 头声明长度与实际不符 → 视为裸 Opus 原样返回。
        let payload = b"abc";
        let mut frame = Vec::with_capacity(V2_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&2u16.to_be_bytes());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&u32::MAX.to_be_bytes()); // payload_size 与实际不符
        frame.extend_from_slice(payload);
        let out = strip_binary_header(&frame);
        assert_eq!(out, frame);
    }

    /// 握手 key 必须为合法 Base64 且长度 24（RFC 6455：16 字节随机值）。
    #[test]
    fn ws_sec_key_is_valid_base64() {
        let key = ws_sec_key();
        assert_eq!(key.len(), 24, "key 长度异常: {}", key);
        assert!(key.ends_with("=="), "末组应补 2 个 =: {}", key);
        assert!(
            key.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'='),
            "含非法 Base64 字符: {}",
            key
        );
    }
}


