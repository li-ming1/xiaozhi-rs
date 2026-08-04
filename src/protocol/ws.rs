//! WebSocket 传输：默认 v2 二进制协议（带时间戳供服务端 AEC），可选 v3 精简头。
//! hello `version` 同时声明二进制协议版本（1=原始 / 2=BinaryProtocol2 / 3=BinaryProtocol3）。
//! 端序均为大端（与官方 ESP32 一致）。

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

/// WebSocket 传输。
pub struct WsTransport {
    /// 二进制协议版本：2 = BinaryProtocol2（时间戳，AEC），3 = BinaryProtocol3（精简头），1 = 原始 Opus。
    pub binary_version: u16,
}

impl Default for WsTransport {
    fn default() -> Self {
        // 默认 v2（带 AEC 时间戳，已联调验证）；可用环境变量 XZ_WS_VERSION=1/2/3 切换。
        let v = std::env::var("XZ_WS_VERSION")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .filter(|v| matches!(v, 1..=3))
            .unwrap_or(2);
        Self { binary_version: v }
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
        // 会话关闭信号：handles drop（Sender 失效）→ 后台任务 `changed()` 返回 Err 退出。
        let (close_tx, close_rx) = watch::channel(());

        let session_start = Instant::now();
        tokio::spawn(send_loop(
            sender,
            control_rx,
            audio_rx_slot,
            binary_version,
            session_start,
            close_rx.clone(),
        ));
        tokio::spawn(recv_loop(receiver, incoming_tx.clone(), binary_version, close_rx));

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
    uri.host()
        .ok_or_else(|| VoiceError::Transport(format!("URL 缺少 host: {}", url)))?;

    // 握手必需头（Host/Connection/Upgrade/Sec-WebSocket-*）由 tokio-tungstenite 自动填充，
    // 这里只附加业务头。
    let request = http::Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {}", params.token))
        .header("Protocol-Version", binary_version.to_string())
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

    let hello = match binary_version {
        1 => ClientMessage::hello_websocket_v1(),
        2 => ClientMessage::hello_websocket_v2(),
        3 => ClientMessage::hello_websocket_v3(),
        v => {
            return Err(VoiceError::Protocol(format!(
                "不支持的二进制协议版本: {}",
                v
            )))
        }
    };
    sender.send_json(&hello).await?;

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

    async fn send_audio_v3(&mut self, opus: &[u8]) -> Result<()> {
        // BinaryProtocol3：type=0、reserved=0、payload_size=htons(大端)。
        let mut frame = Vec::with_capacity(V3_HEADER_SIZE + opus.len());
        frame.push(0u8);
        frame.push(0u8);
        frame.extend_from_slice(&(opus.len() as u16).to_be_bytes());
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
                let res = match binary_version {
                    2 => {
                        let ts = session_start.elapsed().as_millis() as u32;
                        sender.send_audio_v2(&opus, ts).await
                    }
                    3 => sender.send_audio_v3(&opus).await,
                    _ => sender.send_audio_raw(&opus).await,
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
        if diag.elapsed() >= Duration::from_secs(2) {
            info!("WS 上行诊断: 音频帧已发送 {}", sent);
            sent = 0;
            diag = Instant::now();
        }
    }
}

/// 接收循环：文本→JSON，二进制→音频（按版本剥头），断开→Closed。
async fn recv_loop(
    mut receiver: WsReceiver,
    incoming_tx: mpsc::Sender<IncomingEvent>,
    binary_version: u16,
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
                    let payload = strip_binary_header(&data, binary_version);
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

/// 剥离二进制帧头：头字段校验匹配则剥头，否则原样返回（裸 Opus）。
fn strip_binary_header(data: &[u8], binary_version: u16) -> Vec<u8> {
    match binary_version {
        2 => {
            if data.len() >= V2_HEADER_SIZE {
                let version = u16::from_be_bytes([data[0], data[1]]);
                let ty = u16::from_be_bytes([data[2], data[3]]);
                let payload_size =
                    u32::from_be_bytes([data[12], data[13], data[14], data[15]]) as usize;
                if version == 2 && ty == V2_TYPE_OPUS && V2_HEADER_SIZE + payload_size == data.len()
                {
                    return data[V2_HEADER_SIZE..].to_vec();
                }
            }
            data.to_vec()
        }
        3 => {
            if data.len() >= V3_HEADER_SIZE {
                let ty = data[0];
                let payload_size = u16::from_be_bytes([data[2], data[3]]) as usize;
                if ty == 0 && V3_HEADER_SIZE + payload_size == data.len() {
                    return data[V3_HEADER_SIZE..].to_vec();
                }
            }
            data.to_vec()
        }
        _ => data.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_v3_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(V3_HEADER_SIZE + payload.len());
        frame.push(0u8);
        frame.push(0u8);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn v3_header_is_4_bytes_with_big_endian_size() {
        let payload = b"opus-data";
        let frame = build_v3_frame(payload);
        assert_eq!(frame.len(), V3_HEADER_SIZE + payload.len());
        assert_eq!(frame[0], 0);
        assert_eq!(frame[1], 0);
        assert_eq!(
            u16::from_be_bytes([frame[2], frame[3]]),
            payload.len() as u16
        );
    }

    #[test]
    fn strip_v3_recovers_payload() {
        let payload = b"decoded-opus-frame";
        let frame = build_v3_frame(payload);
        let out = strip_binary_header(&frame, 3);
        assert_eq!(out, payload);
    }

    #[test]
    fn strip_v3_falls_back_to_raw_when_header_mismatch() {
        // 头声明长度与实际不符 → 视为裸 Opus 原样返回。
        let mut bad = build_v3_frame(b"abc");
        bad[2] = 0xFF;
        bad[3] = 0xFF;
        let out = strip_binary_header(&bad, 3);
        assert_eq!(out, bad);
    }

    #[test]
    fn strip_v2_is_unchanged() {
        // v2 头：version=2(大端), type=0, reserved, timestamp, payload_size。
        let payload = b"v2-opus";
        let mut frame = Vec::with_capacity(V2_HEADER_SIZE + payload.len());
        frame.extend_from_slice(&2u16.to_be_bytes());
        frame.extend_from_slice(&0u16.to_be_bytes());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&12345u32.to_be_bytes());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(payload);
        let out = strip_binary_header(&frame, 2);
        assert_eq!(out, payload);
    }

    #[test]
    fn strip_v1_returns_raw() {
        let raw = b"raw-opus";
        assert_eq!(strip_binary_header(raw, 1), raw);
        assert_eq!(strip_binary_header(build_v3_frame(b"x").as_slice(), 1), build_v3_frame(b"x"));
    }
}


