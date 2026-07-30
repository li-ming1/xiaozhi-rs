//! WebSocket 协议实现（收发分离，无锁互不阻塞）
//!
//! 将 WebSocketStream split 为独立的发送端与接收端，
//! 消除旧 Arc<Mutex<Stream>> 架构中 receive 持锁跨 await 阻塞 send 的互锁问题。

use anyhow::{anyhow, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use serde::Serialize;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::protocol::Message as WsMessage,
    Connector, MaybeTlsStream, WebSocketStream,
};

use crate::message::{Message, ServerMessage};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 接收到的消息类型
#[derive(Debug)]
pub enum ReceivedMessage {
    Json(ServerMessage),
    Audio(Vec<u8>),
}

/// WebSocket 发送端（持有 SplitSink，与接收端独立，无锁）
pub struct WsSender {
    sink: SplitSink<WsStream, WsMessage>,
}

impl WsSender {
    pub async fn send_json(&mut self, msg: &impl Serialize) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        debug!("发送 JSON: {}", json);
        self.sink
            .send(WsMessage::Text(json))
            .await
            .map_err(|e| anyhow!("发送失败: {}", e))?;
        Ok(())
    }

    pub async fn send_audio(&mut self, data: &[u8]) -> Result<()> {
        self.sink
            .send(WsMessage::Binary(data.to_vec()))
            .await
            .map_err(|e| anyhow!("音频发送失败: {}", e))?;
        debug!("发送音频: {} 字节", data.len());
        Ok(())
    }
}

/// WebSocket 接收端（持有 SplitStream，与发送端独立，无锁）
pub struct WsReceiver {
    stream: SplitStream<WsStream>,
}

impl WsReceiver {
    pub async fn receive(&mut self) -> Result<ReceivedMessage> {
        match self.stream.next().await {
            Some(Ok(WsMessage::Text(text))) => {
                debug!("接收 JSON: {}", text);
                let msg: ServerMessage = serde_json::from_str(&text)?;
                Ok(ReceivedMessage::Json(msg))
            }
            Some(Ok(WsMessage::Binary(data))) => {
                debug!("接收音频: {} 字节", data.len());
                Ok(ReceivedMessage::Audio(data))
            }
            Some(Ok(WsMessage::Close(close_frame))) => {
                info!("服务器关闭连接: {:?}", close_frame);
                Err(anyhow!("连接已关闭: {:?}", close_frame))
            }
            Some(Ok(msg)) => {
                warn!("收到非文本/二进制消息: {:?}", msg);
                Err(anyhow!("未知消息类型"))
            }
            Some(Err(e)) => Err(anyhow!("WebSocket错误: {}", e)),
            None => Err(anyhow!("连接已断开")),
        }
    }
}

/// 连接 WebSocket 并完成握手
///
/// 返回 (发送端, 接收端, session_id)。
/// 收发分离后，receive 等待消息时不再阻塞 send，彻底消除收发互锁。
pub async fn connect_and_handshake(
    url: &str,
    token: &str,
    device_id: &str,
    client_id: &str,
) -> Result<(WsSender, WsReceiver, String)> {
    info!("正在连接: {}", url);

    let connector = Some(Connector::NativeTls(
        native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()?,
    ));

    let uri: http::Uri = url
        .parse()
        .map_err(|e| anyhow!("URI 解析失败: {}", e))?;
    // Host 从 URL 解析，避免硬编码
    let host = uri
        .host()
        .ok_or_else(|| anyhow!("URL 缺少 host: {}", url))?
        .to_string();

    let request = http::Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {}", token))
        .header("Protocol-Version", "1")
        .header("Device-Id", device_id)
        .header("Client-Id", client_id)
        .header("Host", host.as_str())
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .map_err(|e| anyhow!("构建请求失败: {}", e))?;

    let (ws_stream, _) = connect_async_tls_with_config(request, None, false, connector)
        .await
        .map_err(|e| anyhow!("WebSocket 连接失败: {}", e))?;

    // split：收发分离，消除互锁
    let (sink, stream) = ws_stream.split();
    let mut sender = WsSender { sink };
    let mut receiver = WsReceiver { stream };

    info!("WebSocket 连接成功");

    // 握手
    let hello = Message::hello();
    info!("发送 hello: {}", serde_json::to_string(&hello)?);
    sender.send_json(&hello).await?;

    info!("等待服务器 hello 响应...");
    match receiver.receive().await? {
        ReceivedMessage::Json(ServerMessage::Hello { session_id, .. }) => {
            info!("握手成功，session_id: {}", session_id);
            Ok((sender, receiver, session_id))
        }
        ReceivedMessage::Json(msg) => {
            warn!("期望 hello 响应，收到: {:?}", msg);
            Err(anyhow!("期望 hello 响应，收到: {:?}", msg))
        }
        ReceivedMessage::Audio(data) => {
            warn!("期望 JSON，收到音频数据: {} 字节", data.len());
            Err(anyhow!("期望 JSON，收到音频"))
        }
    }
}
