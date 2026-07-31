//! WebSocket 协议：split 收发双端，消除持锁跨 await 的收发互锁。

use anyhow::{anyhow, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use serde::Serialize;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{protocol::Message as WsMessage, Bytes},
    MaybeTlsStream, WebSocketStream,
};

use crate::message::{Message, ServerMessage};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// 业务层可消费的消息。Audio 持有引用计数的 Bytes，解码时 deref 零拷贝。
#[derive(Debug)]
pub enum ReceivedMessage {
    Json(ServerMessage),
    Audio(Bytes),
}

/// 发送端：独占 SplitSink，与接收端无共享状态。
pub struct WsSender {
    sink: SplitSink<WsStream, WsMessage>,
}

impl WsSender {
    pub async fn send_json(&mut self, msg: &impl Serialize) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        debug!("发送 JSON: {}", json);
        self.sink
            .send(WsMessage::Text(json.into()))
            .await
            .map_err(|e| anyhow!("发送失败: {}", e))?;
        Ok(())
    }

    pub async fn send_audio(&mut self, data: Vec<u8>) -> Result<()> {
        // owned Vec 直接 move，省一次 to_vec；音频帧高频且字节数可预测，故不打日志。
        self.sink
            .send(WsMessage::Binary(data.into()))
            .await
            .map_err(|e| anyhow!("音频发送失败: {}", e))?;
        Ok(())
    }

    /// 心跳 Ping：兼触发 Sink flush，排空底层自动排队的 Pong。
    pub async fn send_ping(&mut self, payload: Vec<u8>) -> Result<()> {
        debug!("发送 Ping: {} 字节", payload.len());
        self.sink
            .send(WsMessage::Ping(payload.into()))
            .await
            .map_err(|e| anyhow!("Ping 发送失败: {}", e))?;
        Ok(())
    }
}

/// 接收端：独占 SplitStream，与发送端无共享状态。
pub struct WsReceiver {
    stream: SplitStream<WsStream>,
}

impl WsReceiver {
    pub async fn receive(&mut self) -> Result<ReceivedMessage> {
        // 控制帧由底层自动处理（收到 Ping 自动排队 Pong，随下次 flush 发出），应用层忽略并续等。
        loop {
            match self.stream.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    debug!("接收 JSON: {}", text);
                    let msg: ServerMessage = serde_json::from_str(&text)?;
                    return Ok(ReceivedMessage::Json(msg));
                }
                Some(Ok(WsMessage::Binary(data))) => {
                    return Ok(ReceivedMessage::Audio(data));
                }
                Some(Ok(WsMessage::Ping(payload))) => {
                    debug!("收到 Ping: {} 字节（已自动排队 Pong）", payload.len());
                }
                Some(Ok(WsMessage::Pong(payload))) => {
                    debug!("收到 Pong: {} 字节", payload.len());
                }
                Some(Ok(WsMessage::Close(close_frame))) => {
                    info!("服务器关闭连接: {:?}", close_frame);
                    return Err(anyhow!("连接已关闭: {:?}", close_frame));
                }
                Some(Ok(msg)) => {
                    warn!("收到未处理的消息类型: {:?}", msg);
                }
                Some(Err(e)) => return Err(anyhow!("WebSocket错误: {}", e)),
                None => return Err(anyhow!("连接已断开")),
            }
        }
    }
}

/// 建连并完成握手，返回 (发送端, 接收端, session_id)；收发分离后互不阻塞。
pub async fn connect_and_handshake(
    url: &str,
    token: &str,
    device_id: &str,
    client_id: &str,
) -> Result<(WsSender, WsReceiver, String)> {
    info!("正在连接: {}", url);

    // connector 传 None 即启用 webpki-roots，免 native-tls/OpenSSL 依赖。
    let uri: http::Uri = url
        .parse()
        .map_err(|e| anyhow!("URI 解析失败: {}", e))?;
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

    let (ws_stream, _) = connect_async_tls_with_config(request, None, false, None)
        .await
        .map_err(|e| anyhow!("WebSocket 连接失败: {}", e))?;

    // split 收发双端，消除互锁。
    let (sink, stream) = ws_stream.split();
    let mut sender = WsSender { sink };
    let mut receiver = WsReceiver { stream };

    info!("WebSocket 连接成功");

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
