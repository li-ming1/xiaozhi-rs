//! WebSocket 协议实现
//!
//! 支持 JSON 文本消息和二进制音频帧

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::protocol::Message as WsMessage,
    Connector, MaybeTlsStream, WebSocketStream,
};

use crate::message::{Message, ServerMessage};

/// WebSocket 协议客户端
pub struct WebSocketProtocol {
    url: String,
    token: String,
    device_id: String,
    client_id: String,
    ws: Option<Arc<Mutex<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>>,
    session_id: Arc<Mutex<String>>,
}

/// 接收到的消息类型
#[derive(Debug)]
pub enum ReceivedMessage {
    Json(ServerMessage),
    Audio(Vec<u8>),
}

impl WebSocketProtocol {
    pub fn new(url: String, token: String, device_id: String, client_id: String) -> Self {
        Self {
            url,
            token,
            device_id,
            client_id,
            ws: None,
            session_id: Arc::new(Mutex::new(String::new())),
        }
    }

    /// 连接 WebSocket
    pub async fn connect(&mut self) -> Result<()> {
        info!("正在连接: {}", self.url);

        // 创建 TLS 连接器（跳过证书验证）
        let connector = Some(Connector::NativeTls(
            native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()?,
        ));

        // 构建带 Headers 的 HTTP Request
        let uri: http::Uri = self.url.parse()
            .map_err(|e| anyhow!("URI 解析失败: {}", e))?;

        let request = http::Request::builder()
            .uri(uri)
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Protocol-Version", "1")
            .header("Device-Id", &self.device_id)
            .header("Client-Id", &self.client_id)
            .header("Host", "api.tenclass.net")
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Version", "13")
            .header("Sec-WebSocket-Key", tungstenite::handshake::client::generate_key())
            .body(())
            .map_err(|e| anyhow!("构建请求失败: {}", e))?;

        // 连接
        let (ws_stream, _) = connect_async_tls_with_config(request, None, false, connector)
            .await
            .map_err(|e| anyhow!("WebSocket 连接失败: {}", e))?;

        self.ws = Some(Arc::new(Mutex::new(ws_stream)));
        info!("WebSocket 连接成功");
        Ok(())
    }

    /// 发送 hello 握手
    pub async fn send_hello(&mut self) -> Result<()> {
        let hello = Message::hello();
        self.send_json(&hello).await?;

        // 等待服务器响应
        let response = self.receive_json().await?;
        match response {
            ServerMessage::Hello { session_id, .. } => {
                *self.session_id.lock().await = session_id.clone();
                info!("握手成功，session_id: {}", session_id);
                Ok(())
            }
            _ => Err(anyhow!("期望 hello 响应，收到: {:?}", response)),
        }
    }

    /// 获取 session_id
    pub async fn session_id(&self) -> String {
        self.session_id.lock().await.clone()
    }

    /// 发送 JSON 消息
    pub async fn send_json(&self, msg: &Message) -> Result<()> {
        let json = serde_json::to_string(msg)?;
        debug!("发送 JSON: {}", json);

        if let Some(ws) = &self.ws {
            ws.lock().await.send(WsMessage::Text(json)).await?;
        }

        Ok(())
    }

    /// 发送二进制音频数据
    pub async fn send_audio(&self, data: &[u8]) -> Result<()> {
        if let Some(ws) = &self.ws {
            ws.lock().await.send(WsMessage::Binary(data.to_vec())).await?;
            debug!("发送音频: {} 字节", data.len());
        }
        Ok(())
    }

    /// 接收消息（JSON 或音频）
    pub async fn receive(&self) -> Result<ReceivedMessage> {
        if let Some(ws) = &self.ws {
            match ws.lock().await.next().await {
                Some(Ok(WsMessage::Text(text))) => {
                    debug!("接收 JSON: {}", text);
                    let msg: ServerMessage = serde_json::from_str(&text)?;
                    Ok(ReceivedMessage::Json(msg))
                }
                Some(Ok(WsMessage::Binary(data))) => {
                    debug!("接收音频: {} 字节", data.len());
                    Ok(ReceivedMessage::Audio(data))
                }
                Some(Ok(WsMessage::Close(_))) => {
                    info!("服务器关闭连接");
                    Err(anyhow!("连接已关闭"))
                }
                Some(Ok(msg)) => {
                    warn!("收到非文本/二进制消息: {:?}", msg);
                    Err(anyhow!("未知消息类型"))
                }
                Some(Err(e)) => {
                    Err(anyhow!("WebSocket错误: {}", e))
                }
                None => {
                    Err(anyhow!("连接已断开"))
                }
            }
        } else {
            Err(anyhow!("未连接"))
        }
    }

    /// 尝试接收消息（非阻塞）
    #[allow(dead_code)]
    pub fn try_receive(&self) -> Result<Option<ReceivedMessage>> {
        // 注意：这个实现需要同步访问 WebSocket，这里简化处理
        // 实际应该使用 tokio::select! 或单独的接收任务
        Ok(None)
    }

    /// 接收 JSON 消息（兼容旧接口）
    pub async fn receive_json(&self) -> Result<ServerMessage> {
        match self.receive().await? {
            ReceivedMessage::Json(msg) => Ok(msg),
            ReceivedMessage::Audio(_) => Err(anyhow!("期望 JSON，收到音频")),
        }
    }

    /// 关闭连接
    #[allow(dead_code)]
    pub async fn close(&mut self) -> Result<()> {
        if let Some(ws) = self.ws.take() {
            ws.lock().await.close(None).await?;
            info!("WebSocket 已关闭");
        }
        Ok(())
    }

    /// 检查是否已连接
    #[allow(dead_code)]
    pub fn is_connected(&self) -> bool {
        self.ws.is_some()
    }
}