//! 传输适配器闭集 + 统一收发句柄。
//!
//! 设计约束（来自重构方案）：不使用 `async_trait`、不使用 `dyn` 动态分派、
//! 不引入公开泛型。以 `TransportAdapter` 枚举静态分派，连接成功后返回统一的
//! [`TransportHandles`]（mpsc 通道），监督循环与具体传输彻底解耦。
//!
//! 音频上行采用 latest-slot 语义：发送任务只取最新帧，过期帧直接丢弃，
//! 避免网络发送落后导致延迟线性增长。控制消息使用有界 mpsc，不静默丢失。

pub mod message;
pub mod mqtt_udp;
pub mod scripted;
pub mod ws;

use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, Notify};

use crate::error::Result;
use message::{AudioParams, ClientMessage, ServerMessage};

/// 连接所需参数（由 OTA 配置 + 设备身份推导）。
#[derive(Clone, Debug)]
pub struct ConnectParams {
    pub device_id: String,
    pub client_id: String,
    pub token: String,
    /// WebSocket URL（回退链路）。
    pub ws_url: String,
    /// MQTT 配置（主链路；None 表示 OTA 未下发 MQTT，仅可用 WebSocket）。
    pub mqtt: Option<MqttParams>,
}

/// MQTT 连接参数。
#[derive(Clone, Debug)]
pub struct MqttParams {
    pub host: String,
    pub port: u16,
    /// true = TLS(8883)，false = 明文 TCP(1883)。
    pub tls: bool,
    pub username: String,
    pub password: String,
    /// MQTT ClientId（与设备 Client-Id 区分）。
    pub mqtt_client_id: String,
    pub publish_topic: String,
    pub subscribe_topic: String,
}

/// 入站事件。
#[derive(Debug)]
pub enum IncomingEvent {
    Json(ServerMessage),
    /// 已解密的 Opus 音频载荷（UDP）或原始二进制音频（WebSocket）。
    Audio(Vec<u8>),
    /// 传输层已断开。
    Closed,
}

/// 连接成功后的统一句柄。监督循环仅依赖此结构，与具体传输无关。
pub struct TransportHandles {
    pub session_id: String,
    pub server_audio: AudioParams,
    /// 控制消息发送（容量 16，满则背压；发送任务保证不静默丢失）。
    pub control_tx: mpsc::Sender<ClientMessage>,
    /// 音频发送（latest-slot：发送任务取最新，过期帧丢弃）。
    pub audio_tx: LatestSlot<Vec<u8>>,
    /// 入站事件流。
    pub incoming_rx: mpsc::Receiver<IncomingEvent>,
    /// 传输关闭信号（用于通知监督循环停止向已断开的传输推送）。
    pub close_tx: mpsc::Sender<()>,
}

/// 传输适配器闭集：枚举即静态分派，无 dyn / 无 async_trait。
pub enum TransportAdapter {
    WebSocket(ws::WsTransport),
    MqttUdp(mqtt_udp::MqttUdpTransport),
    Scripted(scripted::ScriptedTransport),
}

impl TransportAdapter {
    /// 建连并完成 hello 协商，返回统一句柄。
    pub async fn connect(self, params: &ConnectParams) -> Result<TransportHandles> {
        match self {
            TransportAdapter::WebSocket(t) => t.connect(params).await,
            TransportAdapter::MqttUdp(t) => t.connect(params).await,
            TransportAdapter::Scripted(t) => t.connect(params).await,
        }
    }
}

/// latest-slot 通道：发送方覆写最新值，接收方取走后置空。
/// 多次发送之间只保留最后一份，天然实现"过期帧丢弃"。
pub struct LatestSlot<T> {
    inner: Arc<Mutex<Option<T>>>,
    notify: Arc<Notify>,
}

impl<T> Clone for LatestSlot<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            notify: self.notify.clone(),
        }
    }
}

impl<T> LatestSlot<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// 覆写最新值并唤醒接收方。
    pub async fn store(&self, value: T) {
        *self.inner.lock().await = Some(value);
        self.notify.notify_one();
    }

    /// 等待并取走最新值（置空）。若已被取走则等待下一次 store。
    pub async fn take(&self) -> T {
        loop {
            if let Some(v) = self.inner.lock().await.take() {
                return v;
            }
            self.notify.notified().await;
        }
    }

    /// 非阻塞尝试取走。
    pub async fn try_take(&self) -> Option<T> {
        self.inner.lock().await.take()
    }
}

impl<T> Default for LatestSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> LatestSlot<T> {
    /// 成对产生发送端与接收端（共享同一底层槽）。
    pub fn pipe(self) -> (LatestSlot<T>, LatestSlot<T>) {
        let tx = self.clone();
        (tx, self)
    }
}

/// 当前毫秒时间戳（u32，自 Unix epoch，约 49 天回绕）。服务端 AEC 用。
pub(crate) fn now_ms() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
        & 0xFFFF_FFFF) as u32
}
