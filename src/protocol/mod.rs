//! 传输适配器闭集 + 统一收发句柄（枚举静态分派；音频 latest-slot，控制有界 mpsc）。

pub mod message;
pub mod mqtt_udp;
pub mod ws;

use std::sync::Arc;

use tokio::sync::{mpsc, watch, Mutex, Notify};

use crate::error::Result;
use message::{AudioParams, ClientMessage, ServerMessage};

pub(crate) const CONTROL_CHANNEL_CAP: usize = 16;
pub(crate) const INCOMING_CHANNEL_CAP: usize = 64;

/// 连接所需参数（由 OTA 配置 + 设备身份推导）。
#[derive(Clone, Debug)]
pub struct ConnectParams {
    pub device_id: String,
    pub client_id: String,
    pub token: String,
    pub ws_url: String,
    pub mqtt: Option<MqttParams>,
}

/// MQTT 连接参数。
#[derive(Clone, Debug)]
pub struct MqttParams {
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: String,
    pub password: String,
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
    pub control_tx: mpsc::Sender<ClientMessage>,
    pub audio_tx: LatestSlot<Vec<u8>>,
    pub incoming_rx: mpsc::Receiver<IncomingEvent>,
    /// drop 时 watch Sender 失效，后台任务经 `changed()` 返回 Err 退出，防悬挂泄漏。
    _close: watch::Sender<()>,
}

/// 传输适配器闭集：枚举即静态分派，无 dyn / 无 async_trait。
pub enum TransportAdapter {
    WebSocket(ws::WsTransport),
    MqttUdp(mqtt_udp::MqttUdpTransport),
}

impl TransportAdapter {
    /// 建连并完成 hello 协商，返回统一句柄。
    pub async fn connect(self, params: &ConnectParams) -> Result<TransportHandles> {
        match self {
            TransportAdapter::WebSocket(t) => t.connect(params).await,
            TransportAdapter::MqttUdp(t) => t.connect(params).await,
        }
    }
}

/// latest-slot 通道：发送方覆写最新值，接收方取走后置空，天然实现"过期帧丢弃"。
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

    pub async fn store(&self, value: T) {
        *self.inner.lock().await = Some(value);
        self.notify.notify_one();
    }

    pub async fn take(&self) -> T {
        loop {
            if let Some(v) = self.inner.lock().await.take() {
                return v;
            }
            self.notify.notified().await;
        }
    }

    /// 成对产生发送端与接收端（共享同一底层槽）。
    pub fn pipe(self) -> (LatestSlot<T>, LatestSlot<T>) {
        let tx = self.clone();
        (tx, self)
    }
}

impl<T> Default for LatestSlot<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// 当前毫秒时间戳（u32，自 Unix epoch，约 49 天回绕）。服务端 AEC 用。
pub(crate) fn now_ms() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u32)
        .unwrap_or(0)
}
