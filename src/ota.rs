//! OTA 配置与激活：reqwest（共享 rustls/ring，纯 Rust 跨平台）。
//!
//! 本地 IP 探测（原 8.8.8.8 UDP connect）已移除 —— 地址只从实际官方服务路由推导，
//! MQTT endpoint 由 OTA 下发或从 WebSocket URL 的 host 推导。

use anyhow::{anyhow, Context, Result};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::identity::DeviceIdentity;

const OTA_URL: &str = "https://api.tenclass.net/xiaozhi/ota/";
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// OTA 响应：连接参数与可选激活任务。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OtaConfig {
    #[serde(default)]
    pub websocket: WebSocketConfig,
    /// 可选 MQTT 配置（主链路）；缺省时回退 WebSocket-only。
    #[serde(default)]
    pub mqtt: Option<OtaMqttConfig>,
    #[serde(default)]
    pub activation: Option<ActivationData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebSocketConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

/// MQTT 配置（与官方 OTA 响应结构对应）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OtaMqttConfig {
    /// endpoint，可含端口（"host:port"）；缺省端口见 `derive_mqtt`。
    pub endpoint: Option<String>,
    /// 服务器下发的 MQTT client_id（必须使用，勿自造）。
    #[serde(default)]
    pub client_id: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub publish_topic: Option<String>,
    #[serde(default)]
    pub subscribe_topic: Option<String>,
    #[serde(default)]
    pub tls: bool,
}

/// 激活任务载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationData {
    pub challenge: String,
    pub code: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub authorization_url: Option<String>,
}

/// 全局复用 HTTP 客户端（含连接池，激活重试时避免重复 TLS 握手）。
fn client() -> Result<&'static reqwest::Client> {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    if let Some(c) = CLIENT.get() {
        return Ok(c);
    }
    // 并发时可能重复构建，但仅首个成功者被保留，其余丢弃，无副作用。
    let built = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .context("构建 HTTP 客户端失败")?;
    Ok(CLIENT.get_or_init(|| built))
}

/// 拉取 OTA 配置。
pub async fn fetch_config(identity: &DeviceIdentity) -> Result<OtaConfig> {
    info!("正在连接服务器（超时60秒）...");
    let client = client()?;

    let payload = serde_json::json!({
        "application": {
            "version": "2.1.1",
            "elf_sha256": identity.hmac_key,
        },
        "board": {
            "type": "bread-compact-wifi",
            "name": "xiaozhi-rs",
            "mac": identity.device_id,
        }
    });

    let response = client
        .post(OTA_URL)
        .header("Device-Id", &identity.device_id)
        .header("Client-Id", &identity.client_id)
        .header(
            "User-Agent",
            format!("bread-compact-wifi/xiaozhi-rs-{}", env!("CARGO_PKG_VERSION")),
        )
        .header("Accept-Language", "zh-CN")
        .json(&payload)
        .send()
        .await
        .context("OTA 请求失败")?;

    if !response.status().is_success() {
        return Err(anyhow!("OTA 请求失败: HTTP {}", response.status()));
    }

    let text = response.text().await.context("读取 OTA 响应失败")?;
    // 打印脱敏原始响应，便于核对服务器实际返回的连接字段（如 mqtt 对象结构）。
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
        let mut masked = v.clone();
        if let Some(ws) = masked
            .get_mut("websocket")
            .and_then(|w| w.as_object_mut())
            && let Some(t) = ws.get_mut("token")
        {
            *t = serde_json::Value::String("***".into());
        }
        if let Some(mq) = masked.get_mut("mqtt").and_then(|m| m.as_object_mut()) {
            for key in ["password", "pwd", "secret"] {
                if let Some(v) = mq.get_mut(key) {
                    *v = serde_json::Value::String("***".into());
                }
            }
        }
        info!("OTA 响应(脱敏): {}", masked);
    }

    let config: OtaConfig = serde_json::from_str(&text).context("OTA 响应解析失败")?;
    // 不打印完整 config：其中含 mqtt.password / websocket.token，debug 级别也不应泄密。

    if config.activation.is_some() {
        warn!("设备需要激活");
    } else {
        info!("设备已授权");
    }
    Ok(config)
}

/// 轮询激活完成（60s 超时，每 5s 一次）。
pub async fn wait_for_activation(identity: &DeviceIdentity, challenge: &str) -> Result<()> {
    let client = client()?;
    let hmac_signature = identity.generate_hmac_signature(challenge);

    let payload = serde_json::json!({
        "Payload": {
            "algorithm": "hmac-sha256",
            "serial_number": identity.serial_number,
            "challenge": challenge,
            "hmac": hmac_signature,
        }
    });

    let activate_url = format!("{}activate", OTA_URL);
    const MAX_POLLS: u32 = 12;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);

    info!("请在60秒内完成激活...");

    for attempt in 1..=MAX_POLLS {
        if attempt > 1 {
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        let response = client
            .post(&activate_url)
            .header("Device-Id", &identity.device_id)
            .header("Client-Id", &identity.client_id)
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) => match resp.status().as_u16() {
                200 => {
                    info!("激活成功！");
                    return Ok(());
                }
                202 => {
                    let remaining = (MAX_POLLS - attempt) * POLL_INTERVAL.as_secs() as u32;
                    info!("等待用户输入验证码...（剩余{}秒）", remaining);
                }
                status => warn!("服务器返回状态码: {}", status),
            },
            Err(_) => {
                let remaining = (MAX_POLLS - attempt) * POLL_INTERVAL.as_secs() as u32;
                info!(
                    "网络请求失败，{}秒后重试...（剩余{}秒）",
                    POLL_INTERVAL.as_secs(),
                    remaining
                );
            }
        }
    }

    Err(anyhow!("激活码已过期（60秒超时）"))
}
