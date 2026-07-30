//! OTA 配置拉取与激活流程

use anyhow::{anyhow, Result};
use log::{info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::identity::DeviceIdentity;

const OTA_URL: &str = "https://api.tenclass.net/xiaozhi/ota/";
const ACTIVATION_MAX_RETRIES: u32 = 60;
const ACTIVATION_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// OTA 响应配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtaConfig {
    pub websocket: WebSocketConfig,
    #[serde(default)]
    pub mqtt: Option<MqttConfig>,
    #[serde(default)]
    pub activation: Option<ActivationData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub url: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttConfig {
    pub endpoint: Option<String>,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub publish_topic: Option<String>,
    pub subscribe_topic: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationData {
    pub challenge: String,
    pub code: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub authorization_url: Option<String>,
}

/// 从 OTA 服务器获取配置
pub async fn fetch_config(identity: &DeviceIdentity) -> Result<OtaConfig> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(true)
        .build()?;

    let payload = serde_json::json!({
        "application": {
            "version": "2.1.1",
            "elf_sha256": identity.hmac_key,
        },
        "board": {
            "type": "bread-compact-wifi",
            "name": "xiaozhi-rs",
            "ip": get_local_ip()?,
            "mac": identity.device_id,
        }
    });

    let response = client
        .post(OTA_URL)
        .header("Device-Id", &identity.device_id)
        .header("Client-Id", &identity.client_id)
        .header("Content-Type", "application/json")
        .header("User-Agent", "bread-compact-wifi/xiaozhi-rs-0.1.0")
        .header("Accept-Language", "zh-CN")
        .json(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow!("OTA 请求失败: HTTP {}", response.status()));
    }

    let ota_response: serde_json::Value = response.json().await?;
    info!("OTA 响应: {}", serde_json::to_string_pretty(&ota_response)?);

    // 解析响应
    let mut config = OtaConfig {
        websocket: WebSocketConfig {
            url: None,
            token: None,
        },
        mqtt: None,
        activation: None,
    };

    // 提取 WebSocket 配置
    if let Some(ws) = ota_response.get("websocket") {
        config.websocket.url = ws.get("url").and_then(|v| v.as_str()).map(String::from);
        config.websocket.token = ws.get("token").and_then(|v| v.as_str()).map(String::from);
    }

    // 提取 MQTT 配置
    if let Some(mqtt) = ota_response.get("mqtt") {
        config.mqtt = Some(serde_json::from_value(mqtt.clone())?);
    }

    // 提取激活数据
    if let Some(activation) = ota_response.get("activation") {
        config.activation = Some(serde_json::from_value(activation.clone())?);
        warn!("设备需要激活");
    } else {
        info!("设备已授权");
    }

    Ok(config)
}

/// 等待激活完成（轮询）
pub async fn wait_for_activation(
    identity: &DeviceIdentity,
    challenge: &str,
    code: &str,
) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .danger_accept_invalid_certs(true)
        .build()?;

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

    for attempt in 1..=ACTIVATION_MAX_RETRIES {
        info!("激活尝试 {}/{}...", attempt, ACTIVATION_MAX_RETRIES);

        let response = client
            .post(&activate_url)
            .header("Device-Id", &identity.device_id)
            .header("Client-Id", &identity.client_id)
            .header("Activation-Version", "2")
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        match response.status().as_u16() {
            200 => {
                info!("激活成功！");
                return Ok(());
            }
            202 => {
                info!("等待用户输入验证码，{}秒后重试...", ACTIVATION_RETRY_INTERVAL.as_secs());
                tokio::time::sleep(ACTIVATION_RETRY_INTERVAL).await;
            }
            _ => {
                warn!("服务器返回: {}，{}秒后重试...", response.status(), ACTIVATION_RETRY_INTERVAL.as_secs());
                tokio::time::sleep(ACTIVATION_RETRY_INTERVAL).await;
            }
        }
    }

    Err(anyhow!("激活失败，已达到最大重试次数"))
}

/// 获取本机IP
fn get_local_ip() -> Result<String> {
    use std::net::UdpSocket;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip().to_string())
}