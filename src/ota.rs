//! OTA 配置拉取与激活流程

use anyhow::{anyhow, Result};
use log::{info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::identity::DeviceIdentity;

const OTA_URL: &str = "https://api.tenclass.net/xiaozhi/ota/";

/// OTA 响应配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtaConfig {
    #[serde(default)]
    pub websocket: WebSocketConfig,
    #[serde(default)]
    pub activation: Option<ActivationData>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WebSocketConfig {
    pub url: Option<String>,
    pub token: Option<String>,
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
    info!("正在连接服务器（超时60秒）...");
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(15))
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

    // 直接反序列化（字段缺失由 #[serde(default)] 处理，未知字段自动忽略）
    let config: OtaConfig = serde_json::from_value(ota_response)?;

    if config.activation.is_some() {
        warn!("设备需要激活");
    } else {
        info!("设备已授权");
    }

    Ok(config)
}

/// 等待激活完成（60秒超时）
pub async fn wait_for_activation(
    identity: &DeviceIdentity,
    challenge: &str,
) -> Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
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

    // 60秒超时，每5秒轮询一次（共12次）
    const MAX_POLLS: u32 = 12;
    const POLL_INTERVAL: Duration = Duration::from_secs(5);

    info!("请在60秒内完成激活...");

    for attempt in 1..=MAX_POLLS {
        // 先等待，给用户时间输入
        if attempt > 1 {
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        // 注意：v1 激活版本不添加 Activation-Version 头部
        let response = client
            .post(&activate_url)
            .header("Device-Id", &identity.device_id)
            .header("Client-Id", &identity.client_id)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await;

        match response {
            Ok(resp) => {
                match resp.status().as_u16() {
                    200 => {
                        info!("激活成功！");
                        return Ok(());
                    }
                    202 => {
                        let remaining = (MAX_POLLS - attempt) * 5;
                        info!("等待用户输入验证码...（剩余{}秒）", remaining);
                    }
                    status => {
                        warn!("服务器返回状态码: {}", status);
                    }
                }
            }
            Err(_) => {
                // 网络错误，继续重试
                let remaining = (MAX_POLLS - attempt) * 5;
                info!("网络请求失败，{}秒后重试...（剩余{}秒）", POLL_INTERVAL.as_secs(), remaining);
            }
        }
    }

    Err(anyhow!("激活码已过期（60秒超时）"))
}

/// 获取本机IP
fn get_local_ip() -> Result<String> {
    use std::net::UdpSocket;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip().to_string())
}