//! OTA 配置与激活：ureq 阻塞调用经 spawn_blocking 隔离，不占满单线程 runtime。

use anyhow::{anyhow, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::identity::DeviceIdentity;

const OTA_URL: &str = "https://api.tenclass.net/xiaozhi/ota/";

/// OTA 响应：连接参数与可选激活任务。
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

/// 构造 ureq Agent：关闭状态码自动报错（激活须手动区分 200/202）。
fn build_agent(timeout: Duration) -> ureq::Agent {
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(timeout))
        .http_status_as_error(false)
        .build();
    ureq::Agent::new_with_config(config)
}

/// 拉取 OTA 配置（阻塞 I/O 经 spawn_blocking 移出异步上下文）。
pub async fn fetch_config(identity: &DeviceIdentity) -> Result<OtaConfig> {
    let identity = identity.clone();
    tokio::task::spawn_blocking(move || fetch_config_blocking(&identity)).await?
}

fn fetch_config_blocking(identity: &DeviceIdentity) -> Result<OtaConfig> {
    info!("正在连接服务器（超时60秒）...");
    let agent = build_agent(Duration::from_secs(60));

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

    let mut response = agent
        .post(OTA_URL)
        .header("Device-Id", &identity.device_id)
        .header("Client-Id", &identity.client_id)
        .header("User-Agent", "bread-compact-wifi/xiaozhi-rs-0.1.0")
        .header("Accept-Language", "zh-CN")
        .send_json(&payload)?;

    if !response.status().is_success() {
        return Err(anyhow!("OTA 请求失败: HTTP {}", response.status()));
    }

    // 直接反序列化；缺字段由 #[serde(default)] 兜底，未知字段自动忽略。
    let config: OtaConfig = response.body_mut().read_json()?;
    debug!("OTA 响应: {:?}", config);

    if config.activation.is_some() {
        warn!("设备需要激活");
    } else {
        info!("设备已授权");
    }

    Ok(config)
}

/// 轮询激活完成（60s 超时，每 5s 一次）。
pub async fn wait_for_activation(identity: &DeviceIdentity, challenge: &str) -> Result<()> {
    let identity = identity.clone();
    let challenge = challenge.to_string();
    tokio::task::spawn_blocking(move || wait_for_activation_blocking(&identity, &challenge)).await?
}

fn wait_for_activation_blocking(identity: &DeviceIdentity, challenge: &str) -> Result<()> {
    let agent = build_agent(Duration::from_secs(10));

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
        // 首轮免等，后续间隔轮询，留出用户输入时间。
        if attempt > 1 {
            std::thread::sleep(POLL_INTERVAL);
        }

        // v1 激活协议不加 Activation-Version 头。
        let response = agent
            .post(&activate_url)
            .header("Device-Id", &identity.device_id)
            .header("Client-Id", &identity.client_id)
            .send_json(&payload);

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
                // 网络抖动，计入剩余时间后重试。
                let remaining = (MAX_POLLS - attempt) * 5;
                info!("网络请求失败，{}秒后重试...（剩余{}秒）", POLL_INTERVAL.as_secs(), remaining);
            }
        }
    }

    Err(anyhow!("激活码已过期（60秒超时）"))
}

/// 经 UDP 连接到公网地址，取本地出口 IP（不实际发包）。
fn get_local_ip() -> Result<String> {
    use std::net::UdpSocket;

    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let local_addr = socket.local_addr()?;
    Ok(local_addr.ip().to_string())
}
