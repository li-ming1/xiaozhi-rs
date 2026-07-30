//! 小智客户端 - Rust 极简实现

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;
use std::io::Write;

mod audio;
mod client;
mod identity;
mod message;
mod opus_codec;
mod ota;
mod protocol;

use client::Client;
use identity::DeviceIdentity;
use ota::OtaConfig;

#[derive(Parser)]
#[command(name = "xiaozhi-rs")]
#[command(about = "小智客户端", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// 启动客户端（自动激活+连接）
    Start {
        /// 跳过激活流程
        #[arg(short, long)]
        skip_activation: bool,
    },
    /// 显示设备信息
    Info,
    /// 清除设备身份（重新激活）
    Reset,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 自定义日志格式：本地时间（UTC+8）+ 消息
    // 使用 O(1) 日期算法，避免每次日志都循环减年份
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let local = now + 8 * 3600; // UTC+8
            let days = (local / 86400) as i64;
            let secs = local % 86400;

            let (year, month, day) = civil_from_days(days);
            let hour = (secs / 3600) as u8;
            let min = ((secs % 3600) / 60) as u8;
            let sec = (secs % 60) as u8;

            writeln!(
                buf,
                "[{:04}-{:02}-{:02} {:02}:{:02}:{:02}] {}",
                year, month, day, hour, min, sec,
                record.args()
            )
        })
        .init();

    let cli = Cli::parse();

    // 如果没有提供命令，显示帮助信息
    if let Some(cmd) = cli.command {
        match cmd {
            Commands::Start { skip_activation } => {
                run_with_signal(skip_activation).await?;
            }
            Commands::Info => {
                show_device_info()?;
            }
            Commands::Reset => {
                reset_device()?;
            }
        }
    } else {
        // 默认行为：启动客户端
        println!("小智客户端 v0.1.0");
        println!("用法: xiaozhi-rs <COMMAND>");
        println!();
        println!("Commands:");
        println!("  start   启动语音对话");
        println!("  info    显示设备信息");
        println!("  reset   重置设备身份");
        println!();
        println!("正在启动语音对话...");
        println!("(按 Ctrl+C 退出)");
        println!();

        run_with_signal(false).await?;
    }

    Ok(())
}

/// Howard Hinnant 的 civil_from_days 算法：O(1) 将"自 epoch 起的天数"转为 (年, 月, 日)
/// 替代旧实现的逐年/逐月循环，高频日志下显著降低开销
fn civil_from_days(z: i64) -> (i32, u8, u8) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8;
    let m = if mp < 10 { (mp + 3) as u8 } else { (mp - 9) as u8 };
    let y = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (y, m, d)
}

/// 运行客户端，监听 Ctrl+C 优雅退出
async fn run_with_signal(skip_activation: bool) -> Result<()> {
    tokio::select! {
        res = run_client(skip_activation) => res,
        _ = tokio::signal::ctrl_c() => {
            info!("收到 Ctrl+C 信号，正在退出...");
            Ok(())
        }
    }
}

/// 启动客户端：身份加载 → 激活 → 连接 → 对话
async fn run_client(skip_activation: bool) -> Result<()> {
    info!("正在启动小智客户端...");

    // 1. 加载/创建设备身份
    let mut identity = DeviceIdentity::load_or_create()?;

    // 如果跳过激活，使用测试 MAC 地址
    if skip_activation {
        identity.device_id = DeviceIdentity::get_test_mac_address();
        info!("使用测试 MAC 地址: {}", identity.device_id);
    }

    // 2. OTA 拉取配置 + 激活
    let ota_config = ensure_activated(&mut identity, skip_activation).await?;

    // 3. 连接并对话
    connect_and_run(&identity, &ota_config).await
}

/// 确保设备已激活，返回 OTA 配置
async fn ensure_activated(
    identity: &mut DeviceIdentity,
    skip_activation: bool,
) -> Result<OtaConfig> {
    info!("正在从 OTA 服务器获取配置...");
    let mut ota_config = ota::fetch_config(identity).await?;

    if skip_activation || identity.is_activated() {
        return Ok(ota_config);
    }

    const MAX_RETRIES: u32 = 5;
    let mut retry_count = 0;

    while retry_count < MAX_RETRIES {
        if let Some(activation_data) = &ota_config.activation {
            retry_count += 1;
            info!("设备需要激活（尝试 {}/{}）", retry_count, MAX_RETRIES);
            info!("请访问 https://xiaozhi.me/ 输入激活码：{}", activation_data.code);

            // 等待激活（60秒超时）
            match ota::wait_for_activation(
                identity,
                &activation_data.challenge,
            )
            .await
            {
                Ok(_) => {
                    info!("激活成功！");
                    identity.set_activated()?;
                    return Ok(ota_config);
                }
                Err(e) => {
                    if retry_count < MAX_RETRIES {
                        info!("激活码已过期，正在重新获取...");
                        ota_config = ota::fetch_config(identity).await?;
                    } else {
                        return Err(anyhow::anyhow!("激活失败，已达到最大重试次数: {}", e));
                    }
                }
            }
        } else {
            // 没有激活数据，表示已激活
            return Ok(ota_config);
        }
    }

    Ok(ota_config)
}

/// 连接 WebSocket 并开始对话
async fn connect_and_run(identity: &DeviceIdentity, ota_config: &OtaConfig) -> Result<()> {
    let ws_url = ota_config
        .websocket
        .url
        .clone()
        .ok_or_else(|| anyhow::anyhow!("未获取到 WebSocket URL"))?;
    let token = ota_config
        .websocket
        .token
        .clone()
        .unwrap_or_else(|| "test-token".to_string());

    info!("正在连接 WebSocket: {}", ws_url);
    let mut client = Client::new(ws_url, token, identity.device_id.clone(), identity.client_id.clone());
    client.connect().await?;

    info!("已连接！开始语音对话...");
    client.start_conversation().await?;

    Ok(())
}

fn show_device_info() -> Result<()> {
    let identity = DeviceIdentity::load_or_create()?;

    println!("设备信息:");
    println!("  设备ID (MAC): {}", identity.device_id);
    println!("  客户端ID: {}", identity.client_id);
    println!("  序列号: {}", identity.serial_number);
    println!("  已激活: {}", identity.is_activated());
    println!();
    println!("配置文件: {}", identity.efuse_path.display());

    Ok(())
}

fn reset_device() -> Result<()> {
    let identity = DeviceIdentity::load_or_create()?;
    std::fs::remove_file(&identity.efuse_path)?;
    println!("设备身份已清除，下次启动将重新生成");
    Ok(())
}
