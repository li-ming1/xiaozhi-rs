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
    // 自定义日志格式：本地时间 + 消息
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            // 计算本地时间（UTC+8）
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            // UTC+8 转换
            let local = now + 8 * 3600;
            let days_since_epoch = local / 86400;
            let secs = local % 86400;

            // 正确的日期计算算法
            fn days_in_year(year: i32) -> i32 {
                if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                    366
                } else {
                    365
                }
            }

            fn days_in_month(year: i32, month: u8) -> u8 {
                match month {
                    1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                    4 | 6 | 9 | 11 => 30,
                    2 => if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) { 29 } else { 28 },
                    _ => 0,
                }
            }

            // 计算年份
            let mut year = 1970;
            let mut days_left = days_since_epoch as i32;
            loop {
                let days = days_in_year(year);
                if days_left < days {
                    break;
                }
                days_left -= days;
                year += 1;
            }

            // 计算月份和日期
            let mut month: u8 = 1;
            loop {
                let days = days_in_month(year, month) as i32;
                if days_left < days {
                    break;
                }
                days_left -= days;
                month += 1;
            }

            let day = (days_left + 1) as u8;
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
                run_client(skip_activation).await?;
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
        
        run_client(false).await?;
    }

    Ok(())
}

async fn run_client(skip_activation: bool) -> Result<()> {
    info!("正在启动小智客户端...");

    // 1. 加载/创建设备身份
    let mut identity = DeviceIdentity::load_or_create()?;

    // 如果跳过激活，使用测试 MAC 地址
    if skip_activation {
        identity.device_id = DeviceIdentity::get_test_mac_address();
        info!("使用测试 MAC 地址: {}", identity.device_id);
    }

    // 2. OTA 拉取配置
    info!("正在从 OTA 服务器获取配置...");
    let mut ota_config = ota::fetch_config(&identity).await?;

    // 3. 检查激活状态
    if !skip_activation && !identity.is_activated() {
        const MAX_RETRIES: u32 = 5;
        let mut retry_count = 0;

        while retry_count < MAX_RETRIES {
            if let Some(activation_data) = &ota_config.activation {
                retry_count += 1;
                info!("设备需要激活（尝试 {}/{}）", retry_count, MAX_RETRIES);
                info!("请访问 https://xiaozhi.me/ 输入激活码：{}", activation_data.code);
                
                // 等待激活（60秒超时）
                match ota::wait_for_activation(&identity, &activation_data.challenge, &activation_data.code).await {
                    Ok(_) => {
                        info!("激活成功！");
                        identity.set_activated()?;
                        break;
                    }
                    Err(e) => {
                        if retry_count < MAX_RETRIES {
                            info!("激活码已过期，正在重新获取...");
                            // 重新获取配置（新的激活码）
                            ota_config = ota::fetch_config(&identity).await?;
                        } else {
                            return Err(anyhow::anyhow!("激活失败，已达到最大重试次数: {}", e));
                        }
                    }
                }
            } else {
                // 没有激活数据，表示已激活
                break;
            }
        }
    }

    // 4. 连接 WebSocket
    let ws_url = ota_config.websocket.url.ok_or_else(|| anyhow::anyhow!("未获取到 WebSocket URL"))?;
    let token = ota_config.websocket.token.unwrap_or_else(|| "test-token".to_string());

    info!("正在连接 WebSocket: {}", ws_url);
    let mut client = Client::new(ws_url, token, identity.device_id, identity.client_id);
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