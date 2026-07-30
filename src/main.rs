//! 小智客户端 - Rust 极简实现

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::info;

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
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

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
    let identity = DeviceIdentity::load_or_create()?;

    // 2. OTA 拉取配置
    info!("正在从 OTA 服务器获取配置...");
    let ota_config = ota::fetch_config(&identity).await?;

    // 3. 检查激活状态
    if !skip_activation && !identity.is_activated() {
        if let Some(activation_data) = &ota_config.activation {
            info!("设备未激活，请访问以下地址激活：");
            if let Some(url) = &activation_data.authorization_url {
                info!("  网址: {}", url);
            }
            info!("  激活码: {}", activation_data.code);

            // 轮询激活
            ota::wait_for_activation(&identity, &activation_data.challenge, &activation_data.code).await?;
            info!("激活成功！");
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