//! xiaozhi-rs — 单进程全双工语音客户端。

use anyhow::Result;
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
use ota::OtaConfig;

// 单线程 runtime：业务仅为单一 select! 循环，音频回调驻留于系统线程，无调度器开销。
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // rustls 自带 ring provider 须在使用任意 HTTPS 调用前注册一次；重复安装返回 Err，可安全忽略。
    rustls::crypto::ring::default_provider().install_default().ok();

    init_logger();

    // 手写参数解析（仅三个子命令），弃用 clap 以消除其依赖树。
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        // 缺省子命令：进入对话。
        None => {
            print_usage();
            println!();
            println!("正在启动语音对话...");
            println!("(按 Ctrl+C 退出)");
            println!();
            run_with_signal(false).await?;
        }
        Some("start") => {
            let skip_activation = args.any(|a| a == "-s" || a == "--skip-activation");
            run_with_signal(skip_activation).await?;
        }
        Some("info") => show_device_info()?,
        Some("reset") => reset_device()?,
        Some("-h") | Some("--help") => print_usage(),
        Some(other) => {
            eprintln!("未知命令: {}", other);
            print_usage();
            std::process::exit(2);
        }
    }

    Ok(())
}

fn print_usage() {
    println!("小智客户端 v0.1.0");
    println!("用法: xiaozhi-rs <COMMAND>");
    println!();
    println!("Commands:");
    println!("  start   启动语音对话（-s/--skip-activation 跳过激活）");
    println!("  info    显示设备信息");
    println!("  reset   重置设备身份");
}

/// 零依赖日志前端，替代 env_logger（其正则/着色/模块过滤能力从未被使用）。
/// 级别由 RUST_LOG 注入，宏展开处已受 max_level 静态裁剪。
struct SimpleLogger;

static LOGGER: SimpleLogger = SimpleLogger;

impl log::Log for SimpleLogger {
    // 不过滤 metadata；分级由 set_max_level 在宏展开处静态完成。
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    // 时间戳取 UTC+8，日期由 civil_from_days 以 O(1) 闭式解算，避免历年循环。
    fn log(&self, record: &log::Record) {
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

        eprintln!(
            "[{:04}-{:02}-{:02} {:02}:{:02}:{:02}] {}",
            year, month, day, hour, min, sec,
            record.args()
        );
    }

    fn flush(&self) {}
}

/// 安装日志后端；RUST_LOG 缺省为 info。
fn init_logger() {
    let level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(log::LevelFilter::Info);
    log::set_logger(&LOGGER).expect("日志初始化失败");
    log::set_max_level(level);
}

/// 将"自 epoch 起的天数"闭式映射为 (年, 月, 日)（Howard Hinnant 的 civil_from_days，O(1)）。
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

/// 以 Ctrl+C 为终止信号驱动客户端主循环。
async fn run_with_signal(skip_activation: bool) -> Result<()> {
    tokio::select! {
        res = run_client(skip_activation) => res,
        _ = tokio::signal::ctrl_c() => {
            info!("收到 Ctrl+C 信号，正在退出...");
            Ok(())
        }
    }
}

/// 客户端生命周期：身份 → 激活 → 建连 → 对话。
async fn run_client(skip_activation: bool) -> Result<()> {
    info!("正在启动小智客户端...");

    let mut identity = DeviceIdentity::load_or_create()?;

    if skip_activation {
        identity.device_id = DeviceIdentity::get_test_mac_address();
        info!("使用测试 MAC 地址: {}", identity.device_id);
    }

    let ota_config = ensure_activated(&mut identity, skip_activation).await?;

    connect_and_run(&identity, &ota_config).await
}

/// 保证设备已激活，返回 OTA 配置；未激活则进入轮询授权流程。
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
            // 无 activation 字段即视为已授权。
            return Ok(ota_config);
        }
    }

    Ok(ota_config)
}

/// 建立 WebSocket 并进入全双工对话。
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
