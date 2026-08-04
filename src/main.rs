//! xiaozhi-rs — 单进程全双工语音客户端（MQTT+UDP 主链路 / WebSocket 回退）。

use anyhow::Result;
use log::{info, warn};
use tokio_util::sync::CancellationToken;

use xiaozhi_rs::identity::DeviceIdentity;
use xiaozhi_rs::ota::OtaConfig;
use xiaozhi_rs::RealtimeVoice;

/// 退出原因文件（任何路径都写入，防静默崩溃无法定位）。
fn write_exit_reason(msg: &str) {
    let _ = std::fs::write("xiaozhi-exit.log", format!("{}\n", msg));
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    std::panic::set_hook(Box::new(|info| {
        let msg = format!("[panic] {:?}", info);
        eprintln!("{}", msg);
        write_exit_reason(&msg);
    }));
    // ring provider 须在任意 HTTPS 调用前注册一次（reqwest/rumqttc 复用）。
    rustls::crypto::ring::default_provider().install_default().ok();

    init_logger();

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None => {
            print_usage();
            println!();
            println!("正在启动语音对话...");
            println!("(按 Ctrl+C 退出)");
            println!();
            run_with_signal(false).await?;
        }
        Some("start") => run_with_signal(false).await?,
        Some("skip") => run_with_signal(true).await?,
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
    println!("小智客户端 v{}", env!("CARGO_PKG_VERSION"));
    println!("用法: xiaozhi-rs <COMMAND>");
    println!();
    println!("Commands:");
    println!("  start   启动语音对话");
    println!("  skip    跳过激活直接启动");
    println!("  info    显示设备信息");
    println!("  reset   重置设备身份");
}

/// 零依赖日志前端。级别由 RUST_LOG 注入。
struct SimpleLogger;

static LOGGER: SimpleLogger = SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::ZERO)
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
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    // 独立任务监听 Ctrl+C 并取消 shutdown；不等待其完成（进程退出时自然终止）。
    let _sig_task = tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("收到 Ctrl+C 信号，正在退出...");
                shutdown_clone.cancel();
            }
            Err(e) => warn!("信号监听失败: {}（忽略，不退出）", e),
        }
    });

    let res = run_client(skip_activation, shutdown).await;
    let reason = match &res {
        Ok(()) => "normal-return（supervisor 已返回）".to_string(),
        Err(e) => format!("error: {:?}", e),
    };
    eprintln!("[退出] {}", reason);
    info!("客户端退出原因: {}", reason);
    write_exit_reason(&format!("[退出] {}", reason));
    // 不等待 sig_task：supervisor 只在 shutdown 取消时返回，而激活失败/配置错误等
    // 路径 shutdown 从未取消，await 会使进程挂起。Ctrl+C 已触发时任务自会完成。
    res
}

/// 客户端生命周期：身份 → 激活 → 监督器（内建重连/回退）。
async fn run_client(skip_activation: bool, shutdown: CancellationToken) -> Result<()> {
    info!("正在启动小智客户端...");

    let mut identity = DeviceIdentity::load_or_create()?;
    if skip_activation {
        identity.device_id = DeviceIdentity::TEST_MAC.to_string();
        info!("使用测试 MAC 地址: {}", identity.device_id);
    }

    let ota_config = ensure_activated(&mut identity, skip_activation).await?;

    let voice = RealtimeVoice::from_ota(identity, ota_config)?;
    voice.run_until(shutdown).await?;
    Ok(())
}

/// 保证设备已激活，返回 OTA 配置；未激活则进入轮询授权流程。
async fn ensure_activated(
    identity: &mut DeviceIdentity,
    skip_activation: bool,
) -> Result<OtaConfig> {
    info!("正在从 OTA 服务器获取配置...");
    let mut ota_config = xiaozhi_rs::ota::fetch_config(identity).await?;

    if skip_activation || identity.is_activated() {
        return Ok(ota_config);
    }

    const MAX_RETRIES: u32 = 5;
    for attempt in 1..=MAX_RETRIES {
        let Some(activation_data) = &ota_config.activation else {
            return Ok(ota_config);
        };
        info!("设备需要激活（尝试 {}/{}）", attempt, MAX_RETRIES);
        info!("请访问 https://xiaozhi.me/ 输入激活码：{}", activation_data.code);

        match xiaozhi_rs::ota::wait_for_activation(identity, &activation_data.challenge).await {
            Ok(_) => {
                info!("激活成功！");
                identity.set_activated()?;
                return Ok(ota_config);
            }
            Err(_) if attempt < MAX_RETRIES => {
                info!("激活码已过期，正在重新获取...");
                ota_config = xiaozhi_rs::ota::fetch_config(identity).await?;
            }
            Err(e) => return Err(anyhow::anyhow!("激活失败，已达到最大重试次数: {}", e)),
        }
    }
    Ok(ota_config)
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
