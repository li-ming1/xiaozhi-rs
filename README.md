# xiaozhi-rs

小智（Xiaozhi）语音助手的 Rust 客户端 —— 单进程全双工语音对话，跨平台、零系统 TLS 依赖。

## 功能特性

- **双链路传输** — MQTT+UDP 主链路（AES-128-CTR 加密 + 序列号防重放），WebSocket v2 回退（二进制时间戳头，供服务端 AEC）；UDP 黑洞检测与 MQTT 熔断自动切换
- **实时语音对话** — 官方协议标准 60ms 音频帧全双工实时双向流
- **Opus 编解码** — 上行 16kHz/单声道/60ms，下行采样率服从服务器协商（16/24/48kHz）；`opusic-sys` 构建期静态内置 libopus，运行时零外部依赖
- **高质量重采样** — rubato 4.0 `AsyncSincFixedIn`（256-tap BlackmanHarris2），替代旧 Catmull-Rom；±1000ppm 时钟漂移实时补偿
- **无锁实时管线** — CPAL 回调零锁零分配，捕获/播放独立 DSP worker，rtrb SPSC 环形缓冲
- **语音活动检测** — 本地 VAD 静音抑制：静音帧不编码不上行，降低背景噪声对服务器 ASR/VAD 的干扰，空闲时零上行开销
- **自适应码率** — 按 10s 丢包窗口分级（Good/Fair/Poor）：编码侧 32/28/20kbps + FEC/DTX 切换，弱网同步启用解码侧 FEC 恢复
- **流畅听感加固** — 输出欠载 5ms 软静音、Opus PLC、自适应深度播放缓冲（40–240ms）、TTS 切换清缓冲
- **稳健连接** — 心跳保活 + decorrelated-jitter 无限退避；每次会话重建连接状态，任何状态不跨连接复用
- **设备激活** — OTA 配置拉取 + HMAC-SHA256 签名激活
- **跨平台** — Windows / macOS / Linux 一套代码（含 x64 与 ARM64）
- **纯 Rust TLS** — rustls + ring，无需安装 OpenSSL

## 快速开始

前往 [Releases](../../releases) 下载对应平台的压缩包，解压后直接运行：

| 平台 | 文件 |
|------|------|
| Windows x64 | `xiaozhi-rs-windows-x64.zip` |
| Windows ARM64 | `xiaozhi-rs-windows-arm64.zip` |
| macOS Intel | `xiaozhi-rs-macos-x64.tar.gz` |
| macOS Apple Silicon | `xiaozhi-rs-macos-arm64.tar.gz` |
| Linux x64 | `xiaozhi-rs-linux-x64.tar.gz` |
| Linux ARM64 | `xiaozhi-rs-linux-arm64.tar.gz` |

Opus 编解码器已由 `opusic-sys` 静态内置进可执行文件，解压后即可运行，无需部署任何动态库。

```bash
# 启动语音对话（首次运行需激活，见下文"激活流程"）
xiaozhi-rs start

# 调试用：跳过激活（服务器自动授权）
xiaozhi-rs skip
```

## 从源码编译

依赖：Rust 工具链（stable）。**无需** OpenSSL / ALSA 以外的系统库（Linux 下 `cpal` 需要 ALSA 头文件）。

```bash
# Linux 先装 ALSA 开发库（Windows/macOS 跳过）
sudo apt-get install -y libasound2-dev

cargo build --release
```

产物位于 `target/release/xiaozhi-rs`（Windows 为 `.exe`）。libopus 由 `opusic-sys`（bundled）在构建期用 cmake 编译并静态链接，无需随包分发 Opus 库。

推送 `v*` 标签即可触发 GitHub Actions 构建全部 6 个平台并发布 Release：

```bash
git tag v0.1.0 && git push origin v0.1.0
```

## 使用方法

**双击可执行程序**即直接启动语音对话（等价于 `xiaozhi-rs start`，首次运行需激活）。

```bash
xiaozhi-rs start                 # 启动语音对话
xiaozhi-rs skip                  # 跳过激活直接启动（测试用）
xiaozhi-rs info                  # 查看设备身份
xiaozhi-rs reset                 # 重置设备身份（重新生成 MAC/序列号，需重新激活）
```

日志级别由环境变量 `RUST_LOG` 控制（`error` / `warn` / `info` / `debug`，默认 `info`）：

```bash
RUST_LOG=debug xiaozhi-rs start
```

### 激活流程

1. 运行 `xiaozhi-rs start`（跳过激活请用 `xiaozhi-rs skip`）；
2. 程序打印激活码，访问 [xiaozhi.me](https://xiaozhi.me/) 输入；
3. 激活成功后自动连接；激活状态持久化，后续启动无需重复。

### 身份与配置

设备身份（MAC、客户端 ID、序列号、HMAC 密钥、激活状态）保存在 `efuse.json`，位置按平台约定定位（无需外部 crate）：

| 平台 | 路径 |
|------|------|
| Windows | `%APPDATA%\xiaozhi-rs\efuse.json` |
| macOS | `~/Library/Application Support/xiaozhi-rs/efuse.json` |
| Linux | `$XDG_CONFIG_HOME/xiaozhi-rs/efuse.json`（或 `~/.config/xiaozhi-rs/efuse.json`） |

## 项目结构

```
src/
├── main.rs          CLI 入口 + 多线程 runtime + ring TLS provider + 日志器
├── lib.rs           模块声明与公共导出
├── supervisor.rs    VoiceSupervisor 状态机（SelectTransport→Connect→Streaming→Backoff）+ RealtimeVoice 入口 + 传输类型
├── error.rs         类型化错误（认证/瞬态/协议/音频等）
├── identity.rs      设备身份（MAC 派生、HMAC、efuse 持久化）
├── ota.rs           OTA 配置拉取 + 激活轮询（reqwest）
├── crypto.rs        AES-128-CTR + UDP 16 字节包头编解码（IV=包头）
├── protocol/
│   ├── mod.rs       TransportAdapter 闭集 + 统一收发句柄 + latest-slot 音频通道
│   ├── message.rs   线上 JSON 协议类型
│   ├── ws.rs        WebSocket v1/v2/v3 传输（心跳 15s）
│   └── mqtt_udp.rs  MQTT+UDP 主链路（QoS0、防重放、UDP 黑洞检测）
└── audio/
    ├── mod.rs       无锁管线：CPAL 回调 + 独立 DSP worker（CaptureWorker/PlaybackWorker）+ 漂移补偿
    ├── opus.rs      Opus 编解码（opusic-sys 静态内置）+ PLC + 网络分级
    ├── resample.rs  rubato AsyncSinc（256-tap BlackmanHarris2）
    └── buffer.rs    自适应深度播放缓冲（40–240ms）
```

## 许可证

[MIT License](LICENSE)
