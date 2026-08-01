# xiaozhi-rs

小智（Xiaozhi）语音助手的 Rust 客户端 —— 单进程全双工语音对话，跨平台、零系统 TLS 依赖。

## 功能特性

- **实时语音对话** — 全双工 WebSocket，20ms 音频帧实时双向流
- **Opus 编解码** — 16kHz / 单声道 / 20ms 帧长，动态加载本地 Opus 库
- **高质量重采样** — Catmull-Rom 三次样条插值，跨块相位连续
- **流畅听感加固** — 输出欠载软静音、Opus 丢包隐藏（PLC）、目标深度播放缓冲、TTS 切换清缓冲
- **稳健连接** — 心跳保活 + 指数退避自动重连
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

每个包已内置对应平台的 Opus 库（`libs/libopus/`），解压后即可运行，无需额外安装。

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

产物位于 `target/release/xiaozhi-rs`（Windows 为 `.exe`）。Opus 库在 `libs/libopus/` 下按 `{os}/{arch}/` 组织，运行时自动定位加载。

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

设备身份（MAC、客户端 ID、序列号、HMAC 密钥、激活状态）保存在 `efuse.json`，位置由 `directories` crate 按平台定位：

| 平台 | 路径 |
|------|------|
| Windows | `%APPDATA%\com\xiaozhi\xiaozhi-rs\config\efuse.json` |
| macOS | `~/Library/Application Support/com.xiaozhi.xiaozhi-rs/efuse.json` |
| Linux | `~/.config/com/xiaozhi/xiaozhi-rs/efuse.json` |

## 项目结构

```
src/
├── main.rs         CLI 入口 + 零依赖日志器（O(1) 日期解算）
├── client.rs       对话循环、重连退避、TTS 状态机
├── protocol.rs     WebSocket 收发分离（SplitSink/SplitStream，互不阻塞）
├── audio.rs        采集/播放、重采样、播放缓冲、防咔哒状态机
├── opus_codec.rs   Opus FFI 动态加载、编码/解码/PLC
├── identity.rs     设备身份（MAC 派生、HMAC、efuse 持久化）
├── ota.rs          OTA 配置拉取 + 激活轮询
└── message.rs      线上协议类型定义
```

## 许可证

[MIT License](LICENSE)
