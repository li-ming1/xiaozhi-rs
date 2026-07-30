# xiaozhi-rs

小智（Xiaozhi）语音助手的 Rust 客户端实现 —— 轻量、跨平台、零系统 TLS 依赖。

## 功能特性

- **实时语音对话** — 全双工 WebSocket 通信，低延迟音频流
- **Opus 编解码** — 动态加载 Opus 库，16kHz 单声道，20ms 帧长
- **高质量重采样** — Catmull-Rom 三次样条插值，跨块相位连续
- **跨平台** — Windows / macOS / Linux 一套代码，纯 Rust TLS（rustls + ring）
- **设备激活** — OTA 配置拉取 + HMAC 签名激活流程
- **MCP 协议** — 支持 Model Context Protocol 工具调用

## 下载预编译版本

前往 [Releases](../../releases) 页面下载对应平台的压缩包，解压后直接运行：

| 平台 | 文件 |
|------|------|
| Windows x64 | `xiaozhi-rs-windows-x64.zip` |
| Windows ARM64 | `xiaozhi-rs-windows-arm64.zip` |
| macOS x64 (Intel) | `xiaozhi-rs-macos-x64.tar.gz` |
| macOS ARM64 (Apple Silicon) | `xiaozhi-rs-macos-arm64.tar.gz` |
| Linux x64 | `xiaozhi-rs-linux-x64.tar.gz` |
| Linux ARM64 | `xiaozhi-rs-linux-arm64.tar.gz` |

每个压缩包已包含对应平台的 Opus 库，无需额外安装。

## 从源码编译

### 环境要求

| 依赖 | Windows | macOS | Linux |
|------|---------|-------|-------|
| Rust 工具链 | ✓ | ✓ | ✓ |
| Opus 库 | 内置 `libs/libopus/` | 内置 `libs/libopus/` | 内置 `libs/libopus/` |
| TLS | rustls（内置） | rustls（内置） | rustls（内置） |

> 无需安装 OpenSSL 或其他系统 TLS 库，rustls + ring 为纯 Rust 实现。
> Opus 库已为所有 6 个目标平台预编译并包含在 `libs/libopus/` 中，程序运行时自动加载。

```bash
cargo build --release
```

生成的可执行文件位于 `target/release/xiaozhi-rs`（Windows 为 `xiaozhi-rs.exe`）。

### CI 自动构建

推送 `v*` 标签时，GitHub Actions 自动编译全部 6 个平台并发布 Release：

```bash
git tag v0.1.0
git push origin v0.1.0
```

## 使用方法

```bash
# 启动语音对话（首次运行需激活）
xiaozhi-rs start

# 跳过激活（使用测试 MAC 地址，服务器自动授权）
xiaozhi-rs start --skip-activation

# 查看设备信息
xiaozhi-rs info

# 重置设备身份（清除激活状态，重新生成 MAC/序列号）
xiaozhi-rs reset
```

### 激活流程

1. 运行 `xiaozhi-rs start`（不带 `--skip-activation`）
2. 程序显示激活码，访问 [xiaozhi.me](https://xiaozhi.me/) 输入
3. 激活成功后自动连接，后续启动无需重复激活

### 配置文件

配置文件位于：

| 平台 | 路径 |
|------|------|
| Windows | `%APPDATA%\xiaozhi\xiaozhi-rs\config\efuse.json` |
| macOS | `~/Library/Application Support/xiaozhi/xiaozhi-rs/config/efuse.json` |
| Linux | `~/.config/xiaozhi/xiaozhi-rs/config/efuse.json` |

存储设备身份信息（MAC 地址、客户端 ID、序列号、HMAC 密钥、激活状态）。

## 项目结构

```
src/
├── main.rs         CLI 入口 + 日志格式化（O(1) 日期算法）
├── client.rs       核心逻辑：连接管理、对话循环、重连退避
├── protocol.rs     WebSocket 收发分离（SplitSink/SplitStream，无锁互不阻塞）
├── audio.rs        音频采集播放 + Catmull-Rom 重采样（缓冲复用，零堆分配）
├── opus_codec.rs   Opus FFI 编解码（栈数组传递，动态库跨平台加载）
├── identity.rs     设备身份管理（MAC 生成、HMAC 签名、efuse 持久化）
├── ota.rs          OTA 配置拉取 + 激活轮询
└── message.rs      协议消息定义（JSON 序列化）
```

## 技术亮点

- **收发分离架构** — WebSocket Stream split 为独立发送/接收端，消除 `Arc<Mutex<Stream>>` 的收发互锁
- **零拷贝音频管线** — `[f32; FRAME_SIZE]` 栈数组传递，缓冲复用，消除每帧 ~100 次堆分配
- **VecDeque 播放缓冲** — `pop_front` O(1)，替代 `Vec::remove(0)` 的 O(n) 卡顿根因
- **rustls + ring** — 纯 Rust TLS，无需系统 OpenSSL；reqwest 用 `rustls-no-provider` 避免 aws-lc-rs 的 cmake/NASM 编译依赖
- **单帧节拍发送** — 20ms 定时器匹配音频帧长，每 tick 发一帧，避免延迟线性增长
- **LTO + 体积优化** — 项目代码 `opt-level=3`（音频 DSP），依赖库 `opt-level="z"`（体积）

## 许可证

[MIT License](LICENSE)
