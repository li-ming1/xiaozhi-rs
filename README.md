# xiaozhi-rs

小智（Xiaozhi）语音助手的 Rust 客户端 —— 单进程全双工语音对话，跨平台、零系统 TLS 依赖。

## 功能特性

- **双链路传输** — MQTT+UDP 主链路（AES-128-CTR 加密 + 序列号防重放），WebSocket v2 回退（二进制时间戳头，供服务端 AEC）；UDP 黑洞检测与 MQTT 熔断自动切换
- **实时语音对话** — 官方协议标准 60ms 音频帧全双工实时双向流
- **Opus 编解码** — 上行 16kHz/单声道/60ms，下行采样率服从服务器协商（16/24/48kHz）；`opusic-sys` 构建期静态内置 libopus，运行时零外部依赖
- **高质量重采样** — rubato 4.0 `AsyncSincFixedIn`（256-tap BlackmanHarris2），替代旧 Catmull-Rom；±1000ppm 时钟漂移实时补偿
- **无锁实时管线** — CPAL 回调零锁零分配，捕获/播放独立 DSP worker，rtrb SPSC 环形缓冲
- **语音活动检测** — 本地 VAD 静音抑制：静音帧不编码不上行，降低背景噪声对服务器 ASR/VAD 的干扰，空闲时零上行开销；防抖期起始帧确认后补发，开口不漏首音节
- **抗干扰打断抑制** — TTS 播放期自动抬高上行触发阈值、缩短尾随、暂停底噪学习：旁人说话与扬声器回声不再误打断，只有近讲真人能插话；用户真正打断时本地 TTS 10ms 内闪避让路；阈值全部可用环境变量调
- **自适应码率** — 按 10s 窗口的 UDP 序列号精确计损分级（Good/Fair/Poor）：编码侧 32/28/20kbps + FEC/DTX 切换；解码端丢包以 Opus PLC 补帧（连续补帧封顶，避免拖尾）
- **流畅听感加固** — 输出欠载 5ms 软静音、Opus PLC（连续补帧封顶）、自适应深度播放缓冲（40–240ms，句间停顿不再抬高延迟）、TTS 切换即时清空输出环、下行积压自动丢弃过期帧保实时
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

## 使用场景

小智是**全双工实时语音**——你说的时候它就在听，随时可以打断，对话像真人一样自然。接好服务端后，它可以陪你做很多事：

- **日常闲聊** — 天气、新闻、八卦、音乐、知识问答……连续对话不冷场，想到什么说什么
- **情绪陪伴** — 心情不好时倾诉几句，它倾听、安慰、给建议，永远有耐心
- **英语口语对练** — 跟它英文对话练口语练听力，它语速自然，听不懂还会追问
- **演讲 / 台词陪练** — 把稿子说给它听，让它当你的专属听众
- **哄睡与放松** — 让它讲个故事、读首诗，陪你在舒缓的对话里慢慢入眠

> 对话内容由接入的 小智AI 服务端生成；本项目负责把 小智AI 变成"随时可打断、低延迟、自然顺滑"的语音通话体验。

## 后台配置推荐

登录 [小智后台](https://xiaozhi.me/console/agents) 为你的设备配置智能体，以下组合可获得最佳体验：

- **模型** — 推荐 **DeepSeekV4（性格丰富）**：回复自然有温度、表达生动，闲聊陪伴时"人味儿"更足
- **角色音色** — 推荐 **普通话-湾湾小何 v2**：声音自然亲和、吐字清晰，听感舒适不机械

> 仅为推荐组合，可按喜好自由搭配模型与音色——全双工语音对话体验均不受影响。

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
git tag v0.1.3 && git push origin v0.1.3
```

## 使用方法

**双击可执行程序**即直接启动语音对话（等价于 `xiaozhi-rs start`，首次运行需激活）。

```bash
xiaozhi-rs start                 # 启动语音对话
xiaozhi-rs skip                  # 跳过激活直接启动（测试用）
xiaozhi-rs info                  # 查看设备身份
xiaozhi-rs doctor                # 检查默认音频设备与格式
xiaozhi-rs reset                 # 重置设备身份（重新生成 MAC/序列号，需重新激活）
```

日志级别由环境变量 `RUST_LOG` 控制（`error` / `warn` / `info` / `debug`，默认 `info`）：

```bash
RUST_LOG=debug xiaozhi-rs start
```

### 干扰抑制调参

用音箱外放时，机器人自己的声音或旁人说话会被当成"用户插话"上传，导致 TTS 被动不动打断。
客户端在 TTS 播放期自动抬高上行触发阈值来抑制，强度可用环境变量按房间噪声调整：

| 变量 | 默认 | 含义 |
|------|------|------|
| `XIAOZHI_VAD_PLAY_BOOST_DB` | `12` | 播放期触发阈值额外抬升（dB）。**老被打断就调大**，想更容易插话就调小，`0`= 关闭抑制 |
| `XIAOZHI_VAD_PLAY_TAIL_MS` | `300` | 播放活跃判定的尾随窗口（ms），覆盖播放缓冲排空 |
| `XIAOZHI_VAD_ON_DB` | `8` | 触发阈值高于自适应噪声底的 dB |
| `XIAOZHI_VAD_OFF_DB` | `4` | 释放阈值高于噪声底的 dB |
| `XIAOZHI_VAD_MIN_ON_DB` | `-68` | 触发阈值绝对下限（dBFS）。环境嘈杂可调高（如 `-60`） |
| `XIAOZHI_VAD_MIN_OFF_DB` | `-74` | 释放阈值绝对下限（dBFS） |
| `XIAOZHI_VAD_ON_FRAMES` | `2` | 触发防抖帧数（×60ms），调大更不容易误触发 |
| `XIAOZHI_VAD_HANGOVER_MS` | `720` | 语音结束后仍发送的尾随时长，调小更快"说完" |
| `XIAOZHI_VAD_STRONG_DB` | `-52` | 语音态内视为"强音"的门限；说话轻、老被钳制释放可调低（如 `-58`） |

```powershell
# Windows PowerShell：干扰较多时
$env:XIAOZHI_VAD_PLAY_BOOST_DB = "18"; $env:XIAOZHI_VAD_ON_FRAMES = "3"; xiaozhi-rs start
```

播放期抑制之上还有一层本地保护：用户真正插话时，输出音量在 10ms 内闪避到约 -9dB，
服务器下达停止前就为你的声音让路，停口 400ms 后平滑恢复；这一层无需配置。

调试时看 `RUST_LOG=debug` 的"捕获诊断"：其中 `静音N`、`播放期N` 与 `丢弃输入` 能看出 VAD 抑制与采集处理是否正常；
"播放诊断"里的 `占用/目标/漂移/丢弃/欠载` 可确认抖动缓冲、时钟补偿、积压丢帧与输出欠载是否正常。
若机器人说话期间 `RMS` 明显升高，说明是扬声器回声泄漏，改用耳机或降低音量最有效。

### 音频设备选择

`xiaozhi-rs doctor` 会列出全部输入/输出设备及默认格式，并标出系统默认与按环境变量实际将使用的设备。
客户端默认使用系统默认设备；可用环境变量按名称子串（不区分大小写）指定，未匹配时回退默认并输出警告：

| 变量 | 含义 |
|------|------|
| `XIAOZHI_INPUT_DEVICE` | 输入设备名称子串（如 `麦克风阵列`） |
| `XIAOZHI_OUTPUT_DEVICE` | 输出设备名称子串（如 `Realtek`、`耳机`） |

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
│   ├── ws.rs        WebSocket v2 传输（BinaryProtocol2，心跳 15s）
│   └── mqtt_udp.rs  MQTT+UDP 主链路（QoS0、防重放、UDP 黑洞检测）
└── audio/
    ├── mod.rs       无锁管线：CPAL 回调 + 独立 DSP worker（CaptureWorker/PlaybackWorker）+ 漂移补偿
    ├── opus.rs      Opus 编解码（opusic-sys 静态内置）+ PLC + 网络分级
    ├── resample.rs  rubato AsyncSinc（256-tap BlackmanHarris2）
    └── buffer.rs    自适应深度播放缓冲（40–240ms）
```

## 致谢

本项目深度参考了[小智 AI](https://github.com/78xiaozhi-esp32)（xiaozhi-me）的开源设计——MQTT+UDP 双链路、AES-CTR 加密音频、WebSocket 回退、设备激活与 OTA 等协议细节均与其对齐。感谢小智团队与社区的开放精神，让"低成本硬件上的实时语音助手"成为现实，也才有了这个 Rust 客户端。

## 许可证

[MIT License](LICENSE)
