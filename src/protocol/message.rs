//! 线上 JSON 消息协议（WebSocket 与 MQTT 共用文本载荷）。
//!
//! 关键约定（来自官方协议文档）：
//! - WebSocket hello `version = 1`，MQTT hello `version = 3`。
//! - 上行 `audio_params.frame_duration = 60`（ms），`sample_rate = 16000`。
//! - 服务器 hello 响应 `audio_params.sample_rate` 可为 16000/24000/48000；
//!   MQTT 响应额外含 `udp` 字段（server/port/key/nonce）。

use serde::{Deserialize, Serialize};

// ===================== 出站（设备 → 服务器） =====================

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ClientMessage {
    Hello {
        version: u8,
        #[serde(default, skip_serializing_if = "Features::is_empty")]
        features: Features,
        transport: String,
        audio_params: AudioParams,
    },
    Listen {
        session_id: String,
        state: ListenState,
        #[serde(skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Features {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mcp: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub glyph_push: bool,
}

impl Features {
    pub fn is_empty(&self) -> bool {
        !self.mcp && !self.glyph_push
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AudioParams {
    pub format: String,
    pub sample_rate: u32,
    pub channels: u8,
    pub frame_duration: u16,
}

impl Default for AudioParams {
    fn default() -> Self {
        Self {
            format: "opus".to_string(),
            sample_rate: 16000,
            channels: 1,
            frame_duration: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenState {
    Start,
    Stop,
}

// ===================== 入站（服务器 → 设备） =====================

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Hello(ServerHello),
    Tts {
        state: TtsState,
        #[serde(default)]
        text: Option<String>,
    },
    Stt {
        text: String,
    },
    Llm {
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        emotion: Option<String>,
    },
    Listen {
        state: String,
    },
    Mcp {
        #[serde(default)]
        payload: serde_json::Value,
    },
    System {
        command: String,
    },
    Goodbye {
        session_id: String,
    },
    /// 未识别 type：保留原始值供上层诊断。
    #[serde(other)]
    Unknown,
}

/// 服务器 hello 响应。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ServerHello {
    #[serde(default)]
    pub transport: String,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub audio_params: Option<AudioParams>,
    /// 仅 MQTT+UDP 传输存在。
    #[serde(default)]
    pub udp: Option<UdpChannel>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct UdpChannel {
    pub server: String,
    pub port: u16,
    /// AES 密钥（hex 字符串，32 字符 = 16 字节）。
    pub key: String,
    /// AES nonce（hex 字符串，32 字符 = 16 字节）。
    pub nonce: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsState {
    Start,
    Stop,
    SentenceStart,
    SentenceStop,
    #[serde(alias = "sentence_end")]
    SentenceEnd,
}

impl ClientMessage {
    /// WebSocket hello，二进制协议 v1（原始 Opus）。
    pub fn hello_websocket_v1() -> Self {
        Self::Hello {
            version: 1,
            features: Features {
                mcp: true,
                glyph_push: false,
            },
            transport: "websocket".to_string(),
            audio_params: AudioParams::default(),
        }
    }

    /// WebSocket hello，二进制协议 v2（BinaryProtocol2，带时间戳供服务端 AEC）。首选。
    pub fn hello_websocket_v2() -> Self {
        Self::Hello {
            version: 2,
            features: Features {
                mcp: true,
                glyph_push: false,
            },
            transport: "websocket".to_string(),
            audio_params: AudioParams::default(),
        }
    }

    /// WebSocket hello，二进制协议 v3（BinaryProtocol3，4 字节精简头，官方新固件默认）。
    pub fn hello_websocket_v3() -> Self {
        Self::Hello {
            version: 3,
            features: Features {
                mcp: true,
                glyph_push: false,
            },
            transport: "websocket".to_string(),
            audio_params: AudioParams::default(),
        }
    }

    /// MQTT+UDP hello（version=3）。
    pub fn hello_mqtt_udp() -> Self {
        Self::Hello {
            version: 3,
            features: Features {
                mcp: true,
                glyph_push: false,
            },
            transport: "udp".to_string(),
            audio_params: AudioParams::default(),
        }
    }
}
