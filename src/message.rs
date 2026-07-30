//! JSON 消息类型定义

use serde::{Deserialize, Serialize};

/// 客户端发送的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Message {
    /// 握手
    Hello {
        version: u8,
        #[serde(default)]
        features: Features,
        transport: String,
        audio_params: AudioParams,
    },

    /// 监听控制
    Listen {
        session_id: String,
        state: ListenState,
        #[serde(skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },

    /// 中止语音
    Abort {
        session_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

    /// IoT 设备描述
    Iot {
        session_id: String,
        update: bool,
        descriptors: Vec<serde_json::Value>,
    },

    /// MCP 消息
    Mcp {
        session_id: String,
        payload: serde_json::Value,
    },

    /// 关闭会话
    Goodbye {
        session_id: String,
    },
}

/// 服务器发送的消息
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Hello {
        transport: String,
        session_id: String,
        #[serde(default)]
        udp: Option<UdpConfig>,
    },

    Tts {
        state: TtsState,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },

    Listen {
        state: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<String>,
    },

    Stt {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        state: Option<String>,
    },

    Llm {
        #[serde(default)]
        text: Option<String>,
        #[serde(default)]
        emotion: Option<String>,
    },

    Mcp {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        payload: Option<serde_json::Value>,
    },

    Goodbye {
        session_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Features {
    #[serde(default)]
    pub mcp: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            frame_duration: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdpConfig {
    pub server: String,
    pub port: u16,
    pub key: String,
    pub nonce: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenState {
    Start,
    Stop,
    Detect,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsState {
    Start,
    Stop,
    SentenceStart,
    SentenceStop,
    #[serde(alias = "sentence_end")]
    SentenceEnd,
}

impl Message {
    /// 创建 hello 消息
    pub fn hello() -> Self {
        Message::Hello {
            version: 1,
            features: Features { mcp: true },
            transport: "websocket".to_string(),
            audio_params: AudioParams::default(),
        }
    }

    /// 创建监听开始消息
    pub fn listen_start(session_id: &str, mode: &str) -> Self {
        Message::Listen {
            session_id: session_id.to_string(),
            state: ListenState::Start,
            mode: Some(mode.to_string()),
            text: None,
        }
    }

    /// 创建监听停止消息
    pub fn listen_stop(session_id: &str) -> Self {
        Message::Listen {
            session_id: session_id.to_string(),
            state: ListenState::Stop,
            mode: None,
            text: None,
        }
    }
}