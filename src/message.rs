//! JSON 消息类型定义

use serde::{Deserialize, Serialize};

/// 客户端发送的消息（仅保留实际构造的变体）
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
}

/// 服务器发送的消息（保留全部变体以反序列化服务器推送）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Hello {
        transport: String,
        session_id: String,
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
#[serde(rename_all = "snake_case")]
pub enum ListenState {
    Start,
    Stop,
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
}
