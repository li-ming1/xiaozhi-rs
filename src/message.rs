//! 线上消息协议类型。

use serde::{Deserialize, Serialize};

/// 客户端出站消息（仅序列化）。
#[derive(Debug, Serialize)]
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

/// 服务器入站消息（仅反序列化；保留全部变体以匹配推送）。
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ServerMessage {
    Hello {
        session_id: String,
    },

    Tts {
        state: TtsState,
        // sentence_start 携带分句正文；start/stop 无此字段。
        #[serde(default)]
        text: Option<String>,
    },

    Listen {
        state: String,
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

#[derive(Debug, Serialize, Default)]
pub struct Features {
    #[serde(default)]
    pub mcp: bool,
}

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenState {
    Start,
    /// 协议保留（realtime 模式不发送）。
    #[allow(dead_code)]
    Stop,
}

#[derive(Debug, Deserialize)]
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
    /// 构造建连 Hello 消息。
    pub fn hello() -> Self {
        Message::Hello {
            version: 1,
            features: Features { mcp: true },
            transport: "websocket".to_string(),
            audio_params: AudioParams::default(),
        }
    }
}
