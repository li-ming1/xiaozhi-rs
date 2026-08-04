//! 线上 JSON 消息协议（WebSocket 与 MQTT 共用）。
//! 关键约定：WS hello `version=1`、MQTT `version=3`；上行 `frame_duration=60`ms、`sample_rate=16000`。

use serde::{Deserialize, Serialize};

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
    /// 中断会话：异常结束/主动打断时通知服务器清理其会话状态
    /// （否则服务器滞留旧会话，新会话音频会被忽略约 60s）。
    Abort {
        session_id: String,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abort_serializes_with_session_and_reason() {
        let m = ClientMessage::Abort {
            session_id: "abc".into(),
            reason: "session_terminated".into(),
        };
        let json = serde_json::to_string(&m).unwrap();
        assert_eq!(
            json,
            r#"{"type":"abort","session_id":"abc","reason":"session_terminated"}"#
        );
    }
}

/// serde 过滤：false 字段不序列化（等价于 `std::ops::Not::not`，语义更直白）。
fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Features {
    #[serde(default, skip_serializing_if = "is_false")]
    pub mcp: bool,
    #[serde(default, skip_serializing_if = "is_false")]
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
    /// 统一 hello 构造：固定 features（mcp=true）+ 默认音频参数。
    fn hello(version: u8, transport: &str) -> Self {
        Self::Hello {
            version,
            features: Features {
                mcp: true,
                glyph_push: false,
            },
            transport: transport.to_string(),
            audio_params: AudioParams::default(),
        }
    }

    /// v2（BinaryProtocol2，带时间戳供服务端 AEC）。
    pub fn hello_websocket_v2() -> Self {
        Self::hello(2, "websocket")
    }

    pub fn hello_mqtt_udp() -> Self {
        Self::hello(3, "udp")
    }
}
