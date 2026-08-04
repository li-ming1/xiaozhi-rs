//! 类型化错误：供监督状态机按变体分支决策。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum VoiceError {
    #[error("认证失败: {0}")]
    AuthenticationFailed(String),
    #[error("配置无效: {0}")]
    InvalidConfig(String),
    #[error("传输错误: {0}")]
    Transport(String),
    #[error("协议错误: {0}")]
    Protocol(String),
    #[error("音频设备错误: {0}")]
    Audio(String),
    #[error("Opus 错误: {0}")]
    Opus(String),
    #[error("加密错误: {0}")]
    Crypto(String),
    #[error("会话已关闭")]
    SessionClosed,
    #[error("超时: {0}")]
    Timeout(String),
}

pub type Result<T> = std::result::Result<T, VoiceError>;

impl From<std::io::Error> for VoiceError {
    fn from(e: std::io::Error) -> Self {
        VoiceError::Transport(e.to_string())
    }
}

impl From<serde_json::Error> for VoiceError {
    fn from(e: serde_json::Error) -> Self {
        VoiceError::Protocol(e.to_string())
    }
}
