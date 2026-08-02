//! 类型化错误：供监督状态机按变体分支决策（认证失败 vs 瞬态故障 vs 永久错误）。

use thiserror::Error;

#[derive(Debug, Error)]
pub enum VoiceError {
    /// 认证失败：立即失效并刷新身份/配置，不计入退避。
    #[error("认证失败: {0}")]
    AuthenticationFailed(String),

    /// 配置无效（OTA 字段缺失、hex 解码失败等）。
    #[error("配置无效: {0}")]
    InvalidConfig(String),

    /// 传输层错误（连接、读写、TLS）。
    #[error("传输错误: {0}")]
    Transport(String),

    /// 协议错误（hello 不匹配、坏包、非法 JSON）。
    #[error("协议错误: {0}")]
    Protocol(String),

    /// 音频设备错误（无设备、流错误）。
    #[error("音频设备错误: {0}")]
    Audio(String),

    /// Opus 编解码错误。
    #[error("Opus 错误: {0}")]
    Opus(String),

    /// 加密错误（AES/hex）。
    #[error("加密错误: {0}")]
    Crypto(String),

    /// 会话已关闭/废弃。
    #[error("会话已关闭")]
    SessionClosed,

    /// 超时。
    #[error("超时: {0}")]
    Timeout(String),

    /// 永久性错误：不应重试。
    #[error("永久错误: {0}")]
    Permanent(String),

    /// 其他未分类错误。
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl VoiceError {
    /// 是否为瞬态故障（值得退避重试）。
    pub fn is_transient(&self) -> bool {
        !matches!(
            self,
            VoiceError::AuthenticationFailed(_)
                | VoiceError::InvalidConfig(_)
                | VoiceError::Permanent(_)
        )
    }
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
