//! 会话纪元（SessionEpoch）：每次连接/传输创建全新实例。
//!
//! session_id、AES、序列号、Opus 编解码器、抖动缓冲与音频队列均属于纪元状态，
//! 禁止跨连接或跨传输复用（重构方案的硬性约束）。纪元仅作标识与生命周期跟踪，
//! 具体状态由各传输/worker 持有并在新建纪元时重建。

use std::sync::atomic::{AtomicU64, Ordering};

/// 传输类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    MqttUdp,
    WebSocket,
}

/// 会话纪元。
#[derive(Debug)]
pub struct SessionEpoch {
    pub id: u64,
    pub session_id: String,
    pub transport: TransportKind,
}

static EPOCH_COUNTER: AtomicU64 = AtomicU64::new(0);

impl SessionEpoch {
    pub fn new(session_id: String, transport: TransportKind) -> Self {
        Self {
            id: EPOCH_COUNTER.fetch_add(1, Ordering::Relaxed),
            session_id,
            transport,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_are_unique_and_monotonic() {
        let a = SessionEpoch::new("s1".into(), TransportKind::WebSocket);
        let b = SessionEpoch::new("s2".into(), TransportKind::MqttUdp);
        assert_ne!(a.id, b.id);
        assert!(b.id > a.id);
    }
}
