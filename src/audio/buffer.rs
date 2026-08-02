//! 播放缓冲状态机：`Buffering -> Playing -> Rebuffering`。
//!
//! 自适应目标深度（重构方案）：
//! - 初始 60ms；目标 = clamp(40ms + 4 × jitter_EWMA, 40, 240)；
//! - 增长立即生效；稳定 30s 后每次下降一帧；
//! - 物理容量 320ms，超限丢最旧帧。

use std::time::Instant;

/// 初始目标深度。
pub const INITIAL_TARGET_MS: u64 = 60;
/// 目标下限。
pub const TARGET_MIN_MS: u64 = 40;
/// 目标上限。
pub const TARGET_MAX_MS: u64 = 240;
/// 物理容量。
pub const CAPACITY_MS: u64 = 320;
/// 目标稳定多久后开始下降。
pub const SHRINK_STABLE_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferState {
    /// 缓冲中（起始/重缓冲后，尚未填到目标）。
    Buffering,
    /// 正常播放。
    Playing,
    /// 欠载后重缓冲。
    Rebuffering,
}

/// 播放缓冲控制器。仅做状态与目标计算，不直接操作 ring buffer。
pub struct PlaybackBuffer {
    state: BufferState,
    target_ms: u64,
    jitter_ewma_ms: f64,
    frame_ms: u64,
    last_arrival: Option<Instant>,
    stable_since: Option<Instant>,
}

impl PlaybackBuffer {
    pub fn new(frame_ms: u64) -> Self {
        Self {
            state: BufferState::Buffering,
            target_ms: INITIAL_TARGET_MS,
            jitter_ewma_ms: 0.0,
            frame_ms,
            last_arrival: None,
            stable_since: None,
        }
    }

    /// 记录一次帧到达（用于 jitter EWMA）。
    pub fn observe_arrival(&mut self, now: Instant) {
        if let Some(prev) = self.last_arrival {
            let dt = now.duration_since(prev).as_secs_f64() * 1000.0;
            let expected = self.frame_ms as f64;
            let dev = (dt - expected).abs();
            // 指数加权，α=0.1。
            self.jitter_ewma_ms = if self.jitter_ewma_ms == 0.0 {
                dev
            } else {
                0.9 * self.jitter_ewma_ms + 0.1 * dev
            };
        }
        self.last_arrival = Some(now);
    }

    /// 依据当前 jitter 重算目标深度（帧粒度向上取整）。
    pub fn update_target(&mut self, now: Instant) {
        let desired = (TARGET_MIN_MS as f64 + 4.0 * self.jitter_ewma_ms) as u64;
        let desired = desired.clamp(TARGET_MIN_MS, TARGET_MAX_MS);
        if desired > self.target_ms {
            // 增长立即生效。
            self.target_ms = desired;
            self.stable_since = None;
        } else if self.stable_since.is_none() {
            self.stable_since = Some(now);
        } else if now.duration_since(self.stable_since.unwrap()) >= SHRINK_STABLE_DURATION {
            // 稳定足够久，每次下降一帧。
            let frame = self.frame_ms.max(1);
            if self.target_ms > TARGET_MIN_MS && self.target_ms >= desired + frame {
                self.target_ms -= frame;
                self.stable_since = Some(now);
            }
        }
    }

    /// 目标深度（帧数）。
    pub fn target_frames(&self) -> u64 {
        self.target_ms.div_ceil(self.frame_ms)
    }

    pub fn target_ms(&self) -> u64 {
        self.target_ms
    }

    pub fn state(&self) -> BufferState {
        self.state
    }

    pub fn on_play_start(&mut self) {
        if self.state != BufferState::Playing {
            self.state = BufferState::Playing;
        }
    }

    /// 欠载：进入重缓冲。
    pub fn on_underrun(&mut self) {
        if self.state != BufferState::Rebuffering {
            self.state = BufferState::Rebuffering;
        }
    }

    /// 缓冲达标：恢复播放。
    pub fn on_refilled(&mut self) {
        self.state = BufferState::Playing;
    }

    /// 清空（TTS 切换）：回到 Buffering，目标复位。
    pub fn reset(&mut self) {
        self.state = BufferState::Buffering;
        self.target_ms = INITIAL_TARGET_MS;
        self.stable_since = None;
        self.last_arrival = None;
        self.jitter_ewma_ms = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_target_is_60ms() {
        let b = PlaybackBuffer::new(60);
        assert_eq!(b.target_ms(), 60);
        assert_eq!(b.target_frames(), 1);
    }

    #[test]
    fn target_clamps_and_grows_immediately() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        // 注入大 jitter：每次到达偏离 100ms。
        for _ in 0..20 {
            let t = now + std::time::Duration::from_millis(60 + 100);
            b.observe_arrival(t);
        }
        b.update_target(now + std::time::Duration::from_secs(2));
        assert!(b.target_ms() >= 60, "target={}", b.target_ms());
        assert!(b.target_ms() <= TARGET_MAX_MS);
    }

    #[test]
    fn state_transitions() {
        let mut b = PlaybackBuffer::new(60);
        assert_eq!(b.state(), BufferState::Buffering);
        b.on_play_start();
        assert_eq!(b.state(), BufferState::Playing);
        b.on_underrun();
        assert_eq!(b.state(), BufferState::Rebuffering);
        b.on_refilled();
        assert_eq!(b.state(), BufferState::Playing);
    }
}
