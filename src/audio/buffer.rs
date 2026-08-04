//! 播放缓冲目标深度控制：目标 = clamp(40ms + 4×jitter_EWMA, 40, 240)，增长即时、稳定后下降。

use std::time::Instant;

pub const INITIAL_TARGET_MS: u64 = 60;
pub const TARGET_MIN_MS: u64 = 40;
pub const TARGET_MAX_MS: u64 = 240;
/// 目标稳定多久后开始下降。
pub const SHRINK_STABLE_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

/// 播放缓冲控制器。仅做目标深度计算，不直接操作 ring buffer。
pub struct PlaybackBuffer {
    target_ms: u64,
    jitter_ewma_ms: f64,
    frame_ms: u64,
    last_arrival: Option<Instant>,
    stable_since: Option<Instant>,
}

impl PlaybackBuffer {
    pub fn new(frame_ms: u64) -> Self {
        Self {
            target_ms: INITIAL_TARGET_MS,
            jitter_ewma_ms: 0.0,
            frame_ms,
            last_arrival: None,
            stable_since: None,
        }
    }

    /// 记录一次帧到达（用于 jitter EWMA，α=0.1）。
    pub fn observe_arrival(&mut self, now: Instant) {
        if let Some(prev) = self.last_arrival {
            let dt = now.duration_since(prev).as_secs_f64() * 1000.0;
            let expected = self.frame_ms as f64;
            let dev = (dt - expected).abs();
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
            self.target_ms = desired;
            self.stable_since = None;
        } else if let Some(since) = self.stable_since
            && now.duration_since(since) >= SHRINK_STABLE_DURATION
        {
            // 稳定足够久，每次下降一帧（保留 desired 一帧余量，防止过度收缩）。
            let frame = self.frame_ms.max(1);
            if self.target_ms > TARGET_MIN_MS && self.target_ms >= desired + frame {
                self.target_ms -= frame;
                self.stable_since = Some(now);
            }
        } else if self.stable_since.is_none() {
            // 首次进入"无需增长"：开始稳定计时，满 SHRINK_STABLE_DURATION 后才允许收缩。
            self.stable_since = Some(now);
        }
    }

    pub fn target_ms(&self) -> u64 {
        self.target_ms
    }

    /// 清空（TTS 切换）：目标复位。
    pub fn reset(&mut self) {
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
    }

    #[test]
    fn target_clamps_and_grows_immediately() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        for _ in 0..20 {
            let t = now + std::time::Duration::from_millis(60 + 100);
            b.observe_arrival(t);
        }
        b.update_target(now + std::time::Duration::from_secs(2));
        assert!(b.target_ms() >= 60, "target={}", b.target_ms());
        assert!(b.target_ms() <= TARGET_MAX_MS);
    }

    /// 回归：稳定期计时启动后，抖动回落且稳定满 30s，目标深度应开始下降。
    /// 注意：所有 Instant 参数必须单调递增（duration_since 反向会 panic）。
    #[test]
    fn target_shrinks_after_stable_period() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        // 高抖动（间隔 60+200ms）使目标深度增长到上限。
        for i in 0..20 {
            let t = now + std::time::Duration::from_millis(60 + 260 * i);
            b.observe_arrival(t);
        }
        b.update_target(now + std::time::Duration::from_secs(5));
        let grown = b.target_ms();
        assert!(grown > 60, "target 未增长: {}", grown);
        // 增长后首次"无需增长"：启动稳定计时。
        b.update_target(now + std::time::Duration::from_secs(6));
        // 抖动归零（严格 60ms 间隔，首帧紧接上轮尾帧）、稳定满 30s：
        // EWMA 衰减使 desired 回落，应下降至少一帧。
        for i in 0..20 {
            let t = now + std::time::Duration::from_millis(5060 + 60 * i);
            b.observe_arrival(t);
        }
        b.update_target(now + std::time::Duration::from_secs(38));
        assert!(
            b.target_ms() < grown,
            "稳定 30s 后 target 未收缩: {} -> {}",
            grown,
            b.target_ms()
        );
    }
}
