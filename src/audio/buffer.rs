//! 播放缓冲目标深度控制：目标 = clamp(40ms + 4×jitter_EWMA, 40, 240)，增长即时、稳定后下降。

use std::time::Instant;

pub const INITIAL_TARGET_MS: u64 = 60;
pub const TARGET_MIN_MS: u64 = 40;
pub const TARGET_MAX_MS: u64 = 240;
/// 目标稳定多久后开始下降。
pub const SHRINK_STABLE_DURATION: std::time::Duration = std::time::Duration::from_secs(10);
/// 超过该帧数倍的到达间隙视为服务器句间停顿，不计入网络抖动。
const GAP_RESET_FRAMES: u64 = 3;

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
    /// 句间静默是服务器停顿而非网络抖动：超过 3 帧的间隙只重置基准，
    /// 否则每秒的 TTS 间隔都会把目标深度顶到上限，长期抬高对话延迟。
    pub fn observe_arrival(&mut self, now: Instant) {
        if let Some(prev) = self.last_arrival {
            let dt = now.duration_since(prev).as_secs_f64() * 1000.0;
            if dt > (self.frame_ms * GAP_RESET_FRAMES) as f64 {
                self.last_arrival = Some(now);
                return;
            }
            let dev = (dt - self.frame_ms as f64).abs();
            // 首次观测同样按 α=0.1 入 EMA：单次迟到帧不应立刻把目标深度顶到上限。
            self.jitter_ewma_ms = 0.9 * self.jitter_ewma_ms + 0.1 * dev;
        }
        self.last_arrival = Some(now);
    }

    /// 依据当前 jitter 重算目标深度：增长即时（保流畅），稳定后逐步回落到期望值（降延迟）。
    pub fn update_target(&mut self, now: Instant) {
        let desired = (TARGET_MIN_MS as f64 + 4.0 * self.jitter_ewma_ms) as u64;
        let desired = desired.clamp(TARGET_MIN_MS, TARGET_MAX_MS);
        if desired > self.target_ms {
            self.target_ms = desired;
            self.stable_since = None;
        } else if self.target_ms > desired {
            match self.stable_since {
                Some(since) if now.duration_since(since) >= SHRINK_STABLE_DURATION => {
                    self.target_ms = (self.target_ms - self.frame_ms).max(desired);
                    self.stable_since = Some(now);
                }
                None => self.stable_since = Some(now),
                _ => {}
            }
        } else {
            self.stable_since = None;
        }
    }

    pub fn target_ms(&self) -> u64 {
        self.target_ms
    }

    /// TTS 切换：只断开到达序列（避免句间停顿计入抖动）；
    /// 抖动估计、目标深度与稳定计时跨句保留，密集对话时目标才能按时回落。
    pub fn reset(&mut self) {
        self.last_arrival = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
            b.observe_arrival(now + Duration::from_millis(60 + 100));
        }
        b.update_target(now + Duration::from_secs(2));
        assert!(b.target_ms() >= 60, "target={}", b.target_ms());
        assert!(b.target_ms() <= TARGET_MAX_MS);
    }

    /// 回归：稳定期计时启动、抖动回落且稳定后，目标深度应下降。
    /// 注意：Instant 参数必须单调递增（duration_since 反向会 panic）。
    #[test]
    fn target_shrinks_after_stable_period() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        // 高抖动（间隔 120ms，仍低于句间停顿阈值）→ 目标深度增长到上限。
        for i in 0..20 {
            b.observe_arrival(now + Duration::from_millis(60 + 120 * i));
        }
        b.update_target(now + Duration::from_secs(5));
        let grown = b.target_ms();
        assert!(grown > 60, "target 未增长: {}", grown);
        // 首次"无需增长"启动稳定计时；随后抖动归零（严格 60ms）、
        // 稳定满 SHRINK_STABLE_DURATION：应下降至少一帧。
        for i in 0..20 {
            b.observe_arrival(now + Duration::from_millis(5060 + 60 * i));
        }
        b.update_target(now + Duration::from_secs(7));
        b.update_target(now + Duration::from_secs(38));
        assert!(
            b.target_ms() < grown,
            "稳定后 target 未收缩: {} -> {}",
            grown,
            b.target_ms()
        );
    }

    /// 句间静默（>3 帧）不是网络抖动：不得抬高目标深度。
    #[test]
    fn sentence_gaps_do_not_inflate_target() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        for i in 0..10 {
            b.observe_arrival(now + Duration::from_millis(60 * i));
        }
        // 句子结束，静默 1s，然后新句子恢复 60ms 节奏。
        b.observe_arrival(now + Duration::from_millis(1600));
        for i in 0..10 {
            b.observe_arrival(now + Duration::from_millis(1660 + 60 * i));
        }
        b.update_target(now + Duration::from_secs(3));
        assert_eq!(b.target_ms(), INITIAL_TARGET_MS, "句间停顿抬高了目标深度");
    }

    /// 稳定足够久后应回落到期望深度（最低 40ms），而不是永久停在首次增长值。
    #[test]
    fn target_shrinks_to_desired_after_stable() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        for i in 0..5 {
            b.observe_arrival(now + Duration::from_millis(60 + 90 * i));
        }
        b.update_target(now + Duration::from_secs(1));
        assert!(b.target_ms() > INITIAL_TARGET_MS);
        // 抖动归零（60 帧 EWMA 衰减到可忽略），此后每次 update 前进 10s。
        for i in 0..60 {
            b.observe_arrival(now + Duration::from_millis(1100 + 60 * i));
        }
        for step in 0..10u64 {
            b.update_target(now + Duration::from_secs(5 + 10 * (step + 1)));
        }
        assert_eq!(b.target_ms(), TARGET_MIN_MS, "未回落到最低目标深度");
    }

    /// 单次迟到帧（丢 1 帧）只轻微抬高目标，不能一击顶到上限。
    #[test]
    fn single_late_frame_does_not_max_target() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        b.observe_arrival(now);
        b.observe_arrival(now + Duration::from_millis(60));
        b.observe_arrival(now + Duration::from_millis(180)); // 120ms 间隔 = 丢 1 帧
        b.update_target(now + Duration::from_secs(1));
        assert!(
            b.target_ms() <= 100,
            "单次迟到帧把目标顶到 {}ms",
            b.target_ms()
        );
    }

    /// TTS 切换只断开到达序列，不应丢掉已学到的网络抖动估计。
    #[test]
    fn flush_preserves_network_jitter_estimate() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        for i in 0..20 {
            b.observe_arrival(now + Duration::from_millis(60 + 120 * i));
        }
        b.update_target(now + Duration::from_secs(5));
        let grown = b.target_ms();
        assert!(grown > 60, "高抖动下目标未增长: {}", grown);
        b.reset();
        b.update_target(now + Duration::from_secs(6));
        assert_eq!(b.target_ms(), grown, "TTS 切换重置了网络抖动估计");
    }

    /// TTS 切换不得重启稳定收缩计时，否则对话密集时目标永远不回落。
    #[test]
    fn flush_does_not_restart_shrink_timer() {
        let mut b = PlaybackBuffer::new(60);
        let now = Instant::now();
        for i in 0..20 {
            b.observe_arrival(now + Duration::from_millis(60 + 120 * i));
        }
        b.update_target(now + Duration::from_secs(5));
        assert!(b.target_ms() > 60, "高抖动下目标未增长");
        // 抖动归零，并启动稳定计时。
        for i in 0..20 {
            b.observe_arrival(now + Duration::from_millis(5100 + 60 * i));
        }
        b.update_target(now + Duration::from_secs(7));
        b.reset(); // TTS 切换
        b.update_target(now + Duration::from_secs(18)); // 距稳定起点 11s
        assert!(
            b.target_ms() < 240,
            "TTS 切换后目标未回落: {}",
            b.target_ms()
        );
    }
}
