//! 输出协同：播放活跃/插话闪避/清环信号，输出回调与欠载统计。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use cpal::traits::DeviceTrait;
use cpal::{Device, FromSample, SizedSample, Stream, StreamConfig};
use log::warn;
use rtrb::Consumer;

use crate::error::{Result, VoiceError};

/// 用户插话时的本地 TTS 闪避增益（约 -9dB）：让用户听清自己，同时削弱扬声器回声。
const DUCK_GAIN: f32 = 0.35;
/// 闪避建立时间（10ms）：打断要"立刻让路"，但不能产生爆音。
const DUCK_ATTACK_MS: f32 = 10.0;
/// 闪避恢复时间（250ms）：用户停口后平滑回到原音量。
const DUCK_RELEASE_MS: f32 = 250.0;
/// 用户插话闪避保持窗口：语音帧持续刷新；停口后 400ms 恢复 TTS 音量。
pub(super) const DUCK_HOLD_MS: u64 = 400;

/// 捕获 ↔ 播放 实时协同信号：单调时钟 + 播放活跃窗口 + 用户插话窗口 + 输出清环代号。
///
/// 全部为原子标量，CPAL 回调与 DSP worker 均可无锁访问；时间统一以共享
/// `Instant` 为基准，不受系统墙钟调整影响。
pub(super) struct DuplexSignal {
    epoch: Instant,
    /// 播放活跃截止（ms，自 epoch）；0 = 未播放。
    play_until_ms: AtomicU64,
    /// 用户插话闪避截止（ms）；语音帧持续刷新。
    duck_until_ms: AtomicU64,
    /// 输出清环代号：播放 worker 递增，输出回调发现变化即丢弃环内残留。
    flush_serial: AtomicU64,
    /// 清环时的累计写入样本数：只丢弃该序号之前的样本，新 TTS 首帧不会被误删。
    flush_target: AtomicU64,
}

impl DuplexSignal {
    pub(super) fn new() -> Self {
        Self {
            epoch: Instant::now(),
            play_until_ms: AtomicU64::new(0),
            duck_until_ms: AtomicU64::new(0),
            flush_serial: AtomicU64::new(0),
            flush_target: AtomicU64::new(0),
        }
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// 收到下行音频：把播放活跃窗口推进到 `now + tail_ms`。
    pub(super) fn touch_play(&self, tail_ms: u64) {
        self.play_until_ms
            .store(self.now_ms() + tail_ms, Ordering::Relaxed);
    }

    pub(super) fn stop_play(&self) {
        self.play_until_ms.store(0, Ordering::Relaxed);
    }

    pub(super) fn is_playing(&self) -> bool {
        self.now_ms() < self.play_until_ms.load(Ordering::Relaxed)
    }

    /// 用户插话：TTS 立即让路（输出回调按 10ms 包络闪避）。
    pub(super) fn mark_barge_in(&self, hold_ms: u64) {
        self.duck_until_ms
            .store(self.now_ms() + hold_ms, Ordering::Relaxed);
    }

    fn is_ducked(&self) -> bool {
        self.now_ms() < self.duck_until_ms.load(Ordering::Relaxed)
    }

    /// TTS 切换：请求输出回调丢弃 `written_total` 之前写入的残留音频。
    pub(super) fn request_flush(&self, written_total: u64) {
        // 先写目标再推进代号：回调看到新代号时目标已可见（Release/Acquire 配对）。
        self.flush_target.store(written_total, Ordering::Release);
        self.flush_serial.fetch_add(1, Ordering::Release);
    }

    pub(super) fn flush_serial(&self) -> u64 {
        self.flush_serial.load(Ordering::Acquire)
    }

    pub(super) fn flush_target(&self) -> u64 {
        self.flush_target.load(Ordering::Acquire)
    }
}

/// 输出欠载状态：断流时 5ms 斜坡淡出、恢复时 5ms 淡入（两侧都消爆音）。
///
/// 只做淡出会在"静音 → 有声"的边界留下阶跃（每次回复第一个字都"啪"一声），
/// 因此欠载期记录 `fade_in_left`，下一批真实样本按同一斜坡从 0 抬起。
struct Underrun {
    last_out: f32,
    ramp_left: usize,
    ramp_pending: bool,
    /// 淡入剩余样本数（>0 表示刚从静音恢复，输出需按斜坡抬起）。
    fade_in_left: usize,
    ramp_len: usize,
    /// 已从环中取出的样本总数（含清环丢弃），用于精确的清环边界。
    consumed: u64,
}

impl Underrun {
    fn new(ramp_len: usize) -> Self {
        Self {
            last_out: 0.0,
            ramp_left: 0,
            ramp_pending: true,
            fade_in_left: 0,
            ramp_len: ramp_len.max(1),
            consumed: 0,
        }
    }

    /// 清环（TTS 切换）：先按当前电平淡出再静音，避免"说到一半被切断"的爆音。
    /// `consumed` 是环的累计进度，跨清环保留。
    fn begin_silence(&mut self) {
        self.ramp_pending = false;
        self.ramp_left = if self.last_out.abs() > 1e-4 {
            self.ramp_len
        } else {
            0
        };
        self.fade_in_left = self.ramp_len;
    }

    /// 取一个单声道样本；ring 空时返回淡出/静音值，恢复后从 0 淡入。
    #[inline]
    fn next(&mut self, consumer: &mut Consumer<f32>) -> f32 {
        let Some(v) = consumer.pop().ok() else {
            if self.ramp_pending {
                self.ramp_pending = false;
                self.ramp_left = if self.last_out.abs() > 1e-4 {
                    self.ramp_len - 1
                } else {
                    0
                };
            }
            // 断流期间保持"下一批样本需淡入"的待办状态。
            self.fade_in_left = self.ramp_len;
            if self.ramp_left > 0 {
                self.ramp_left -= 1;
                // 以"最后一个真实样本"为基准线性衰减：不能把已衰减值再乘一次（会变成指数骤降）。
                return self.last_out * (self.ramp_left as f32 / self.ramp_len as f32);
            }
            self.last_out = 0.0;
            return 0.0;
        };
        self.ramp_pending = true;
        self.consumed += 1;
        if self.fade_in_left > 0 {
            self.fade_in_left -= 1;
            // 1/ramp_len → 1.0：首样本接近静音，5ms 内抬到满幅。
            let gain = 1.0 - self.fade_in_left as f32 / self.ramp_len as f32;
            let out = v * gain;
            self.last_out = out;
            return out;
        }
        self.last_out = v;
        v
    }
}

/// 输出音量包络：用户插话时快速闪避、停口后缓慢恢复，逐样本平滑（无 zipper 噪声）。
struct OutputGain {
    current: f32,
    attack: f32,
    release: f32,
}

impl OutputGain {
    fn new(sample_rate: u32) -> Self {
        let coef = |ms: f32| 1.0 - (-1.0 / (ms * sample_rate as f32 / 1000.0)).exp();
        Self {
            current: 1.0,
            attack: coef(DUCK_ATTACK_MS),
            release: coef(DUCK_RELEASE_MS),
        }
    }

    #[inline]
    fn next(&mut self, target: f32) -> f32 {
        let coef = if target < self.current {
            self.attack
        } else {
            self.release
        };
        self.current += (target - self.current) * coef;
        if (target - self.current).abs() < 1e-3 {
            self.current = target;
        }
        self.current
    }
}

/// 输出流构建参数（设备侧固定配置）。
pub(super) struct OutputTarget<'a> {
    pub(super) device: &'a Device,
    pub(super) config: StreamConfig,
    pub(super) channels: usize,
    pub(super) sample_rate: u32,
    pub(super) ramp_len: usize,
}

/// 输出回调共享信号（回调闭包与 DSP worker 各持一份 Arc）。
pub(super) struct OutputShared {
    pub(super) cb_count: Arc<AtomicU64>,
    pub(super) cb_samples: Arc<AtomicU64>,
    /// 输出欠载事件数（供播放诊断读取）。
    pub(super) underruns: Arc<AtomicU64>,
    pub(super) duplex: Arc<DuplexSignal>,
    pub(super) device_error: Arc<AtomicBool>,
}

/// 输出回调：rtrb 读单声道 → 插话闪避 → 声道展开 + 格式转换；欠载 5ms 斜坡静音。
pub(super) fn build_output_stream<T>(
    target: OutputTarget<'_>,
    mut consumer: Consumer<f32>,
    shared: OutputShared,
) -> Result<Stream>
where
    T: FromSample<f32> + SizedSample + Copy + Send + 'static,
{
    let OutputTarget {
        device,
        config,
        channels,
        sample_rate,
        ramp_len,
    } = target;
    let mut state = OutputState::new(
        sample_rate,
        ramp_len,
        &shared.duplex,
        shared.underruns.clone(),
    );
    let device_error = shared.device_error.clone();
    // cb_samples 由回调本地累积，批量提交原子（每累计 960 样本一次 fetch_add，
    // 约 10-20ms @96/48kHz，避免每样本原子操作）。
    let mut local_cb: u64 = 0;
    let stream = device
        .build_output_stream(
            config,
            move |data: &mut [T], _: &_| {
                let consumed = state.fill(
                    data,
                    channels,
                    &mut consumer,
                    &shared.duplex,
                    &shared.cb_count,
                );
                local_cb += consumed as u64;
                if local_cb >= 960 {
                    shared.cb_samples.fetch_add(local_cb, Ordering::Relaxed);
                    local_cb = 0;
                }
            },
            move |err| {
                warn!("输出流错误: {}", err);
                device_error.store(true, Ordering::Relaxed);
            },
            None,
        )
        .map_err(|e| VoiceError::Audio(format!("构建输出流失败: {}", e)))?;
    Ok(stream)
}

/// 输出回调状态（无锁、零分配、零日志）。
struct OutputState {
    underrun: Underrun,
    gain: OutputGain,
    /// 已处理的清环代号。
    flush_seen: u64,
    /// 欠载事件计数（回调 → 诊断）。
    underruns: Arc<AtomicU64>,
    /// 上一次回调是否已处于"取不满"状态，用于只计事件沿。
    starving: bool,
}

impl OutputState {
    fn new(
        sample_rate: u32,
        ramp_len: usize,
        duplex: &DuplexSignal,
        underruns: Arc<AtomicU64>,
    ) -> Self {
        Self {
            underrun: Underrun::new(ramp_len),
            gain: OutputGain::new(sample_rate),
            flush_seen: duplex.flush_serial(),
            underruns,
            starving: true,
        }
    }

    fn fill<T>(
        &mut self,
        data: &mut [T],
        channels: usize,
        consumer: &mut Consumer<f32>,
        duplex: &DuplexSignal,
        cb_count: &AtomicU64,
    ) -> usize
    where
        T: FromSample<f32> + Copy,
    {
        cb_count.fetch_add(1, Ordering::Relaxed);
        // TTS 切换：只丢弃 flush 之前写入的样本；新 TTS 首帧即使在回调观察前
        // 已入环也不会被误删（否则每次回复可能吞掉第一个字）。
        let serial = duplex.flush_serial();
        if serial != self.flush_seen {
            self.flush_seen = serial;
            let drop_n = duplex.flush_target().saturating_sub(self.underrun.consumed);
            for _ in 0..drop_n {
                if consumer.pop().is_ok() {
                    self.underrun.consumed += 1;
                } else {
                    break;
                }
            }
            self.underrun.begin_silence();
            // 清环后的静音是预期行为，不计为欠载。
            self.starving = true;
        }
        let duck = if duplex.is_ducked() { DUCK_GAIN } else { 1.0 };
        let readable = consumer.slots();
        let frames = data.len() / channels;
        // 取不满一整块 = 本次回调会触发欠载斜坡；只记"够→不够"的事件沿。
        let starving = readable < frames;
        if starving && !self.starving {
            self.underruns.fetch_add(1, Ordering::Relaxed);
        }
        self.starving = starving;
        let consumed_before = self.underrun.consumed;
        for i in 0..frames {
            let sample = self.underrun.next(consumer);
            let converted = T::from_sample_(sample * self.gain.next(duck));
            for c in 0..channels {
                data[i * channels + c] = converted;
            }
        }
        (self.underrun.consumed - consumed_before) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplex_windows_expire_and_flush_advances() {
        let d = DuplexSignal::new();
        assert!(!d.is_playing() && !d.is_ducked());
        d.touch_play(0); // now + 0 已到期
        assert!(!d.is_playing());
        d.touch_play(10_000);
        d.mark_barge_in(10_000);
        assert!(d.is_playing() && d.is_ducked());
        d.stop_play();
        assert!(!d.is_playing());
        let serial = d.flush_serial();
        d.request_flush(123);
        assert_eq!(d.flush_serial(), serial + 1);
        assert_eq!(d.flush_target(), 123);
    }

    /// 插话闪避包络：快速压低、缓慢恢复。
    #[test]
    fn output_gain_ducks_and_recovers() {
        let mut g = OutputGain::new(48_000);
        let mut min = 1.0f32;
        for _ in 0..4_800 {
            min = min.min(g.next(DUCK_GAIN));
        }
        assert!((min - DUCK_GAIN).abs() < 0.01, "闪避未到位: {}", min);
        let mut max = min;
        for _ in 0..120_000 {
            max = max.max(g.next(1.0));
        }
        assert!((max - 1.0).abs() < 0.01, "未恢复: {}", max);
    }

    /// 输出环清空：TTS 切换代号推进后，回调丢弃全部旧样本。
    #[test]
    fn output_flush_drains_stale_samples() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        for _ in 0..4096 {
            producer.push(0.5).unwrap();
        }
        let duplex = DuplexSignal::new();
        let cb = AtomicU64::new(0);
        let mut state = OutputState::new(48_000, 240, &duplex, Arc::new(AtomicU64::new(0)));
        let mut data = [0f32; 480];
        duplex.request_flush(4096);
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert_eq!(consumer.slots(), 0, "清环后仍有残留样本");
        assert!(data.iter().all(|s| *s == 0.0), "清环回调应输出静音");
    }

    /// 清环只删 flush 之前的样本：flush 后写入的新 TTS 首帧必须保留。
    #[test]
    fn flush_keeps_samples_written_after_it() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        for _ in 0..960 {
            producer.push(0.5).unwrap();
        }
        let duplex = DuplexSignal::new();
        let cb = AtomicU64::new(0);
        let mut state = OutputState::new(48_000, 240, &duplex, Arc::new(AtomicU64::new(0)));
        let mut data = [0f32; 480];
        // 先消费半帧旧音频。
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!((data[479] - 0.5).abs() < 1e-3);
        // flush 目标 = 已写入的 960 样本；之后写入的新音频（0.25）不能被清掉。
        duplex.request_flush(960);
        for _ in 0..480 {
            producer.push(0.25).unwrap();
        }
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!(
            (data[479] - 0.25).abs() < 1e-3,
            "flush 误删了新音频: {}",
            data[479]
        );
        assert_eq!(consumer.slots(), 0);
    }

    /// 插话闪避：输出增益在数十毫秒内压到 DUCK_GAIN 附近。
    #[test]
    fn output_fill_ducks_when_user_barges_in() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        for _ in 0..4096 {
            producer.push(0.5).unwrap();
        }
        let duplex = DuplexSignal::new();
        duplex.mark_barge_in(10_000);
        let cb = AtomicU64::new(0);
        let mut state = OutputState::new(48_000, 240, &duplex, Arc::new(AtomicU64::new(0)));
        let mut data = [0f32; 480];
        for _ in 0..5 {
            state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        }
        let tail = data[data.len() - 1];
        assert!(
            (tail - 0.5 * DUCK_GAIN).abs() < 0.02,
            "闪避未生效: {}",
            tail
        );
    }

    /// 断流后恢复必须淡入：首批样本从接近 0 抬起，5ms 内到满幅。
    /// 否则每次回复第一个字都会"啪"一声（0 → 满幅阶跃）。
    #[test]
    fn output_fades_in_after_underrun() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        let duplex = DuplexSignal::new();
        let cb = AtomicU64::new(0);
        let mut state = OutputState::new(48_000, 240, &duplex, Arc::new(AtomicU64::new(0)));
        let mut data = [1.0f32; 480];
        // 环空：整块静音，内部进入"待淡入"状态。
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!(data.iter().all(|s| *s == 0.0), "空环应输出静音");

        for _ in 0..480 {
            producer.push(1.0).unwrap();
        }
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!(data[0] < 0.02, "首样本未淡入: {}", data[0]);
        assert!(data[239] > 0.9, "淡入未在 5ms 内完成: {}", data[239]);
        assert!((data[479] - 1.0).abs() < 1e-3, "淡入后未到满幅");
    }

    /// 清环（TTS 被打断/切换）必须淡出：不能从满幅直接跳 0。
    #[test]
    fn output_fades_out_on_flush() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        for _ in 0..2048 {
            producer.push(1.0).unwrap();
        }
        let duplex = DuplexSignal::new();
        let cb = AtomicU64::new(0);
        let mut state = OutputState::new(48_000, 240, &duplex, Arc::new(AtomicU64::new(0)));
        let mut data = [0f32; 480];
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!((data[479] - 1.0).abs() < 1e-3, "前置播放异常");

        duplex.request_flush(2048);
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert!(data[0] > 0.9, "淡出起点应接近原电平: {}", data[0]);
        assert!(data[239].abs() < 0.02, "淡出未在 5ms 内完成: {}", data[239]);
        assert_eq!(data[479], 0.0, "淡出后应保持静音");
    }

    /// 环形缓冲不足一块时记一次欠载；连续空回调与清环静音不重复计数。
    #[test]
    fn output_counts_underrun_transitions() {
        let (mut producer, mut consumer) = rtrb::RingBuffer::new(8192);
        for _ in 0..480 {
            producer.push(0.5).unwrap();
        }
        let duplex = DuplexSignal::new();
        let cb = AtomicU64::new(0);
        let underruns = Arc::new(AtomicU64::new(0));
        let mut state = OutputState::new(48_000, 240, &duplex, underruns.clone());
        let mut data = [0f32; 480];
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert_eq!(underruns.load(Ordering::Relaxed), 0, "满缓冲不应计欠载");
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert_eq!(underruns.load(Ordering::Relaxed), 1, "首次欠载未计数");
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert_eq!(underruns.load(Ordering::Relaxed), 1, "连续欠载重复计数");
        duplex.request_flush(480);
        state.fill(&mut data, 1, &mut consumer, &duplex, &cb);
        assert_eq!(underruns.load(Ordering::Relaxed), 1, "清环静音被计为欠载");
    }
}
