//! VAD / 高通 / 音频调参：捕获侧语音活动检测与环境变量配置。

use log::debug;

use crate::audio::opus::FRAME_MS;

/// 播放期底噪学习暂停、尾随缩短的默认配置。
const PLAY_TAIL_MS: u64 = 300;

/// 一阶高通（≈80Hz）：滤除直流偏置与空调/风扇低频隆隆声，
/// 避免 VAD 被低频能量误触，同时让上行语音更干净（每样本 2 次乘加）。
pub(super) struct HighPass {
    alpha: f32,
    prev_in: f32,
    prev_out: f32,
}

impl HighPass {
    pub(super) fn new(sample_rate: u32) -> Self {
        // alpha = RC / (RC + dt)，截止频率 80Hz。
        let rc = 1.0 / (2.0 * std::f32::consts::PI * 80.0);
        let dt = 1.0 / sample_rate as f32;
        Self {
            alpha: rc / (rc + dt),
            prev_in: 0.0,
            prev_out: 0.0,
        }
    }

    pub(super) fn process(&mut self, frame: &mut [f32]) {
        for s in frame {
            let y = self.alpha * (self.prev_out + *s - self.prev_in);
            self.prev_in = *s;
            self.prev_out = y;
            *s = y;
        }
    }
}

/// 环境变量取值：解析失败或未设置回落默认值。
fn env_num<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<T>().ok())
        .unwrap_or(default)
}

/// dB → 线性幅度比。
fn db_to_lin(db: f32) -> f32 {
    10f32.powf(db / 20.0)
}

/// VAD 可调参数（环境变量覆盖，未设置回落默认）。
///
/// | 变量 | 默认 | 含义 |
/// |------|------|------|
/// | `XIAOZHI_VAD_ON_DB` | 8 | 触发阈值高于噪声底的 dB |
/// | `XIAOZHI_VAD_OFF_DB` | 4 | 释放阈值高于噪声底的 dB |
/// | `XIAOZHI_VAD_MIN_ON_DB` | -68 | 触发阈值绝对下限（dBFS） |
/// | `XIAOZHI_VAD_MIN_OFF_DB` | -74 | 释放阈值绝对下限（dBFS） |
/// | `XIAOZHI_VAD_ON_FRAMES` | 2 | 触发防抖帧数（×60ms） |
/// | `XIAOZHI_VAD_HANGOVER_MS` | 720 | 语音结束后仍发送的尾随时长 |
/// | `XIAOZHI_VAD_STRONG_DB` | -52 | 语音态内视为"强音"的门限；轻声说话老被钳制释放可调低 |
/// | `XIAOZHI_VAD_CLAMP_MS` | 2000 | 语音态无强音多久后强制释放（防噪声卡住） |
/// | `XIAOZHI_VAD_PLAY_BOOST_DB` | 12 | 播放期触发阈值额外抬升（0=关闭抑制） |
/// | `XIAOZHI_VAD_PLAY_TAIL_MS` | 300 | 播放活跃判定的尾随窗口（缓冲排空余量） |
pub(super) struct VadConfig {
    on_db: f32,
    off_db: f32,
    min_on_db: f32,
    min_off_db: f32,
    on_confirm_frames: u32,
    hangover_ms: u32,
    speech_min_db: f32,
    clamp_ms: u32,
    play_boost_db: f32,
}

impl VadConfig {
    pub(super) fn from_env() -> Self {
        Self {
            on_db: env_num("XIAOZHI_VAD_ON_DB", 8.0),
            off_db: env_num("XIAOZHI_VAD_OFF_DB", 4.0),
            min_on_db: env_num("XIAOZHI_VAD_MIN_ON_DB", -68.0),
            min_off_db: env_num("XIAOZHI_VAD_MIN_OFF_DB", -74.0),
            // 上限 8 帧：防抖窗口同时决定起始前导缓存长度，避免异常配置放大内存。
            on_confirm_frames: env_num("XIAOZHI_VAD_ON_FRAMES", 2u32).clamp(1, 8),
            hangover_ms: env_num("XIAOZHI_VAD_HANGOVER_MS", 720u32),
            speech_min_db: env_num("XIAOZHI_VAD_STRONG_DB", -52.0),
            clamp_ms: env_num("XIAOZHI_VAD_CLAMP_MS", 2000u32).max(FRAME_MS),
            play_boost_db: env_num("XIAOZHI_VAD_PLAY_BOOST_DB", 12.0),
        }
    }
}

/// 音频可调参数：环境变量在会话启动时一次性读取，捕获/播放两侧共享同一份。
pub(super) struct AudioTuning {
    pub(super) vad: VadConfig,
    /// 播放活跃尾随窗口（ms）：覆盖播放缓冲排空时间。
    pub(super) play_tail_ms: u64,
    /// Opus 编码复杂度（0-10）：越低越省 CPU，默认 10。
    pub(super) opus_complexity: i32,
}

impl AudioTuning {
    pub(super) fn from_env() -> Self {
        let vad = VadConfig::from_env();
        let play_tail_ms = env_num("XIAOZHI_VAD_PLAY_TAIL_MS", PLAY_TAIL_MS);
        let opus_complexity = env_num("XIAOZHI_OPUS_COMPLEXITY", 10i32).clamp(0, 10);
        debug!(
            "音频调参: VAD 触发=噪声底{:+.0}dB(下限{:.0}dBFS), 释放={:+.0}dB, 防抖{}帧, 尾随{}ms, 强音{:+.0}dBFS, 钳制{}ms, 播放期抑制{:+.0}dB/尾窗{}ms, Opus复杂度{}",
            vad.on_db,
            vad.min_on_db,
            vad.off_db,
            vad.on_confirm_frames,
            vad.hangover_ms,
            vad.speech_min_db,
            vad.clamp_ms,
            vad.play_boost_db,
            play_tail_ms,
            opus_complexity
        );
        Self {
            vad,
            play_tail_ms,
            opus_complexity,
        }
    }
}

/// 语音活动检测：静音帧不编码不上行，降低背景噪声对服务器 VAD/ASR 干扰。
/// 防误触设计：
/// 1. 触发防抖——连续 2 帧超阈值才进入语音（过滤点击/碰撞等 <120ms 瞬态噪声）；
///    防抖期候选帧由捕获管线暂存，确认后补发，不会吞掉首音节。
/// 2. 自适应噪声底——非语音段持续学习，持续底噪会把触发阈值推高到噪声之上。
/// 3. 能量钳制——语音态若连续 2s 无强音（默认 ≥ -52dBFS，可经环境变量调整）则判为噪声误触发，
///    立即释放并重估噪声底，避免环境声把客户端卡在"说话态"。
/// 4. 滞回 + hangover 0.72s——尾随防切词（宁多勿漏，避免吞字）。
/// 5. 播放期抑制——TTS 播放中抬高触发阈值、缩短 hangover、暂停底噪学习，
///    使旁人说话/扬声器回声不足以打断，只有近讲真人能插话。
pub(super) struct Vad {
    pub(super) speech: bool,
    /// 连续超阈值帧数（触发防抖计数）。
    on_count: u32,
    hangover: u32,
    /// 语音态内连续无强音的帧数（能量钳制计数）。
    low_speech_run: u32,
    /// 噪声底估计（EMA，非语音段更新）。
    pub(super) noise_floor: f32,
    /// 触发阈值缩放系数（= 10^(8dB/20)，预计算避免每帧 powf）。
    pub(super) on_scale: f32,
    /// 释放阈值缩放系数（= 10^(4dB/20)）。
    off_scale: f32,
    /// 语音结束后的尾随帧数（0.72s @ 60ms）。
    hangover_frames: u32,
    /// 进入语音态所需连续超阈帧数（120ms @ 60ms）。
    pub(super) on_confirm_frames: u32,
    /// 能量钳制：语音态连续超过该帧数仍无强音则释放（2s @ 60ms）。
    force_release_frames: u32,
    /// 视为"强音"的最小 RMS（默认 ≈ -52dBFS，可经 `XIAOZHI_VAD_STRONG_DB` 调整）。
    speech_min_rms: f32,
    /// 触发阈值绝对下限（≈ -68dBFS，噪声底收敛前保证不因阈值过低误触发）。
    pub(super) min_on: f32,
    /// 释放阈值绝对下限（≈ -74dBFS，语音尾音不被提前切掉）。
    min_off: f32,
    /// 播放期触发阈值倍数（= 10^(12dB/20)）。
    play_boost: f32,
    /// 播放期 hangover / 钳制窗口的缩减除数（尾音与误触发都更快释放）。
    play_div: u32,
    /// 本次释放是否由能量钳制触发（而非正常说完）：调用方据此决定是否上报句子结束。
    clamp_release: bool,
}

impl Vad {
    pub(super) fn with_config(cfg: &VadConfig) -> Self {
        Self {
            speech: false,
            on_count: 0,
            hangover: 0,
            low_speech_run: 0,
            // 初始噪声底取很低值（-80dBFS），使触发阈值从一开始就是
            // 绝对下限 min_on，启动后即使立刻说话也不会被吞。
            noise_floor: 0.0001,
            on_scale: db_to_lin(cfg.on_db),
            off_scale: db_to_lin(cfg.off_db),
            hangover_frames: (cfg.hangover_ms / FRAME_MS).max(1),
            on_confirm_frames: cfg.on_confirm_frames,
            force_release_frames: cfg.clamp_ms.div_ceil(FRAME_MS).max(1),
            speech_min_rms: db_to_lin(cfg.speech_min_db),
            min_on: db_to_lin(cfg.min_on_db),
            min_off: db_to_lin(cfg.min_off_db),
            play_boost: db_to_lin(cfg.play_boost_db),
            play_div: 2,
            clamp_release: false,
        }
    }

    /// 最近一次释放是否由能量钳制触发（噪声误触发纠正，非用户说完）。
    pub(super) fn released_by_clamp(&self) -> bool {
        self.clamp_release
    }

    /// 输入一帧 RMS 与播放态，返回是否应发送该帧（true=语音，false=静音跳过）。
    /// `playing` 为真表示 TTS 正在外放：此时抬阈值、缩尾随、停学底噪。
    pub(super) fn decide(&mut self, rms: f32, playing: bool) -> bool {
        self.clamp_release = false;
        let on = (self.noise_floor * self.on_scale).max(self.min_on)
            * if playing { self.play_boost } else { 1.0 };
        let off = (self.noise_floor * self.off_scale).max(self.min_off);
        let div = if playing { self.play_div } else { 1 };
        let release_at = (self.force_release_frames / div).max(1);
        let hang_at = (self.hangover_frames / div).max(1);
        if self.speech {
            // 能量钳制：长时无强音判为噪声误触发，释放并重估底噪。
            // 播放期已按抬高后的门限判入，同样门限即视为有效话音，
            // 否则用户插话超过 1s 会被误判为回声而截断句中。
            let active = rms >= self.speech_min_rms || (playing && rms >= on);
            if active {
                self.low_speech_run = 0;
            } else {
                self.low_speech_run += 1;
                if self.low_speech_run >= release_at {
                    self.speech = false;
                    self.hangover = 0;
                    self.low_speech_run = 0;
                    self.clamp_release = true;
                    // 播放期不重估：TTS 回声会污染底噪，导致停止播放后阈值虚高。
                    if !playing {
                        self.noise_floor = self.noise_floor * 0.5 + rms * 0.5;
                    }
                    return false;
                }
            }
            if rms < off {
                self.hangover += 1;
                if self.hangover >= hang_at {
                    self.speech = false;
                    self.hangover = 0;
                    self.low_speech_run = 0;
                }
            } else {
                self.hangover = 0;
            }
            return self.speech;
        }
        // 非语音段：触发需要连续 on_confirm_frames 帧超阈值（滤瞬态噪声）。
        if rms >= on {
            self.on_count += 1;
            if self.on_count >= self.on_confirm_frames {
                self.speech = true;
                self.on_count = 0;
                self.hangover = 0;
                self.low_speech_run = 0;
            }
        } else {
            self.on_count = 0;
        }
        // 非语音段更新噪声底：持续高电平环境噪声会把阈值推高，
        // 自适应解除误触发；待确认帧用慢速，避免语音开头污染底噪。
        // 播放期跳过学习，否则 TTS 回声会把阈值抬到正常语音之上。
        if !playing {
            let rate = if rms >= on { 0.02 } else { 0.05 };
            self.noise_floor = self.noise_floor * (1.0 - rate) + rms * rate;
        }
        self.speech
    }

    /// 是否处于触发防抖的待确认阶段（候选起始帧应保留而非丢弃）。
    pub(super) fn awaiting_onset(&self) -> bool {
        !self.speech && self.on_count > 0
    }
}

/// 测试用固定配置（不受环境变量影响，保证断言确定性）。
#[cfg(test)]
pub(super) fn test_config() -> VadConfig {
    VadConfig {
        on_db: 8.0,
        off_db: 4.0,
        min_on_db: -68.0,
        min_off_db: -74.0,
        on_confirm_frames: 2,
        hangover_ms: 720,
        speech_min_db: -52.0,
        clamp_ms: 2000,
        play_boost_db: 12.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::opus::{ENCODER_FRAME_SIZE, ENCODER_RATE};

    fn test_vad() -> Vad {
        Vad::with_config(&test_config())
    }

    /// 先建立底噪，再连续喂两帧同电平，返回是否被判定为语音。
    fn triggers(rms: f32, playing: bool) -> bool {
        let mut vad = test_vad();
        for _ in 0..20 {
            vad.decide(0.0002, playing); // -74dBFS 环境底噪
        }
        let first = vad.decide(rms, playing);
        let second = vad.decide(rms, playing);
        first || second
    }

    #[test]
    fn vad_triggers_on_speech_and_releases_after_silence() {
        let mut vad = test_vad();
        // 静音（-80dBFS）不触发。
        assert!(!vad.decide(0.0001, false));
        // 语音（-45dBFS）连续 2 帧触发（防抖）。
        assert!(!vad.decide(0.0056, false), "防抖第一帧不应立即触发");
        assert!(vad.decide(0.0056, false), "防抖第二帧应触发");
        // 词间停顿（-50dBFS，仍高于释放阈值）不释放。
        assert!(vad.decide(0.003, false));
        // 尾随：刚进入静音时仍发送（hangover 防切词）。
        assert!(vad.decide(0.00008, false));
        // 持续静音最终释放。
        let mut released = false;
        for _ in 0..40 {
            if !vad.decide(0.00008, false) {
                released = true;
                break;
            }
        }
        assert!(released, "持续静音未释放");
        // 释放后新语音再次触发（仍需防抖 2 帧）。
        assert!(!vad.decide(0.0056, false));
        assert!(vad.decide(0.0056, false));
    }

    #[test]
    fn vad_does_not_swallow_soft_speech_at_startup() {
        // 启动即说话（-55dBFS），阈值应从 min_on 起步，不应吞帧。
        let mut vad = test_vad();
        assert!(!vad.decide(0.0018, false), "防抖第一帧等待确认");
        assert!(vad.decide(0.0018, false), "启动即说话被 VAD 吞掉");
    }

    #[test]
    fn vad_ignores_single_frame_transients() {
        // 单帧高电平（键盘敲击/鼠标点击类瞬态）不应触发语音。
        let mut vad = test_vad();
        for _ in 0..20 {
            assert!(!vad.decide(0.0003, false)); // -70dBFS 环境静音，建立底噪
        }
        assert!(!vad.decide(0.01, false), "单帧瞬态噪声误触发");
        assert!(!vad.decide(0.0003, false), "瞬态后未回到静音");
    }

    #[test]
    fn vad_releases_when_stuck_on_low_level_noise() {
        // 持续 -60dBFS 环境噪声：可能短暂触发，但无强音应在钳制窗口内自动释放，
        // 且释放后噪声底抬高，底噪不再误触发。
        let mut vad = test_vad();
        let mut any_speech = false;
        for _ in 0..(vad.force_release_frames + 10) {
            any_speech |= vad.decide(0.001, false);
        }
        assert!(any_speech, "低电平噪声应短暂触发过（用于验证钳制）");
        for _ in 0..10 {
            assert!(!vad.decide(0.001, false), "钳制释放后底噪仍误触发");
        }
    }

    /// 播放期抑制：旁人说话（-60dBFS）不再打断，近讲强音（-44dBFS）仍可打断。
    #[test]
    fn vad_playback_boosts_threshold() {
        assert!(triggers(0.001, false), "非播放期中电平应触发");
        assert!(
            !triggers(0.001, true),
            "播放期 -60dBFS（旁人/回声）不应触发"
        );
        assert!(triggers(0.006, true), "播放期近讲强音应仍能打断");
    }

    /// 播放期尾随减半：静音后更快释放，避免 TTS 尾音被当成用户语音上传。
    #[test]
    fn vad_playback_shortens_hangover() {
        let hang = |playing: bool| {
            let mut vad = test_vad();
            assert!(!vad.decide(0.02, false), "防抖第一帧");
            assert!(vad.decide(0.02, false), "应进入语音态");
            let mut held = 0;
            while vad.decide(0.00008, playing) {
                held += 1;
                assert!(held < 40, "静音后未释放");
            }
            held
        };
        // hangover_frames=12（720ms/60ms）：非播放期额外保持 11 帧，播放期 5 帧。
        assert_eq!(hang(false), 11);
        assert_eq!(hang(true), 5);
    }

    /// 播放期暂停底噪学习：TTS 回声不得抬高噪声底（否则停止播放后灵敏度长期偏低）。
    #[test]
    fn vad_playback_freezes_noise_floor() {
        let mut vad = test_vad();
        let before = vad.noise_floor;
        for _ in 0..30 {
            vad.decide(0.001, true); // 播放期持续中电平
        }
        assert_eq!(vad.noise_floor, before, "播放期不应更新噪声底");
        // 对照：非播放期同样电平会正常抬升底噪。
        let mut vad = test_vad();
        for _ in 0..30 {
            vad.decide(0.001, false);
        }
        assert!(vad.noise_floor > before, "非播放期应继续学习底噪");
    }

    /// 播放期持续插话（-54dBFS，高于抬高后的门限但低于固定强音线）不被钳制误释放。
    #[test]
    fn vad_playback_holds_sustained_barge_in() {
        let mut vad = test_vad();
        assert!(!vad.decide(0.002, true), "防抖第一帧");
        assert!(vad.decide(0.002, true), "播放期近讲应触发");
        for i in 0..(vad.force_release_frames + 5) {
            assert!(vad.decide(0.002, true), "第 {} 帧插话被误释放", i);
        }
    }

    /// 高通：直流快速归零，语音频段基本无损。
    #[test]
    fn high_pass_removes_dc_and_keeps_voice_band() {
        let mut hp = HighPass::new(ENCODER_RATE);
        let mut dc = vec![0.5f32; ENCODER_FRAME_SIZE * 3];
        hp.process(&mut dc);
        let tail = &dc[ENCODER_FRAME_SIZE..];
        assert!(
            tail.iter().all(|s| s.abs() < 0.01),
            "直流未滤除: {}",
            tail[0]
        );

        let mut hp = HighPass::new(ENCODER_RATE);
        let mut sine: Vec<f32> = (0..ENCODER_FRAME_SIZE * 3)
            .map(|i| {
                let t = i as f32 / ENCODER_RATE as f32;
                (2.0 * std::f32::consts::PI * 1000.0 * t).sin() * 0.5
            })
            .collect();
        hp.process(&mut sine);
        let tail = &sine[ENCODER_FRAME_SIZE..];
        let rms = (tail.iter().map(|s| s * s).sum::<f32>() / tail.len() as f32).sqrt();
        assert!(rms > 0.3, "1kHz 语音频段被衰减: RMS={}", rms);
    }
}
