//! 音频子系统：无锁实时管线。
//!
//! 架构（重构方案）：
//! - CPAL 回调只做固定成本的格式转换、声道合并/展开、rtrb 读写与状态变量，
//!   禁止锁、堆分配、重采样、Opus 与日志。
//! - 捕获与播放各有一个独立 DSP worker（Tokio 任务），持有 rtrb 的另一端：
//!   * 捕获 worker：设备率 → 16kHz（rubato Async）→ 60ms 帧 → Opus 编码 → latest-slot。
//!   * 播放 worker：接收 Opus → 解码（服务器采样率）→ 设备率重采样 → rtrb。
//! - 时钟漂移：捕获按"窗口内样本计数"、播放按"缓冲占用"调整重采样比率，
//!   每 500ms 一次，限幅 ±1000ppm、变化率 ≤50ppm/s。

pub mod buffer;
pub mod opus;
pub mod resample;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, FromSample, SampleFormat, SizedSample, Stream, StreamConfig};
use log::{debug, info, warn};
use rtrb::{Consumer, Producer};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{Result, VoiceError};
use crate::protocol::LatestSlot;

use buffer::PlaybackBuffer;
use opus::{NetworkGrade, OpusCodec, ENCODER_FRAME_SIZE};
use resample::AsyncResampler;

/// 漂移调整周期。
const DRIFT_PERIOD: Duration = Duration::from_millis(500);
/// 最大比率偏差（ppm）。
const MAX_DRIFT_PPM: f64 = 1000.0;
/// 比率变化率上限（ppm/s）。
const RATE_LIMIT_PPM_S: f64 = 50.0;
/// 输出环形缓冲容量（样本，单声道）：320ms @ 96kHz 余量。
const OUTPUT_RING_SAMPLES: usize = 32 * 1024;
/// 输入环形缓冲容量（样本，单声道）：约 1s @ 48kHz，
/// 吸收 WASAPI 采集回调的突发（burst）写入，避免 rtrb 满导致丢样本。
const INPUT_RING_SAMPLES: usize = 48 * 1024;
/// 捕获批次上限（样本）：一次循环尽量读空 rtrb，防 burst 丢样本。
const CAPTURE_BATCH: usize = 4096;

/// 下行到播放 worker 的消息。
pub enum PlaybackMsg {
    /// Opus 音频帧。
    Audio(Vec<u8>),
    /// 清空播放缓冲（TTS 切换）。
    Flush,
    /// 关闭。
    Shutdown,
}

/// 音频管理器：持有设备、流与 DSP worker。
pub struct AudioManager {
    input_device: Device,
    output_device: Device,
    input_stream: Option<Stream>,
    output_stream: Option<Stream>,
    workers: Vec<JoinHandle<()>>,
    playback_tx: Option<mpsc::Sender<PlaybackMsg>>,
    grade_tx: Option<mpsc::Sender<NetworkGrade>>,
}

impl AudioManager {
    /// 探测默认设备。
    pub fn new() -> Result<Self> {
        let host = cpal::default_host();
        let input_device = host
            .default_input_device()
            .ok_or_else(|| VoiceError::Audio("未找到输入设备".into()))?;
        let output_device = host
            .default_output_device()
            .ok_or_else(|| VoiceError::Audio("未找到输出设备".into()))?;
        info!("输入设备: {}", input_device);
        info!("输出设备: {}", output_device);
        Ok(Self {
            input_device,
            output_device,
            input_stream: None,
            output_stream: None,
            workers: Vec::new(),
            playback_tx: None,
            grade_tx: None,
        })
    }

    /// 启动音频管线：建流 + 起 DSP worker。
    /// `server_rate` 为服务器下行采样率（解码器按此创建）。
    pub async fn start(&mut self, server_rate: u32, encoded_tx: LatestSlot<Vec<u8>>) -> Result<()> {
        self.stop();

        let input_cfg = self
            .input_device
            .default_input_config()
            .map_err(|e| VoiceError::Audio(format!("获取输入配置失败: {}", e)))?;
        let input_rate = input_cfg.sample_rate();
        let input_channels = input_cfg.channels() as usize;
        let input_format = input_cfg.sample_format();

        let output_cfg = self
            .output_device
            .default_output_config()
            .map_err(|e| VoiceError::Audio(format!("获取输出配置失败: {}", e)))?;
        let output_rate = output_cfg.sample_rate();
        let output_channels = output_cfg.channels() as usize;
        let output_format = output_cfg.sample_format();

        info!(
            "音频管线: 输入 {}Hz/{}ch → 16kHz 编码; 下行 {}Hz → 输出 {}Hz/{}ch",
            input_rate, input_channels, server_rate, output_rate, output_channels
        );

        let (in_producer, in_consumer) = rtrb::RingBuffer::new(INPUT_RING_SAMPLES);
        let (out_producer, out_consumer) = rtrb::RingBuffer::new(OUTPUT_RING_SAMPLES);

        let input_stream = match input_format {
            SampleFormat::I16 => build_input_stream::<i16>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: i16| s as f32 / 32768.0,
                in_producer,
            )?,
            SampleFormat::F32 => build_input_stream::<f32>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: f32| s,
                in_producer,
            )?,
            other => return Err(VoiceError::Audio(format!("不支持的输入格式: {:?}", other))),
        };
        let ramp_len = (output_rate as usize * 5) / 1000;
        let cb_count = Arc::new(AtomicU64::new(0));
        let cb_samples = Arc::new(AtomicU64::new(0));
        let cb_count_out = cb_count.clone();
        let cb_samples_out = cb_samples.clone();
        let output_stream = match output_format {
            SampleFormat::I16 => build_output_stream::<i16>(
                &self.output_device,
                output_cfg.into(),
                output_channels,
                out_consumer,
                ramp_len,
                cb_count,
                cb_samples,
            )?,
            SampleFormat::F32 => build_output_stream::<f32>(
                &self.output_device,
                output_cfg.into(),
                output_channels,
                out_consumer,
                ramp_len,
                cb_count,
                cb_samples,
            )?,
            SampleFormat::U16 => build_output_stream::<u16>(
                &self.output_device,
                output_cfg.into(),
                output_channels,
                out_consumer,
                ramp_len,
                cb_count,
                cb_samples,
            )?,
            other => return Err(VoiceError::Audio(format!("不支持的输出格式: {:?}", other))),
        };
        input_stream
            .play()
            .map_err(|e| VoiceError::Audio(format!("启动采集失败: {}", e)))?;
        output_stream
            .play()
            .map_err(|e| VoiceError::Audio(format!("启动播放失败: {}", e)))?;

        let (playback_tx, playback_rx) = mpsc::channel::<PlaybackMsg>(32);
        let (grade_tx, grade_rx) = mpsc::channel::<NetworkGrade>(4);
        self.workers.push(tokio::spawn(capture_worker(
            in_consumer,
            encoded_tx,
            grade_rx,
            input_rate,
        )));
        self.workers.push(tokio::spawn(playback_worker(
            playback_rx,
            out_producer,
            server_rate,
            output_rate,
            cb_count_out,
            cb_samples_out,
        )));

        self.input_stream = Some(input_stream);
        self.output_stream = Some(output_stream);
        self.playback_tx = Some(playback_tx);
        self.grade_tx = Some(grade_tx);
        Ok(())
    }

    /// 停止流并终止 worker。
    pub fn stop(&mut self) {
        self.input_stream = None;
        self.output_stream = None;
        for w in self.workers.drain(..) {
            w.abort();
        }
        self.playback_tx = None;
        self.grade_tx = None;
    }

    /// 下行发送端。
    pub fn playback_sender(&self) -> Option<mpsc::Sender<PlaybackMsg>> {
        self.playback_tx.clone()
    }

    /// 网络分级控制端（更新 Opus 编码策略）。
    pub fn grade_sender(&self) -> Option<mpsc::Sender<NetworkGrade>> {
        self.grade_tx.clone()
    }
}

/// 输入回调：格式转换 + 下混单声道 + rtrb 入队（满则丢）。
fn build_input_stream<S>(
    device: &Device,
    config: StreamConfig,
    channels: usize,
    to_f32: fn(S) -> f32,
    mut producer: Producer<f32>,
) -> Result<Stream>
where
    S: SizedSample + Copy + Send + 'static,
{
        let stream = device
        .build_input_stream(
            config,
            move |data: &[S], _: &_| {
                let frames = data.len() / channels;
                for f in 0..frames {
                    let mut sum = 0f32;
                    for c in 0..channels {
                        sum += to_f32(data[f * channels + c]);
                    }
                    let mono = sum * (1.0 / channels as f32);
                    let _ = producer.push(mono);
                }
            },
            |err| warn!("输入流错误: {}", err),
            None,
        )
        .map_err(|e| VoiceError::Audio(format!("构建输入流失败: {}", e)))?;
    Ok(stream)
}

/// 输出回调：rtrb 读单声道 → 声道展开 + 格式转换；欠载 5ms 斜坡静音。
/// 点击抑制状态为回调闭包内的局部可变量（FnMut），无需锁/原子。
#[allow(clippy::too_many_arguments)]
fn build_output_stream<T>(
    device: &Device,
    config: StreamConfig,
    channels: usize,
    mut consumer: Consumer<f32>,
    ramp_len: usize,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
) -> Result<Stream>
where
    T: FromSample<f32> + SizedSample + Copy + Send + 'static,
{
    let mut last_out: f32 = 0.0;
    let mut ramp_left: usize = 0;
    let mut ramp_pending: bool = true;
    let stream = device
        .build_output_stream(
            config,
            move |data: &mut [T], _: &_| {
                fill_output(
                    data,
                    channels,
                    &mut consumer,
                    &mut last_out,
                    &mut ramp_left,
                    &mut ramp_pending,
                    ramp_len,
                    &cb_count,
                    &cb_samples,
                );
            },
            |err| warn!("输出流错误: {}", err),
            None,
        )
        .map_err(|e| VoiceError::Audio(format!("构建输出流失败: {}", e)))?;
    Ok(stream)
}

/// 输出回调体（无锁、零分配、零日志）。
#[allow(clippy::too_many_arguments)]
fn fill_output<T>(
    data: &mut [T],
    channels: usize,
    consumer: &mut Consumer<f32>,
    last_out: &mut f32,
    ramp_left: &mut usize,
    ramp_pending: &mut bool,
    ramp_len: usize,
    cb_count: &AtomicU64,
    cb_samples: &AtomicU64,
) where
    T: FromSample<f32> + Copy,
{
    cb_count.fetch_add(1, Ordering::Relaxed);
    let frames = data.len() / channels;
    let ramp_len = ramp_len.max(1);
    for i in 0..frames {
        let sample = match consumer.pop().ok() {
            Some(v) => {
                cb_samples.fetch_add(1, Ordering::Relaxed);
                *last_out = v;
                *ramp_pending = true;
                v
            }
            None => {
                if *ramp_pending {
                    *ramp_pending = false;
                    if last_out.abs() > 1e-4 {
                        *ramp_left = ramp_len - 1;
                    }
                }
                if *ramp_left > 0 {
                    *ramp_left -= 1;
                    *last_out * (*ramp_left as f32 / ramp_len as f32)
                } else {
                    0.0
                }
            }
        };
        let converted = T::from_sample_(sample);
        for c in 0..channels {
            data[i * channels + c] = converted;
        }
    }
}

/// 捕获统计（诊断用）：RMS 电平与编码帧数，每 2s 打日志，
/// 用于确认麦克风数据是否真实流入（排除 Windows 麦克风隐私/静音问题）。
struct CaptureStats {
    flush_at: Instant,
    samples: u64,
    sum_sq: f64,
    frames: u64,
    /// 语音帧数（VAD 判定后发送）。
    speech_frames: u64,
    /// 静音帧数（VAD 判定后跳过）。
    silence_frames: u64,
    /// 最近一次 AGC 增益（线性）。
    agc_gain: f32,
}

impl CaptureStats {
    fn new() -> Self {
        Self {
            flush_at: Instant::now(),
            samples: 0,
            sum_sq: 0.0,
            frames: 0,
            speech_frames: 0,
            silence_frames: 0,
            agc_gain: 1.0,
        }
    }
}

/// 播放统计（诊断用）：确认下行音频是否被正确解码并写入环形缓冲。
struct PlaybackStats {
    flush_at: Instant,
    received: u64,
    decoded_ok: u64,
    decoded_fail: u64,
    plc: u64,
    written: u64,
    /// 最近一帧解码输出样本数。
    last_pcm_len: usize,
    /// 最近一次重采样 input_frames_next()。
    last_needed: usize,
    /// 最近一次重采样产出样本数。
    last_produced: usize,
    /// 重采样无产出的帧数（累计）。
    zero_produced: u64,
}

impl PlaybackStats {
    fn new() -> Self {
        Self {
            flush_at: Instant::now(),
            received: 0,
            decoded_ok: 0,
            decoded_fail: 0,
            plc: 0,
            written: 0,
            last_pcm_len: 0,
            last_needed: 0,
            last_produced: 0,
            zero_produced: 0,
        }
    }
}

/// 漂移控制器：目标比率随观测漂移调整，限幅 ±1000ppm、变化率 ≤50ppm/s。
struct DriftController {
    nominal_ratio: f64,
    ratio: f64,
    last_adjust: Instant,
}

impl DriftController {
    fn new(nominal_ratio: f64) -> Self {
        Self {
            nominal_ratio,
            ratio: nominal_ratio,
            last_adjust: Instant::now(),
        }
    }

    fn current_ppm(&self) -> f64 {
        (self.ratio - self.nominal_ratio) / self.nominal_ratio * 1e6
    }

    /// 朝目标偏移（ppm）移动，遵守速率限制与限幅。
    fn move_toward(&mut self, target_ppm: f64) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_adjust).as_secs_f64();
        self.last_adjust = now;
        let max_step = RATE_LIMIT_PPM_S * dt.max(1e-3);
        let cur = self.current_ppm();
        let step = (target_ppm - cur).clamp(-max_step, max_step);
        let new_ppm = (cur + step).clamp(-MAX_DRIFT_PPM, MAX_DRIFT_PPM);
        self.ratio = self.nominal_ratio * (1.0 + new_ppm / 1e6);
    }
}

/// 帧统计（RMS 与峰值）。
fn frame_stats(frame: &[f32]) -> (f32, f32) {
    if frame.is_empty() {
        return (0.0, 0.0);
    }
    let mut sum_sq = 0.0f64;
    let mut peak = 0.0f32;
    for &s in frame.iter() {
        sum_sq += (s as f64) * (s as f64);
        peak = peak.max(s.abs());
    }
    let rms = (sum_sq / frame.len() as f64).sqrt() as f32;
    (rms, peak)
}

/// 上行自动增益控制（AGC）。
///
/// 目的：用户语音电平偏低导致服务器 ASR/VAD 难以触发。本 AGC 把语音帧
/// RMS 提升到目标电平（约 -21dBFS，上限 36dB），静音帧不放大，增益平滑
/// 更新，并按帧峰值限幅防止过载削波。
struct Agc {
    /// 当前增益（线性）。
    gain: f32,
    /// 视为语音的最小 RMS（≈ -66dBFS）。
    voice_threshold: f32,
    /// 语音帧目标 RMS（≈ -21dBFS）。
    target_rms: f32,
    /// 最大增益（线性，36dB）。
    max_gain: f32,
}

impl Agc {
    fn new() -> Self {
        Self {
            gain: 1.0,
            voice_threshold: 0.0005,
            target_rms: 0.09,
            max_gain: 64.0,
        }
    }

    /// 对一帧（60ms，960 样本）做 AGC。
    fn process_frame(&mut self, frame: &mut [f32]) {
        if frame.is_empty() {
            return;
        }
        let (rms, peak) = frame_stats(frame);
        if rms >= self.voice_threshold {
            let mut target = (self.target_rms / rms).clamp(1.0, self.max_gain);
            // 峰值防削波：增益上限不超过 0.85/peak，避免过载失真。
            if peak > 0.0 {
                target = target.min(0.85 / peak);
            }
            // 升增益快、降增益慢，避免抽泣声/爆音。
            let alpha = if target > self.gain { 0.5 } else { 0.2 };
            self.gain += (target - self.gain) * alpha;
            let g = self.gain;
            for s in frame.iter_mut() {
                *s = (*s * g).clamp(-0.98, 0.98);
            }
        } else {
            // 静音帧：不放大（底噪不被抬高），内部增益缓慢回落待命。
            self.gain += (1.0 - self.gain) * 0.1;
        }
    }
}

/// 语音活动检测（静音抑制）。
///
/// 静音帧不编码、不上行，只发送语音帧，降低背景噪声对服务器 VAD/ASR 的
/// 干扰（服务器端 VAD 阈值不会因持续噪声被抬升）。自适应噪声底 + 滞回 +
/// 尾随缓冲（hangover 1.2s），参数取保守方向（宁多勿漏，避免吞字）。
struct Vad {
    /// 当前是否处于语音段。
    speech: bool,
    /// 已连续低于释放阈值的帧数。
    hangover: u32,
    /// 噪声底估计（EMA，仅静音段更新）。
    noise_floor: f32,
    /// 相对噪声底的触发阈值（dB）。
    on_db: f32,
    /// 相对噪声底的释放阈值（dB）。
    off_db: f32,
    /// 语音结束后的尾随帧数（1.2s @ 60ms）。
    hangover_frames: u32,
    /// 触发绝对下限（≈ -68dBFS）。
    min_on: f32,
    /// 释放绝对下限（≈ -74dBFS）。
    min_off: f32,
}

impl Vad {
    fn new() -> Self {
        Self {
            speech: false,
            hangover: 0,
            // 初始噪声底取很低值（-80dBFS），使触发阈值从一开始就是
            // 绝对下限 min_on，启动后即使立刻说话也不会被吞。
            noise_floor: 0.0001,
            on_db: 8.0,
            off_db: 4.0,
            hangover_frames: 20,
            min_on: 0.0004,
            min_off: 0.0002,
        }
    }

    /// 输入一帧 RMS，返回是否应发送该帧（true=语音，false=静音跳过）。
    fn decide(&mut self, rms: f32) -> bool {
        let on = (self.noise_floor * 10f32.powf(self.on_db / 20.0)).max(self.min_on);
        let off = (self.noise_floor * 10f32.powf(self.off_db / 20.0)).max(self.min_off);
        if self.speech {
            if rms < off {
                self.hangover += 1;
                if self.hangover >= self.hangover_frames {
                    self.speech = false;
                    self.hangover = 0;
                }
            } else {
                self.hangover = 0;
            }
        } else {
            // 静音段：慢跟噪声底（仅低电平观测进入，避免语音污染）。
            if rms < on {
                self.noise_floor = self.noise_floor * 0.98 + rms * 0.02;
            }
            if rms >= on {
                self.speech = true;
                self.hangover = 0;
            }
        }
        self.speech
    }
}

/// 捕获 DSP worker：设备率 → 16kHz → 60ms 帧 → AGC → Opus 编码 → latest-slot。
async fn capture_worker(
    mut input: Consumer<f32>,
    encoded_tx: LatestSlot<Vec<u8>>,
    mut grade_rx: mpsc::Receiver<NetworkGrade>,
    device_rate: u32,
) {
    let nominal = 16_000.0 / device_rate as f64;
    let mut resampler = match AsyncResampler::new(nominal, 160) {
        Ok(r) => r,
        Err(e) => {
            warn!("捕获重采样器创建失败: {}", e);
            return;
        }
    };
    let mut opus = match OpusCodec::new(16_000) {
        Ok(o) => o,
        Err(e) => {
            warn!("捕获 Opus 编码器创建失败: {}", e);
            return;
        }
    };
    opus.set_complexity(10);

    let mut pending: Vec<f32> = Vec::with_capacity(ENCODER_FRAME_SIZE * 2);
    let mut drift = DriftController::new(nominal);
    let mut window_count: u64 = 0;
    let mut window_start = Instant::now();
    let mut stats = CaptureStats::new();
    let mut agc = Agc::new();
    let mut vad = Vad::new();

    loop {
        tokio::select! {
            grade = grade_rx.recv() => {
                if let Some(g) = grade {
                    opus.set_network_grade(g);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(2)) => {
                process_capture_batch(
                    &mut input,
                    &mut resampler,
                    &mut opus,
                    &mut pending,
                    &mut drift,
                    &mut window_count,
                    &mut window_start,
                    &mut stats,
                    &mut agc,
                    &mut vad,
                    &encoded_tx,
                ).await;
            }
        }
    }
}

/// 捕获批次处理（非阻塞）：读取输入 → 重采样 → 漂移校正 → 组帧编码。
#[allow(clippy::too_many_arguments)]
async fn process_capture_batch(
    input: &mut Consumer<f32>,
    resampler: &mut AsyncResampler,
    opus: &mut OpusCodec,
    pending: &mut Vec<f32>,
    drift: &mut DriftController,
    window_count: &mut u64,
    window_start: &mut Instant,
    stats: &mut CaptureStats,
    agc: &mut Agc,
    vad: &mut Vad,
    encoded_tx: &LatestSlot<Vec<u8>>,
) {
    let mut chunk: Vec<f32> = Vec::with_capacity(CAPTURE_BATCH);
    let mut any = false;
    while let Ok(s) = input.pop() {
        chunk.push(s);
        any = true;
        if chunk.len() >= CAPTURE_BATCH {
            break;
        }
    }
    if !any {
        return;
    }
    *window_count += chunk.len() as u64;
    stats.samples += chunk.len() as u64;
    stats.sum_sq += chunk.iter().map(|s| (*s as f64) * (*s as f64)).sum::<f64>();

    let mut out = Vec::new();
    resampler.process(&chunk, drift.ratio, &mut out);
    pending.extend(out);

    // 每 500ms：按窗口样本计数校正比率。
    if window_start.elapsed() >= DRIFT_PERIOD {
        let observed = *window_count as f64 / window_start.elapsed().as_secs_f64();
        if observed > 0.0 {
            let target_ratio = 16_000.0 / observed;
            let target_ppm = (target_ratio - drift.nominal_ratio) / drift.nominal_ratio * 1e6;
            drift.move_toward(target_ppm);
        }
        *window_count = 0;
        *window_start = Instant::now();
    }

    // 组 60ms 帧：VAD 判定语音 → AGC 提升电平；静音 → 清零发送（保持上行
    // 连续，服务器 VAD 收到干净静音，不受背景噪声干扰）。
    while pending.len() >= ENCODER_FRAME_SIZE {
        let mut frame: Vec<f32> = pending.drain(..ENCODER_FRAME_SIZE).collect();
        let (rms, _) = frame_stats(&frame);
        if vad.decide(rms) {
            stats.speech_frames += 1;
            agc.process_frame(&mut frame);
            stats.agc_gain = agc.gain;
        } else {
            stats.silence_frames += 1;
            frame.fill(0.0);
        }
        match opus.encode(&frame) {
            Ok(pkt) => {
                stats.frames += 1;
                encoded_tx.store(pkt).await;
            }
            Err(e) => warn!("Opus 编码失败: {}", e),
        }
    }

    // 每 2s 输出捕获诊断（RUST_LOG=debug 时可见）。
    if stats.flush_at.elapsed() >= Duration::from_secs(2) {
        let rms = if stats.samples > 0 {
            (stats.sum_sq / stats.samples as f64).sqrt()
        } else {
            0.0
        };
        let db = if rms > 1e-9 { 20.0 * rms.log10() } else { -120.0 };
        debug!(
            "捕获诊断: RMS={:.1}dBFS, 输入样本={}, 编码帧={} (语音{} 静音{}), AGC增益={:.1}x",
            db,
            stats.samples,
            stats.frames,
            stats.speech_frames,
            stats.silence_frames,
            stats.agc_gain
        );
        stats.samples = 0;
        stats.sum_sq = 0.0;
        stats.frames = 0;
        stats.speech_frames = 0;
        stats.silence_frames = 0;
        stats.agc_gain = 1.0;
        stats.flush_at = Instant::now();
    }
}

/// 播放 DSP worker：Opus → 解码（服务器率）→ 设备率 → rtrb。
async fn playback_worker(
    mut rx: mpsc::Receiver<PlaybackMsg>,
    mut out: Producer<f32>,
    server_rate: u32,
    output_rate: u32,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
) {
    let nominal = output_rate as f64 / server_rate as f64;
    let mut resampler = match AsyncResampler::new(nominal, 960) {
        Ok(r) => r,
        Err(e) => {
            warn!("播放重采样器创建失败: {}", e);
            return;
        }
    };
    let mut decoder = match OpusCodec::new(server_rate) {
        Ok(d) => d,
        Err(e) => {
            warn!("播放 Opus 解码器创建失败: {}", e);
            return;
        }
    };
    let mut buf = PlaybackBuffer::new(60);
    let mut drift = DriftController::new(nominal);
    let mut last_drift = Instant::now();
    let mut stats = PlaybackStats::new();

    loop {
        match rx.recv().await {
            Some(PlaybackMsg::Audio(opus)) => {
                stats.received += 1;
                buf.observe_arrival(Instant::now());
                let (pcm, used_plc) = match decoder.decode(&opus, false) {
                    Ok(p) => (p, false),
                    Err(e) => {
                        stats.decoded_fail += 1;
                        debug!("Opus 解码失败(len={}): {}", opus.len(), e);
                        match decoder.decode(&[], false) {
                            Ok(p) => {
                                stats.plc += 1;
                                (p, true)
                            }
                            Err(e2) => {
                                warn!("Opus 解码与 PLC 均失败: {}", e2);
                                continue;
                            }
                        }
                    }
                };
                if !used_plc {
                    stats.decoded_ok += 1;
                }
                stats.last_pcm_len = pcm.len();
                stats.last_needed = resampler.input_frames_next();
                let mut out_samples = Vec::new();
                resampler.process(&pcm, drift.ratio, &mut out_samples);
                stats.last_produced = out_samples.len();
                if out_samples.is_empty() {
                    stats.zero_produced += 1;
                }
                let before = out.slots();
                write_samples(
                    &mut out,
                    &out_samples,
                    output_rate,
                    &mut buf,
                    OUTPUT_RING_SAMPLES,
                );
                stats.written += before.saturating_sub(out.slots()) as u64;
                buf.on_play_start();
            }
            Some(PlaybackMsg::Flush) => buf.reset(),
            Some(PlaybackMsg::Shutdown) | None => break,
        }

        if stats.flush_at.elapsed() >= Duration::from_secs(2) {
            let cb = cb_count.swap(0, Ordering::Relaxed);
            let cbs = cb_samples.swap(0, Ordering::Relaxed);
            debug!(
                "播放诊断: 收帧={}, 解码成功={}, 失败={}, PLC={}, 写入={}, pcm={}, needed={}, 产出={}, 零产出={} | 回调={}次, 消费样本={}",
                stats.received,
                stats.decoded_ok,
                stats.decoded_fail,
                stats.plc,
                stats.written,
                stats.last_pcm_len,
                stats.last_needed,
                stats.last_produced,
                stats.zero_produced,
                cb,
                cbs
            );
            stats = PlaybackStats::new();
        }

        if last_drift.elapsed() >= DRIFT_PERIOD {
            buf.update_target(Instant::now());
            // 缓冲占用校正：占用高于目标 → 降低比率（放慢产出），反之升高。
            let free = out.slots() as f64;
            let cap = OUTPUT_RING_SAMPLES as f64;
            let occupancy_ms = ((cap - free) / output_rate as f64) * 1000.0;
            let target_ms = buf.target_ms() as f64;
            if target_ms > 0.0 {
                let err_ppm = ((occupancy_ms - target_ms) / target_ms) * 1e6 * 0.5;
                drift.move_toward(err_ppm.clamp(-MAX_DRIFT_PPM, MAX_DRIFT_PPM));
            }
            last_drift = Instant::now();
        }
    }
}

/// 写入播放环形缓冲；空间不足时丢弃多余（新）样本以封顶延迟。
fn write_samples(
    out: &mut Producer<f32>,
    samples: &[f32],
    output_rate: u32,
    buf: &mut PlaybackBuffer,
    capacity: usize,
) {
    let free = out.slots();
    let occupancy_ms = ((capacity - free) as f64 / output_rate as f64) * 1000.0;
    // 硬同步：占用超 280ms 时本轮整体丢弃，让缓冲自然回落。
    if occupancy_ms > 280.0 {
        return;
    }
    let n = samples.len().min(free);
    let mut written = 0;
    while written < n {
        match out.write_chunk(n - written) {
            Ok(mut chunk) => {
                let (a, b) = chunk.as_mut_slices();
                let (src_a, rest) = samples[written..].split_at(a.len());
                a.copy_from_slice(src_a);
                let (src_b, _) = rest.split_at(b.len());
                b.copy_from_slice(src_b);
                written += a.len() + b.len();
                // 关键：rtrb WriteChunk 不会在 Drop 时自动提交，
                // 必须显式 commit_all() 推进 tail，否则消费者永远读不到。
                chunk.commit_all();
            }
            Err(_) => break,
        }
    }
    if occupancy_ms < 10.0 {
        buf.on_underrun();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一帧：幅度 `amp`、长度 960 的正弦波（避免 DC 偏差）。
    fn make_frame(amp: f32, len: usize) -> Vec<f32> {
        (0..len).map(|i| (i as f32 * 0.05).sin() * amp).collect()
    }

    #[test]
    fn agc_raises_weak_voice_to_target() {
        // 弱语音约 -58dBFS（amp=0.0018，正弦 RMS=amp/√2）。
        let mut agc = Agc::new();
        let mut last_db = -120.0f64;
        // 模拟生产：每帧独立输入，多帧让增益收敛。
        for _ in 0..12 {
            let mut frame = make_frame(0.0018, ENCODER_FRAME_SIZE);
            agc.process_frame(&mut frame);
            let (rms, peak) = frame_stats(&frame);
            last_db = 20.0 * (rms as f64).log10();
            assert!(peak <= 0.99, "AGC 后峰值 {} 异常（削波）", peak);
        }
        // 收敛后应提升到目标附近（> -30dBFS）。
        assert!(last_db > -30.0, "AGC 后 RMS {}dBFS 仍过低", last_db);
    }

    #[test]
    fn agc_does_not_amplify_silence() {
        let mut frame = make_frame(0.00003, ENCODER_FRAME_SIZE); // -90dBFS
        let mut agc = Agc::new();
        agc.gain = 20.0; // 模拟此前语音拉高增益
        agc.process_frame(&mut frame);
        let (rms, _) = frame_stats(&frame);
        // 静音帧不放大：RMS 保持极低。
        assert!(rms < 0.0001, "静音帧被放大: RMS={}", rms);
    }

    #[test]
    fn vad_triggers_on_speech_and_releases_after_silence() {
        let mut vad = Vad::new();
        // 静音（-80dBFS）不触发。
        assert!(!vad.decide(0.0001));
        // 语音（-45dBFS）立即触发。
        assert!(vad.decide(0.0056));
        // 词间停顿（-50dBFS，仍高于释放阈值）不释放。
        assert!(vad.decide(0.003));
        // 尾随：刚进入静音时仍发送（hangover 防切词）。
        assert!(vad.decide(0.00008));
        // 持续静音最终释放。
        let mut released = false;
        for _ in 0..40 {
            if !vad.decide(0.00008) {
                released = true;
                break;
            }
        }
        assert!(released, "持续静音未释放");
        // 释放后新语音再次触发。
        assert!(vad.decide(0.0056));
    }

    #[test]
    fn vad_does_not_swallow_soft_speech_at_startup() {
        // 启动即说话（-55dBFS），阈值应从 min_on 起步，不应吞帧。
        let mut vad = Vad::new();
        assert!(vad.decide(0.0018), "启动即说话被 VAD 吞掉");
    }
}
