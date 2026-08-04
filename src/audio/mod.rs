//! 音频子系统：无锁实时管线。
//! CPAL 回调仅做固定成本的格式转换与 rtrb 读写（禁止锁/堆分配/重采样/Opus/日志）；
//! 捕获与播放各由独立 DSP worker 处理，时钟漂移每 500ms 调整重采样比率（±1000ppm）。

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
use opus::{NetworkGrade, OpusCodec, ENCODER_FRAME_SIZE, ENCODER_RATE, FRAME_MS};
use resample::AsyncResampler;

const DRIFT_PERIOD: Duration = Duration::from_millis(500);
const MAX_DRIFT_PPM: f64 = 1000.0;
const RATE_LIMIT_PPM_S: f64 = 50.0;
/// 输出环形缓冲容量（320ms @ 96kHz 余量）。
const OUTPUT_RING_SAMPLES: usize = 32 * 1024;
/// 输入环形缓冲容量（250ms @ 48kHz，吸收 WASAPI 采集突发写入）。
const INPUT_RING_SAMPLES: usize = 12 * 1024;
/// 捕获批次上限（一次循环尽量读空 rtrb）。
const CAPTURE_BATCH: usize = 4096;

/// 下行到播放 worker 的消息。
pub enum PlaybackMsg {
    Audio(Vec<u8>),
    /// 清空播放缓冲（TTS 切换）。
    Flush,
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
    playback_grade_tx: Option<mpsc::Sender<NetworkGrade>>,
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
            playback_grade_tx: None,
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
        let (playback_grade_tx, playback_grade_rx) = mpsc::channel::<NetworkGrade>(4);
        self.workers.push(tokio::spawn(capture_worker(
            in_consumer,
            encoded_tx,
            grade_rx,
            input_rate,
        )));
        self.workers.push(tokio::spawn(playback_worker(
            playback_rx,
            playback_grade_rx,
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
        self.playback_grade_tx = Some(playback_grade_tx);
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
        self.playback_grade_tx = None;
    }

    /// 下行发送端。忽略返回值会导致下行静默失效。
    #[must_use]
    pub fn playback_sender(&self) -> Option<mpsc::Sender<PlaybackMsg>> {
        self.playback_tx.clone()
    }

    /// 捕获侧网络分级控制端（更新 Opus 编码策略）。忽略返回值会导致分级链路失效。
    #[must_use]
    pub fn grade_sender(&self) -> Option<mpsc::Sender<NetworkGrade>> {
        self.grade_tx.clone()
    }

    /// 播放侧网络分级控制端（更新解码 FEC 策略）。忽略返回值会导致分级链路失效。
    #[must_use]
    pub fn playback_grade_sender(&self) -> Option<mpsc::Sender<NetworkGrade>> {
        self.playback_grade_tx.clone()
    }
}

/// 输入回调：格式转换 + 下混单声道 + rtrb 入队。
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

/// 输出欠载状态：点击抑制所需状态为闭包局部变量，无需锁/原子。
struct Underrun {
    last_out: f32,
    ramp_left: usize,
    ramp_pending: bool,
    ramp_len: usize,
}

impl Underrun {
    fn new(ramp_len: usize) -> Self {
        Self {
            last_out: 0.0,
            ramp_left: 0,
            ramp_pending: true,
            ramp_len,
        }
    }
}

/// 输出回调：rtrb 读单声道 → 声道展开 + 格式转换；欠载 5ms 斜坡静音。
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
    let mut underrun = Underrun::new(ramp_len);
    // cb_samples 由回调本地累积，批量提交原子（每 ~10ms 一次 fetch_add，避免每样本原子操作）。
    let mut local_cb: u64 = 0;
    let stream = device
        .build_output_stream(
            config,
            move |data: &mut [T], _: &_| {
                let consumed = fill_output(data, channels, &mut consumer, &mut underrun, &cb_count);
                local_cb += consumed as u64;
                if local_cb >= 960 {
                    cb_samples.fetch_add(local_cb, Ordering::Relaxed);
                    local_cb = 0;
                }
            },
            |err| warn!("输出流错误: {}", err),
            None,
        )
        .map_err(|e| VoiceError::Audio(format!("构建输出流失败: {}", e)))?;
    Ok(stream)
}

/// 输出回调体（无锁、零分配、零日志）。
fn fill_output<T>(
    data: &mut [T],
    channels: usize,
    consumer: &mut Consumer<f32>,
    u: &mut Underrun,
    cb_count: &AtomicU64,
) -> usize
where
    T: FromSample<f32> + Copy,
{
    cb_count.fetch_add(1, Ordering::Relaxed);
    let frames = data.len() / channels;
    let ramp_len = u.ramp_len.max(1);
    let mut consumed = 0;
    for i in 0..frames {
        let sample = match consumer.pop().ok() {
            Some(v) => {
                consumed += 1;
                u.last_out = v;
                u.ramp_pending = true;
                v
            }
            None => {
                if u.ramp_pending {
                    u.ramp_pending = false;
                    if u.last_out.abs() > 1e-4 {
                        u.ramp_left = ramp_len - 1;
                    }
                }
                if u.ramp_left > 0 {
                    u.ramp_left -= 1;
                    u.last_out * (u.ramp_left as f32 / ramp_len as f32)
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
    consumed
}

/// 捕获统计（每 2s 诊断日志，确认麦克风数据真实流入）。
struct CaptureStats {
    flush_at: Instant,
    samples: u64,
    sum_sq: f64,
    frames: u64,
    /// VAD 判定后发送的语音帧数。
    speech_frames: u64,
    /// VAD 判定后跳过的静音帧数。
    silence_frames: u64,
    /// 最近一次 AGC 增益。
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

    /// 每 2s 诊断输出后清零。
    fn reset(&mut self) {
        self.samples = 0;
        self.sum_sq = 0.0;
        self.frames = 0;
        self.speech_frames = 0;
        self.silence_frames = 0;
        self.agc_gain = 1.0;
        self.flush_at = Instant::now();
    }
}

/// 播放统计（每 2s 诊断日志，确认下行解码与写入正常）。
struct PlaybackStats {
    flush_at: Instant,
    received: u64,
    decoded_ok: u64,
    decoded_fail: u64,
    plc: u64,
    written: u64,
    last_pcm_len: usize,
    last_needed: usize,
    last_produced: usize,
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

/// 漂移控制器：比率随观测漂移调整（限幅 ±1000ppm、变化率 ≤50ppm/s）。
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

/// 帧统计（RMS 与峰值）。单帧 ≤960 样本，f32 精度足够。
struct FrameStats {
    rms: f32,
    peak: f32,
}

fn frame_stats(frame: &[f32]) -> FrameStats {
    if frame.is_empty() {
        return FrameStats {
            rms: 0.0,
            peak: 0.0,
        };
    }
    let mut sum_sq = 0.0f32;
    let mut peak = 0.0f32;
    for &s in frame {
        sum_sq += s * s;
        peak = peak.max(s.abs());
    }
    FrameStats {
        rms: (sum_sq / frame.len() as f32).sqrt(),
        peak,
    }
}

/// 上行自动增益控制：把语音帧 RMS 提升到目标电平（-21dBFS，上限 36dB），静音不放大。
struct Agc {
    gain: f32,
    /// 视为语音的最小 RMS（≈ -66dBFS）。
    voice_threshold: f32,
    /// 语音帧目标 RMS（≈ -21dBFS）。
    target_rms: f32,
    /// 最大增益（36dB）。
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

    /// 对一帧做 AGC。`rms`/`peak` 由调用方传入（复用 VAD 已算的帧统计，避免重复遍历）。
    fn process_frame(&mut self, frame: &mut [f32], rms: f32, peak: f32) {
        if rms < self.voice_threshold {
            self.decay();
            return;
        }
        let mut target = (self.target_rms / rms).clamp(1.0, self.max_gain);
        // 峰值防削波：增益上限不超过 0.85/peak。
        if peak > 0.0 {
            target = target.min(0.85 / peak);
        }
        let alpha = if target > self.gain { 0.5 } else { 0.2 };
        self.gain += (target - self.gain) * alpha;
        let g = self.gain;
        for s in frame.iter_mut() {
            *s = (*s * g).clamp(-0.98, 0.98);
        }
    }

    /// 静音帧：不放大，内部增益缓慢回落待命（由 VAD 静音分支调用）。
    fn decay(&mut self) {
        self.gain += (1.0 - self.gain) * 0.1;
    }
}

/// 语音活动检测：静音帧不编码不上行，降低背景噪声对服务器 VAD/ASR 干扰。
/// 自适应噪声底 + 滞回 + hangover 1.2s（宁多勿漏，避免吞字）。
struct Vad {
    speech: bool,
    hangover: u32,
    /// 噪声底估计（EMA，仅静音段更新）。
    noise_floor: f32,
    /// 触发阈值缩放系数（= 10^(8dB/20)，预计算避免每帧 powf）。
    on_scale: f32,
    /// 释放阈值缩放系数（= 10^(4dB/20)）。
    off_scale: f32,
    /// 语音结束后的尾随帧数（1.2s @ 60ms）。
    hangover_frames: u32,
    min_on: f32,
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
            on_scale: 10f32.powf(8.0 / 20.0),
            off_scale: 10f32.powf(4.0 / 20.0),
            hangover_frames: 20,
            min_on: 0.0004,
            min_off: 0.0002,
        }
    }

    /// 输入一帧 RMS，返回是否应发送该帧（true=语音，false=静音跳过）。
    fn decide(&mut self, rms: f32) -> bool {
        let on = (self.noise_floor * self.on_scale).max(self.min_on);
        let off = (self.noise_floor * self.off_scale).max(self.min_off);
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
        } else if rms >= on {
            self.speech = true;
            self.hangover = 0;
        } else {
            // 静音段慢跟噪声底（仅低电平观测进入，避免语音污染）。
            self.noise_floor = self.noise_floor * 0.98 + rms * 0.02;
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
    let mut worker = match CaptureWorker::new(device_rate) {
        Ok(w) => w,
        Err(e) => {
            warn!("捕获管线初始化失败: {}", e);
            return;
        }
    };
    let mut tick = tokio::time::interval(Duration::from_millis(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            grade = grade_rx.recv() => {
                if let Some(g) = grade {
                    worker.opus.set_network_grade(g);
                }
            }
            _ = tick.tick() => {
                worker.process_batch(&mut input, &encoded_tx).await;
            }
        }
    }
}

/// 捕获管线状态：DSP 组件 + 热路径缓冲（worker 级复用，避免每次循环堆分配）。
struct CaptureWorker {
    resampler: AsyncResampler,
    opus: OpusCodec,
    pending: Vec<f32>,
    chunk: Vec<f32>,
    resample_out: Vec<f32>,
    frame: Vec<f32>,
    drift: DriftController,
    window_count: u64,
    window_start: Instant,
    stats: CaptureStats,
    agc: Agc,
    vad: Vad,
}

impl CaptureWorker {
    fn new(device_rate: u32) -> Result<Self> {
        let nominal = 16_000.0 / device_rate as f64;
        Ok(Self {
            resampler: AsyncResampler::new(nominal, 160)?,
            opus: {
                let mut o = OpusCodec::new(16_000)?;
                o.set_complexity(10);
                o
            },
            pending: Vec::with_capacity(ENCODER_FRAME_SIZE * 2),
            chunk: Vec::with_capacity(CAPTURE_BATCH),
            resample_out: Vec::with_capacity(CAPTURE_BATCH + 8),
            frame: Vec::with_capacity(ENCODER_FRAME_SIZE),
            drift: DriftController::new(nominal),
            window_count: 0,
            window_start: Instant::now(),
            stats: CaptureStats::new(),
            agc: Agc::new(),
            vad: Vad::new(),
        })
    }

    /// 处理一批输入（非阻塞）：读取 → 重采样 → 漂移校正 → 组帧编码。
    async fn process_batch(&mut self, input: &mut Consumer<f32>, encoded_tx: &LatestSlot<Vec<u8>>) {
        if !self.drain_input(input) {
            return;
        }
        self.update_drift();
        self.encode_pending(encoded_tx).await;
        self.flush_stats();
    }

    /// 读空 rtrb 并重采样到 16kHz，产出追加到 pending；无输入返回 false。
    fn drain_input(&mut self, input: &mut Consumer<f32>) -> bool {
        self.chunk.clear();
        let mut any = false;
        while let Ok(s) = input.pop() {
            self.chunk.push(s);
            any = true;
            if self.chunk.len() >= CAPTURE_BATCH {
                break;
            }
        }
        if !any {
            return false;
        }
        self.window_count += self.chunk.len() as u64;
        self.stats.samples += self.chunk.len() as u64;
        self.stats.sum_sq += self.chunk.iter().map(|s| (*s as f64) * (*s as f64)).sum::<f64>();

        self.resample_out.clear();
        self.resampler.process(&self.chunk, self.drift.ratio, &mut self.resample_out);
        self.pending.extend_from_slice(&self.resample_out);
        true
    }

    /// 每 500ms 按窗口样本计数校正重采样比率（补偿时钟漂移）。
    fn update_drift(&mut self) {
        if self.window_start.elapsed() >= DRIFT_PERIOD {
            let observed = self.window_count as f64 / self.window_start.elapsed().as_secs_f64();
            if observed > 0.0 {
                let target_ratio = ENCODER_RATE as f64 / observed;
                let target_ppm =
                    (target_ratio - self.drift.nominal_ratio) / self.drift.nominal_ratio * 1e6;
                self.drift.move_toward(target_ppm);
            }
            self.window_count = 0;
            self.window_start = Instant::now();
        }
    }

    /// 组 60ms 帧：VAD 判定语音才编码上行，空闲时零编码开销。
    async fn encode_pending(&mut self, encoded_tx: &LatestSlot<Vec<u8>>) {
        while self.pending.len() >= ENCODER_FRAME_SIZE {
            self.frame.clear();
            self.frame.extend(self.pending.drain(..ENCODER_FRAME_SIZE));
            let s = frame_stats(&self.frame);
            if self.vad.decide(s.rms) {
                self.stats.speech_frames += 1;
                self.agc.process_frame(&mut self.frame, s.rms, s.peak);
                self.stats.agc_gain = self.agc.gain;
                match self.opus.encode(&self.frame) {
                    Ok(pkt) => {
                        self.stats.frames += 1;
                        encoded_tx.store(pkt).await;
                    }
                    Err(e) => warn!("Opus 编码失败: {}", e),
                }
            } else {
                self.stats.silence_frames += 1;
                // 静音帧：增益回落待命（不放大）。
                self.agc.decay();
            }
        }
    }

    /// 每 2s 输出一次捕获诊断并清零统计。
    fn flush_stats(&mut self) {
        if self.stats.flush_at.elapsed() >= Duration::from_secs(2) {
            let rms = if self.stats.samples > 0 {
                (self.stats.sum_sq / self.stats.samples as f64).sqrt()
            } else {
                0.0
            };
            let db = if rms > 1e-9 { 20.0 * rms.log10() } else { -120.0 };
            debug!(
                "捕获诊断: RMS={:.1}dBFS, 输入样本={}, 编码帧={} (语音{} 静音{}), AGC增益={:.1}x",
                db,
                self.stats.samples,
                self.stats.frames,
                self.stats.speech_frames,
                self.stats.silence_frames,
                self.stats.agc_gain
            );
            self.stats.reset();
        }
    }
}

/// 播放 DSP worker：Opus → 解码（服务器率）→ 设备率 → rtrb。
async fn playback_worker(
    rx: mpsc::Receiver<PlaybackMsg>,
    grade_rx: mpsc::Receiver<NetworkGrade>,
    out: Producer<f32>,
    server_rate: u32,
    output_rate: u32,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
) {
    match PlaybackWorker::new(rx, grade_rx, out, server_rate, output_rate, cb_count, cb_samples) {
        Ok(worker) => worker.run().await,
        Err(e) => warn!("播放管线初始化失败: {}", e),
    }
}

/// 播放管线状态：DSP 组件 + 播放缓冲 + 诊断统计（与 `CaptureWorker` 对称）。
struct PlaybackWorker {
    rx: mpsc::Receiver<PlaybackMsg>,
    grade_rx: mpsc::Receiver<NetworkGrade>,
    out: Producer<f32>,
    /// 服务器声明采样率（解码帧长基准）。
    server_rate: u32,
    output_rate: u32,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
    resampler: AsyncResampler,
    decoder: OpusCodec,
    buf: PlaybackBuffer,
    drift: DriftController,
    last_drift: Instant,
    stats: PlaybackStats,
    /// 当前网络分级（弱网时丢包窗口启用带内 FEC/PLC 恢复）。
    grade: NetworkGrade,
    /// 距上一帧到达的时间：丢包判定基准（超时由帧定时器补恢复帧）。
    last_frame_at: Instant,
    /// 帧长诊断已告警（一次性，避免刷屏）。
    frame_len_warned: bool,
    /// 重采样输出缓冲常驻复用，避免每帧解码后堆分配。
    out_samples: Vec<f32>,
}

impl PlaybackWorker {
    fn new(
        rx: mpsc::Receiver<PlaybackMsg>,
        grade_rx: mpsc::Receiver<NetworkGrade>,
        out: Producer<f32>,
        server_rate: u32,
        output_rate: u32,
        cb_count: Arc<AtomicU64>,
        cb_samples: Arc<AtomicU64>,
    ) -> Result<Self> {
        let nominal = output_rate as f64 / server_rate as f64;
        Ok(Self {
            rx,
            grade_rx,
            out,
            server_rate,
            output_rate,
            cb_count,
            cb_samples,
            resampler: AsyncResampler::new(nominal, 960)?,
            decoder: OpusCodec::new(server_rate)?,
            buf: PlaybackBuffer::new(60),
            drift: DriftController::new(nominal),
            last_drift: Instant::now(),
            stats: PlaybackStats::new(),
            grade: NetworkGrade::Good,
            last_frame_at: Instant::now(),
            frame_len_warned: false,
            out_samples: Vec::with_capacity(960 * 3),
        })
    }

    async fn run(mut self) {
        // 帧定时器：无新帧到达时按时产出 FEC/PLC 恢复帧，保持丢包期间音频连续。
        let mut tick = tokio::time::interval(Duration::from_millis(FRAME_MS as u64));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                g = self.grade_rx.recv() => {
                    if let Some(g) = g {
                        self.grade = g;
                    }
                }
                msg = self.rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg).await;
                }
                _ = tick.tick() => {
                    // 超过两帧时长无新帧 → 判定丢包，补一帧 FEC/PLC 恢复。
                    if self.last_frame_at.elapsed() >= Duration::from_millis(FRAME_MS as u64 * 2) {
                        self.play_plc();
                    }
                }
            }
            self.diag_and_drift();
        }
    }

    /// 处理一条下行消息（Audio/Flush）。
    async fn handle_msg(&mut self, msg: PlaybackMsg) {
        match msg {
            PlaybackMsg::Audio(opus) => {
                self.stats.received += 1;
                self.buf.observe_arrival(Instant::now());
                // 正常包恒 fec=false：decode_fec=1 会把带 FEC 的包解码为"上一帧恢复"而非当前帧，
                // 导致所有帧错位；FEC/PLC 恢复仅在丢包（无帧到达）时由帧定时器触发。
                self.last_frame_at = Instant::now();
                match self.decoder.decode(&opus, false) {
                    Ok(p) => self.write_pcm(p, false),
                    Err(e) => {
                        self.stats.decoded_fail += 1;
                        debug!("Opus 解码失败(len={}): {}", opus.len(), e);
                        self.play_plc();
                    }
                }
            }
            PlaybackMsg::Flush => self.buf.reset(),
        }
    }

    /// 将解码 PCM 重采样后写入输出 ring。
    fn write_pcm(&mut self, pcm: Vec<f32>, used_plc: bool) {
        if !used_plc {
            // 帧长应等于 server_rate 的 60ms：不符说明服务器实际采样率与声明不一致（音调/语速错乱）。
            let expected = self.server_rate as usize * 60 / 1000;
            if pcm.len() != expected && !self.frame_len_warned {
                self.frame_len_warned = true;
                warn!(
                    "播放帧长异常: 期望 {} 实际 {}（服务器采样率与声明 {}Hz 不符）",
                    expected, pcm.len(), self.server_rate
                );
            }
            self.stats.decoded_ok += 1;
        }
        self.stats.last_pcm_len = pcm.len();
        self.stats.last_needed = self.resampler.input_frames_next();
        self.out_samples.clear();
        self.resampler.process(&pcm, self.drift.ratio, &mut self.out_samples);
        self.stats.last_produced = self.out_samples.len();
        if self.out_samples.is_empty() {
            self.stats.zero_produced += 1;
        }
        let before = self.out.slots();
        write_samples(
            &mut self.out,
            &self.out_samples,
            self.output_rate,
            OUTPUT_RING_SAMPLES,
        );
        self.stats.written += before.saturating_sub(self.out.slots()) as u64;
    }

    /// 丢包恢复：请求带内 FEC/PLC 解码一帧（60ms），保持音频流连续。
    fn play_plc(&mut self) {
        match self.decoder.decode(&[], true) {
            Ok(p) => {
                self.stats.plc += 1;
                self.write_pcm(p, true);
            }
            Err(e) => warn!("PLC 失败: {}", e),
        }
    }

    /// 每迭代执行：2s 诊断输出 + 500ms 漂移校正。
    fn diag_and_drift(&mut self) {
        if self.stats.flush_at.elapsed() >= Duration::from_secs(2) {
            let cb = self.cb_count.swap(0, Ordering::Relaxed);
            let cbs = self.cb_samples.swap(0, Ordering::Relaxed);
            debug!(
                "播放诊断: 收帧={}, 解码成功={}, 失败={}, PLC={}, 写入={}, pcm={}, needed={}, 产出={}, 零产出={} | 回调={}次, 消费样本={}",
                self.stats.received,
                self.stats.decoded_ok,
                self.stats.decoded_fail,
                self.stats.plc,
                self.stats.written,
                self.stats.last_pcm_len,
                self.stats.last_needed,
                self.stats.last_produced,
                self.stats.zero_produced,
                cb,
                cbs
            );
            self.stats = PlaybackStats::new();
        }

        if self.last_drift.elapsed() >= DRIFT_PERIOD {
            self.buf.update_target(Instant::now());
            // 缓冲占用校正：占用高 → 降低比率（放慢产出），反之升高。
            let free = self.out.slots() as f64;
            let cap = OUTPUT_RING_SAMPLES as f64;
            let occupancy_ms = ((cap - free) / self.output_rate as f64) * 1000.0;
            let target_ms = self.buf.target_ms() as f64;
            if target_ms > 0.0 {
                let err_ppm = ((occupancy_ms - target_ms) / target_ms) * 1e6 * 0.5;
                self.drift.move_toward(err_ppm.clamp(-MAX_DRIFT_PPM, MAX_DRIFT_PPM));
            }
            self.last_drift = Instant::now();
        }
    }
}

/// 写入播放环形缓冲；空间不足时丢弃多余（新）样本以封顶延迟。
fn write_samples(
    out: &mut Producer<f32>,
    samples: &[f32],
    output_rate: u32,
    capacity: usize,
) {
    let free = out.slots();
    let occupancy_ms = ((capacity - free) as f64 / output_rate as f64) * 1000.0;
    // 占用超 280ms 时整体丢弃本轮，让缓冲自然回落。
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
        for _ in 0..12 {
            let mut frame = make_frame(0.0018, ENCODER_FRAME_SIZE);
            let s = frame_stats(&frame);
            agc.process_frame(&mut frame, s.rms, s.peak);
            let s = frame_stats(&frame);
            last_db = 20.0 * (s.rms as f64).log10();
            assert!(s.peak <= 0.99, "AGC 后峰值 {} 异常（削波）", s.peak);
        }
        // 收敛后应提升到目标附近（> -30dBFS）。
        assert!(last_db > -30.0, "AGC 后 RMS {}dBFS 仍过低", last_db);
    }

    #[test]
    fn agc_does_not_amplify_silence() {
        let mut frame = make_frame(0.00003, ENCODER_FRAME_SIZE); // -90dBFS
        let mut agc = Agc::new();
        agc.gain = 20.0; // 模拟此前语音拉高增益
        let s = frame_stats(&frame);
        agc.process_frame(&mut frame, s.rms, 0.0);
        let s = frame_stats(&frame);
        // 静音帧不放大：RMS 保持极低。
        assert!(s.rms < 0.0001, "静音帧被放大: RMS={}", s.rms);
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

    /// 单次调整受速率限制（≤50ppm/s × 最小采样间隔 1ms = 0.05ppm）。
    #[test]
    fn drift_controller_rate_limits_step() {
        let mut d = DriftController::new(2.0);
        d.move_toward(1_000_000.0); // 远超限幅的目标
        assert!(d.current_ppm().abs() <= 0.051, "速率限制未生效");
    }

    /// 长期大幅调整不越限幅边界（±1000ppm）。
    #[test]
    fn drift_controller_never_exceeds_ppm_limit() {
        let mut d = DriftController::new(2.0);
        for _ in 0..100_000 {
            d.move_toward(1_000_000.0);
        }
        assert!(d.current_ppm() <= MAX_DRIFT_PPM + 1e-6);
        for _ in 0..100_000 {
            d.move_toward(-1_000_000.0);
        }
        assert!(d.current_ppm() >= -MAX_DRIFT_PPM - 1e-6);
    }
}
