//! 音频子系统：无锁实时管线。
//! CPAL 回调仅做固定成本的格式转换与 rtrb 读写（禁止锁/堆分配/重采样/Opus/日志）；
//! 捕获与播放各由独立 DSP worker 处理，时钟漂移每 500ms 调整重采样比率（±1000ppm）。

pub mod buffer;
pub mod opus;
mod output;
pub mod resample;
mod vad;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, SampleFormat, SizedSample, Stream, StreamConfig};
use log::{debug, info, warn};
use rtrb::{Consumer, Producer};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{Result, VoiceError};
use crate::protocol::LatestSlot;

use buffer::PlaybackBuffer;
use opus::{ENCODER_FRAME_SIZE, ENCODER_RATE, FRAME_MS, NetworkGrade, OpusCodec};
use output::{DUCK_HOLD_MS, DuplexSignal, OutputShared, OutputTarget, build_output_stream};
use resample::AsyncResampler;
use vad::{AudioTuning, HighPass, Vad, VadConfig};

const DRIFT_PERIOD: Duration = Duration::from_millis(500);
const MAX_DRIFT_PPM: f64 = 1000.0;
const RATE_LIMIT_PPM_S: f64 = 50.0;
/// 输出环形缓冲容量（320ms @ 96kHz 余量）。
const OUTPUT_RING_SAMPLES: usize = 32 * 1024;
/// 输入环形缓冲容量（250ms @ 48kHz，吸收 WASAPI 采集突发写入）。
const INPUT_RING_SAMPLES: usize = 12 * 1024;
/// 捕获批次上限（一次循环尽量读空 rtrb）。
const CAPTURE_BATCH: usize = 4096;
/// 连续补帧上限：超过约 3 帧（180ms）仍无新包说明不是瞬态丢包，宁可静音也不无限 PLC。
const MAX_PLC_FRAMES: u32 = 3;
/// 下行音频积压上限（帧，约 240ms）：超过即丢弃过期音频，防止一次卡顿把延迟推到数秒。
const MAX_AUDIO_BACKLOG: usize = 4;

/// 下行到播放 worker 的消息。
pub enum PlaybackMsg {
    Audio(Vec<u8>),
    /// 清空播放缓冲（TTS 切换）。
    Flush,
}

/// 音频管线：持有设备、流与 DSP worker。
pub struct AudioPipeline {
    input_device: Device,
    output_device: Device,
    input_stream: Option<Stream>,
    output_stream: Option<Stream>,
    workers: Vec<JoinHandle<()>>,
    playback_tx: Option<mpsc::Sender<PlaybackMsg>>,
    grade_tx: Option<mpsc::Sender<NetworkGrade>>,
    /// CPAL 流错误标志：监督器检测到后结束会话并重建音频管线。
    device_error: Option<Arc<AtomicBool>>,
}

impl AudioPipeline {
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
            device_error: None,
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
            "音频管线: 输入 {}Hz/{}ch/{:?} → 16kHz 编码; 下行 {}Hz → 输出 {}Hz/{}ch/{:?}",
            input_rate,
            input_channels,
            input_format,
            server_rate,
            output_rate,
            output_channels,
            output_format
        );

        let (in_producer, in_consumer) = rtrb::RingBuffer::new(INPUT_RING_SAMPLES);
        let (out_producer, out_consumer) = rtrb::RingBuffer::new(OUTPUT_RING_SAMPLES);
        // 捕获/播放协同信号先于两条流创建：输出回调与两个 DSP worker 共享同一单调时钟。
        let duplex = Arc::new(DuplexSignal::new());
        // CPAL 错误回调（设备拔出/USB 故障）只置位，由监督器异步发现并重建。
        let device_error = Arc::new(AtomicBool::new(false));

        let input_stream = match input_format {
            SampleFormat::I16 => build_input_stream::<i16>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: i16| s as f32 / 32768.0,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::I8 => build_input_stream::<i8>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: i8| s as f32 / 128.0,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::I32 => build_input_stream::<i32>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: i32| s as f32 / 2_147_483_648.0,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::I64 => build_input_stream::<i64>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: i64| (s as f64 / 9_223_372_036_854_775_808.0) as f32,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::U8 => build_input_stream::<u8>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: u8| (s as f32 - 128.0) / 128.0,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::U32 => build_input_stream::<u32>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: u32| ((s as f64 - 2_147_483_648.0) / 2_147_483_648.0) as f32,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::U64 => build_input_stream::<u64>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: u64| {
                    ((s as f64 - 9_223_372_036_854_775_808.0) / 9_223_372_036_854_775_808.0) as f32
                },
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::U16 => build_input_stream::<u16>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: u16| (s as f32 - 32768.0) / 32768.0,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::F32 => build_input_stream::<f32>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: f32| s,
                in_producer,
                device_error.clone(),
            )?,
            SampleFormat::F64 => build_input_stream::<f64>(
                &self.input_device,
                input_cfg.into(),
                input_channels,
                |s: f64| s as f32,
                in_producer,
                device_error.clone(),
            )?,
            other => return Err(VoiceError::Audio(format!("不支持的输入格式: {:?}", other))),
        };
        let ramp_len = (output_rate as usize * 5) / 1000;
        let cb_count = Arc::new(AtomicU64::new(0));
        let cb_samples = Arc::new(AtomicU64::new(0));
        let underruns = Arc::new(AtomicU64::new(0));
        let target = OutputTarget {
            device: &self.output_device,
            config: output_cfg.into(),
            channels: output_channels,
            sample_rate: output_rate,
            ramp_len,
        };
        let shared = OutputShared {
            cb_count: cb_count.clone(),
            cb_samples: cb_samples.clone(),
            underruns: underruns.clone(),
            duplex: duplex.clone(),
            device_error: device_error.clone(),
        };
        let output_stream = match output_format {
            SampleFormat::I16 => build_output_stream::<i16>(target, out_consumer, shared)?,
            SampleFormat::I8 => build_output_stream::<i8>(target, out_consumer, shared)?,
            SampleFormat::I32 => build_output_stream::<i32>(target, out_consumer, shared)?,
            SampleFormat::I64 => build_output_stream::<i64>(target, out_consumer, shared)?,
            SampleFormat::U8 => build_output_stream::<u8>(target, out_consumer, shared)?,
            SampleFormat::U32 => build_output_stream::<u32>(target, out_consumer, shared)?,
            SampleFormat::U64 => build_output_stream::<u64>(target, out_consumer, shared)?,
            SampleFormat::F32 => build_output_stream::<f32>(target, out_consumer, shared)?,
            SampleFormat::F64 => build_output_stream::<f64>(target, out_consumer, shared)?,
            SampleFormat::U16 => build_output_stream::<u16>(target, out_consumer, shared)?,
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
        let tuning = AudioTuning::from_env();
        // 同步构造 worker：初始化失败直接让 start 返回 Err，由监督器走设备重建/重连，
        // 而不是任务静默退出、会话"没声音也不重连"。
        let capture = CaptureWorker::new(input_rate, tuning.vad, duplex.clone())?;
        let playback = PlaybackWorker::new(
            playback_rx,
            PlaybackSetup {
                out: out_producer,
                server_rate,
                output_rate,
                play_tail_ms: tuning.play_tail_ms,
                cb_count,
                cb_samples,
                underruns,
                duplex,
            },
        )?;
        self.workers.push(tokio::spawn(capture_worker(
            in_consumer,
            encoded_tx,
            grade_rx,
            capture,
        )));
        self.workers.push(tokio::spawn(playback_worker(playback)));

        self.input_stream = Some(input_stream);
        self.output_stream = Some(output_stream);
        self.playback_tx = Some(playback_tx);
        self.grade_tx = Some(grade_tx);
        self.device_error = Some(device_error);
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
        self.device_error = None;
    }

    /// 音频流是否发生过 CPAL 错误（设备热拔/流故障）。
    pub fn has_device_error(&self) -> bool {
        self.device_error
            .as_ref()
            .is_some_and(|e| e.load(Ordering::Relaxed))
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
}

/// 输入回调：格式转换 + 下混单声道 + rtrb 入队。
fn build_input_stream<S>(
    device: &Device,
    config: StreamConfig,
    channels: usize,
    to_f32: fn(S) -> f32,
    mut producer: Producer<f32>,
    device_error: Arc<AtomicBool>,
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
            move |err| {
                warn!("输入流错误: {}", err);
                device_error.store(true, Ordering::Relaxed);
            },
            None,
        )
        .map_err(|e| VoiceError::Audio(format!("构建输入流失败: {}", e)))?;
    Ok(stream)
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
    /// 处于 TTS 播放期的帧数（用于观察播放期抑制是否生效）。
    playing_frames: u64,
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
            playing_frames: 0,
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
        self.playing_frames = 0;
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
    /// 因下行积压被丢弃的过期音频帧数。
    trimmed: u64,
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
            trimmed: 0,
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
        if rms < self.voice_threshold || frame.is_empty() {
            self.decay();
            return;
        }
        let mut target = (self.target_rms / rms).clamp(1.0, self.max_gain);
        // 峰值防削波：增益上限不超过 0.85/peak。
        if peak > 0.0 {
            target = target.min(0.85 / peak);
        }
        let alpha = if target > self.gain { 0.5 } else { 0.2 };
        let next = (self.gain + (target - self.gain) * alpha).min(if peak > 0.0 {
            0.9 / peak
        } else {
            self.max_gain
        });
        // 帧内线性过渡：逐帧突变会在帧边界留下阶跃（周期性咔哒），平滑后上行更干净。
        let step = (next - self.gain) / frame.len() as f32;
        let mut g = self.gain;
        for s in frame.iter_mut() {
            g += step;
            *s = (*s * g).clamp(-0.98, 0.98);
        }
        self.gain = next;
    }

    /// 静音帧：不放大，内部增益缓慢回落待命（由 VAD 静音分支调用）。
    fn decay(&mut self) {
        self.gain += (1.0 - self.gain) * 0.1;
    }
}

/// 捕获 DSP worker：设备率 → 16kHz → 60ms 帧 → AGC → Opus 编码 → latest-slot。
async fn capture_worker(
    mut input: Consumer<f32>,
    encoded_tx: LatestSlot<Vec<u8>>,
    mut grade_rx: mpsc::Receiver<NetworkGrade>,
    mut worker: CaptureWorker,
) {
    let mut tick = tokio::time::interval(Duration::from_millis(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            grade = grade_rx.recv() => {
                match grade {
                    Some(g) => worker.opus.set_network_grade(g),
                    None => break,
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
    drift: DriftController,
    window_count: u64,
    window_start: Instant,
    stats: CaptureStats,
    agc: Agc,
    vad: Vad,
    high_pass: HighPass,
    /// 触发防抖期间的候选起始帧（原始 PCM）：确认后补发，避免吞掉首音节。
    start_roll: Vec<f32>,
    /// 播放活跃/插话闪避信号（播放 worker 写，捕获 worker 读）。
    duplex: Arc<DuplexSignal>,
    /// 本轮 TTS 是否已记录过插话（观测用，一轮只打一条）。
    barge_logged: bool,
}

impl CaptureWorker {
    fn new(device_rate: u32, vad_config: VadConfig, duplex: Arc<DuplexSignal>) -> Result<Self> {
        let nominal = 16_000.0 / device_rate as f64;
        let vad = Vad::with_config(&vad_config);
        let onset_capacity = vad.on_confirm_frames.saturating_sub(1) as usize * ENCODER_FRAME_SIZE;
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
            drift: DriftController::new(nominal),
            window_count: 0,
            window_start: Instant::now(),
            stats: CaptureStats::new(),
            agc: Agc::new(),
            vad,
            high_pass: HighPass::new(ENCODER_RATE),
            start_roll: Vec::with_capacity(onset_capacity),
            duplex,
            barge_logged: false,
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
        self.stats.sum_sq += self
            .chunk
            .iter()
            .map(|s| (*s as f64) * (*s as f64))
            .sum::<f64>();

        self.resample_out.clear();
        self.resampler
            .process(&self.chunk, self.drift.ratio, &mut self.resample_out);
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
    /// AGC/编码直接作用于 pending 前缀，省去独立帧缓冲与逐帧复制。
    async fn encode_pending(&mut self, encoded_tx: &LatestSlot<Vec<u8>>) {
        while self.pending.len() >= ENCODER_FRAME_SIZE {
            self.high_pass
                .process(&mut self.pending[..ENCODER_FRAME_SIZE]);
            let s = frame_stats(&self.pending[..ENCODER_FRAME_SIZE]);
            // 播放期：VAD 抬阈值 + 缩尾随，抑制旁人说话与扬声器回声造成的误打断。
            let playing = self.duplex.is_playing();
            if playing {
                self.stats.playing_frames += 1;
            }
            let was_speech = self.vad.speech;
            let send = self.vad.decide(s.rms, playing);
            if playing && (send || self.vad.awaiting_onset()) {
                // 候选帧即预闪避：不等防抖确认，先让路 60ms，也顺带压低扬声器回声；
                // 语音帧持续刷新窗口，用户不停口 TTS 就一直让路。
                self.duplex.mark_barge_in(DUCK_HOLD_MS);
                if send && !self.barge_logged {
                    self.barge_logged = true;
                    debug!("用户插话：TTS 本地闪避");
                }
            } else if !playing {
                self.barge_logged = false;
            }
            if send {
                if !was_speech {
                    self.flush_onset(encoded_tx).await;
                }
                self.stats.speech_frames += 1;
                self.agc
                    .process_frame(&mut self.pending[..ENCODER_FRAME_SIZE], s.rms, s.peak);
                self.stats.agc_gain = self.agc.gain;
                match self.opus.encode(&self.pending[..ENCODER_FRAME_SIZE]) {
                    Ok(pkt) => {
                        self.stats.frames += 1;
                        encoded_tx.store(pkt).await;
                    }
                    Err(e) => warn!("Opus 编码失败: {}", e),
                }
            } else {
                if self.vad.awaiting_onset() {
                    self.stage_onset();
                } else {
                    self.stats.silence_frames +=
                        (self.start_roll.len() / ENCODER_FRAME_SIZE + 1) as u64;
                    self.start_roll.clear();
                }
                // 静音帧：增益回落待命（不放大）。
                self.agc.decay();
            }
            self.pending.drain(..ENCODER_FRAME_SIZE);
        }
    }

    /// 暂存触发防抖期的候选起始帧；确认成立后由 [`Self::flush_onset`] 补发。
    fn stage_onset(&mut self) {
        // 容量恰为 on_confirm_frames-1 帧，正常路径不会溢出。
        self.start_roll
            .extend_from_slice(&self.pending[..ENCODER_FRAME_SIZE]);
    }

    /// 补发确认前已到达的起始帧：防抖不应以吞掉用户首音节为代价。
    async fn flush_onset(&mut self, encoded_tx: &LatestSlot<Vec<u8>>) {
        if self.start_roll.is_empty() {
            return;
        }
        let mut roll = std::mem::take(&mut self.start_roll);
        for frame in roll.as_chunks_mut::<ENCODER_FRAME_SIZE>().0 {
            let s = frame_stats(frame);
            self.agc.process_frame(frame, s.rms, s.peak);
            self.stats.speech_frames += 1;
            match self.opus.encode(frame) {
                Ok(pkt) => {
                    self.stats.frames += 1;
                    encoded_tx.store(pkt).await;
                }
                Err(e) => warn!("Opus 编码失败: {}", e),
            }
        }
        self.stats.agc_gain = self.agc.gain;
        roll.clear();
        self.start_roll = roll;
    }

    /// 每 2s 输出一次捕获诊断并清零统计。
    fn flush_stats(&mut self) {
        if self.stats.flush_at.elapsed() >= Duration::from_secs(2) {
            let rms = if self.stats.samples > 0 {
                (self.stats.sum_sq / self.stats.samples as f64).sqrt()
            } else {
                0.0
            };
            let db = if rms > 1e-9 {
                20.0 * rms.log10()
            } else {
                -120.0
            };
            let nf_db = 20.0 * self.vad.noise_floor.max(1e-9).log10();
            let on_db = (self.vad.noise_floor * self.vad.on_scale)
                .max(self.vad.min_on)
                .max(1e-9)
                .log10()
                * 20.0;
            debug!(
                "捕获诊断: RMS={:.1}dBFS, 输入样本={}, 编码帧={} (语音{} 静音{} 播放期{}), AGC增益={:.1}x, 噪声底={:.1}dBFS, 触发阈值={:.1}dBFS",
                db,
                self.stats.samples,
                self.stats.frames,
                self.stats.speech_frames,
                self.stats.silence_frames,
                self.stats.playing_frames,
                self.stats.agc_gain,
                nf_db,
                on_db
            );
            self.stats.reset();
        }
    }
}

/// 播放 worker 装配参数（设备/编解码时钟与协同信号）。
struct PlaybackSetup {
    out: Producer<f32>,
    server_rate: u32,
    output_rate: u32,
    play_tail_ms: u64,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
    underruns: Arc<AtomicU64>,
    duplex: Arc<DuplexSignal>,
}

/// 播放 DSP worker：Opus → 解码（服务器率）→ 设备率 → rtrb。
async fn playback_worker(worker: PlaybackWorker) {
    worker.run().await;
}

/// 播放管线状态：DSP 组件 + 播放缓冲 + 诊断统计（与 `CaptureWorker` 对称）。
struct PlaybackWorker {
    rx: mpsc::Receiver<PlaybackMsg>,
    out: Producer<f32>,
    /// 服务器声明采样率（解码帧长基准）。
    server_rate: u32,
    output_rate: u32,
    cb_count: Arc<AtomicU64>,
    cb_samples: Arc<AtomicU64>,
    /// 输出回调累计的欠载事件（每 2s 诊断后清零）。
    underruns: Arc<AtomicU64>,
    resampler: AsyncResampler,
    decoder: OpusCodec,
    buf: PlaybackBuffer,
    drift: DriftController,
    last_drift: Instant,
    stats: PlaybackStats,
    /// 距上一帧到达的时间：丢包判定基准（超时由帧定时器补恢复帧）。
    last_frame_at: Instant,
    /// 帧长诊断已告警（一次性，避免刷屏）。
    frame_len_warned: bool,
    /// 解码输出复用缓冲（每帧原地重写，避免每帧堆分配）。
    pcm: Vec<f32>,
    /// 重采样输出缓冲常驻复用，避免每帧解码后堆分配。
    out_samples: Vec<f32>,
    /// 捕获/播放协同：播放活跃窗口、插话闪避与输出清环。
    duplex: Arc<DuplexSignal>,
    /// 播放活跃尾随窗口（ms）：覆盖播放缓冲排空时间。
    play_tail_ms: u64,
    /// 连续 PLC 补帧计数：无新包时封顶，避免空闲期无限补帧。
    plc_run: u32,
    /// 累计写入输出环的样本数：清环边界以此为准。
    total_written: u64,
}

impl PlaybackWorker {
    fn new(rx: mpsc::Receiver<PlaybackMsg>, setup: PlaybackSetup) -> Result<Self> {
        let PlaybackSetup {
            out,
            server_rate,
            output_rate,
            play_tail_ms,
            cb_count,
            cb_samples,
            underruns,
            duplex,
        } = setup;
        let nominal = output_rate as f64 / server_rate as f64;
        Ok(Self {
            rx,
            out,
            server_rate,
            output_rate,
            cb_count,
            cb_samples,
            underruns,
            resampler: AsyncResampler::new(nominal, 960)?,
            decoder: OpusCodec::new(server_rate)?,
            buf: PlaybackBuffer::new(60),
            drift: DriftController::new(nominal),
            last_drift: Instant::now(),
            stats: PlaybackStats::new(),
            last_frame_at: Instant::now(),
            frame_len_warned: false,
            pcm: Vec::with_capacity(server_rate as usize * 60 / 1000),
            out_samples: Vec::with_capacity(960 * 3),
            duplex,
            play_tail_ms,
            plc_run: 0,
            total_written: 0,
        })
    }

    async fn run(mut self) {
        // 帧定时器：无新帧到达时按时产出 FEC/PLC 恢复帧，保持丢包期间音频连续。
        let mut tick = tokio::time::interval(Duration::from_millis(FRAME_MS as u64));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            self.trim_backlog();
            tokio::select! {
                msg = self.rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg);
                }
                _ = tick.tick() => {
                    // 仅在上一次真实帧的播放窗口内补帧：句间/会话空闲不生成 PLC，
                    // 否则新 TTS 的首帧前会先插入最多 180ms 的静音假帧。
                    if self.duplex.is_playing()
                        && self.last_frame_at.elapsed() >= Duration::from_millis(FRAME_MS as u64 * 2)
                    {
                        self.play_plc();
                    }
                }
            }
            self.diag_and_drift();
        }
    }

    /// 处理一条下行消息（Audio/Flush）。
    fn handle_msg(&mut self, msg: PlaybackMsg) {
        match msg {
            PlaybackMsg::Audio(opus) => self.handle_audio(opus),
            PlaybackMsg::Flush => self.handle_flush(),
        }
    }

    /// 下行积压超过上限时丢弃过期音频：保留最后一次 Flush 之后的最后一条，
    /// 其余直接跳过（解码器按丢包处理），避免对话延迟随卡顿时长线性增长。
    fn trim_backlog(&mut self) {
        let mut flush = false;
        let mut latest: Option<Vec<u8>> = None;
        let mut dropped = 0u64;
        while self.rx.len() > MAX_AUDIO_BACKLOG {
            match self.rx.try_recv() {
                Ok(PlaybackMsg::Audio(a)) => {
                    if latest.replace(a).is_some() {
                        dropped += 1;
                    }
                }
                Ok(PlaybackMsg::Flush) => {
                    if latest.take().is_some() {
                        dropped += 1;
                    }
                    flush = true;
                }
                Err(_) => break,
            }
        }
        if dropped > 0 {
            self.stats.trimmed += dropped;
            debug!("下行积压，丢弃 {} 帧过期音频", dropped);
        }
        if flush {
            self.handle_flush();
        }
        if let Some(opus) = latest {
            self.handle_audio(opus);
        }
    }

    fn handle_audio(&mut self, opus: Vec<u8>) {
        self.stats.received += 1;
        self.plc_run = 0;
        self.buf.observe_arrival(Instant::now());
        self.duplex.touch_play(self.play_tail_ms);
        // 正常包恒 fec=false：decode_fec=1 会把带 FEC 的包解码为"上一帧恢复"而非当前帧，
        // 导致所有帧错位；FEC/PLC 恢复仅在丢包（无帧到达）时由帧定时器触发。
        self.last_frame_at = Instant::now();
        match self.decoder.decode_into(&opus, false, &mut self.pcm) {
            Ok(()) => self.write_pcm(false),
            Err(e) => {
                self.stats.decoded_fail += 1;
                debug!("Opus 解码失败(len={}): {}", opus.len(), e);
                self.play_plc();
            }
        }
    }

    /// TTS 切换：清空本地播放（含输出环中 flush 之前的残留），捕获侧立即恢复常规灵敏度。
    fn handle_flush(&mut self) {
        self.buf.reset();
        self.duplex.stop_play();
        self.duplex.request_flush(self.total_written);
        self.last_frame_at = Instant::now();
        self.plc_run = 0;
    }

    /// 将解码 PCM（`self.pcm`，由 decode_into 填充）重采样后写入输出 ring。
    fn write_pcm(&mut self, used_plc: bool) {
        if !used_plc {
            // 帧长应等于 server_rate 的 60ms：不符说明服务器实际采样率与声明不一致（音调/语速错乱）。
            let expected = self.server_rate as usize * 60 / 1000;
            if self.pcm.len() != expected && !self.frame_len_warned {
                self.frame_len_warned = true;
                warn!(
                    "播放帧长异常: 期望 {} 实际 {}（服务器采样率与声明 {}Hz 不符）",
                    expected,
                    self.pcm.len(),
                    self.server_rate
                );
            }
            self.stats.decoded_ok += 1;
        }
        self.stats.last_pcm_len = self.pcm.len();
        self.stats.last_needed = self.resampler.input_frames_next();
        self.out_samples.clear();
        self.resampler
            .process(&self.pcm, self.drift.ratio, &mut self.out_samples);
        self.stats.last_produced = self.out_samples.len();
        if self.out_samples.is_empty() {
            self.stats.zero_produced += 1;
        }
        let written = write_samples(
            &mut self.out,
            &self.out_samples,
            self.output_rate,
            OUTPUT_RING_SAMPLES,
        );
        self.total_written += written as u64;
        self.stats.written += written as u64;
    }

    /// 丢包恢复：请求带内 FEC/PLC 解码一帧（60ms），保持音频流连续。
    fn play_plc(&mut self) {
        // 真实丢包会在 1-2 帧内恢复；再久就是句间静默，继续补帧会制造无限拖尾，
        // 并让播放活跃窗口永不到期（TTS 结束后 VAD 仍被强化抑制）。
        if self.plc_run >= MAX_PLC_FRAMES {
            return;
        }
        self.plc_run += 1;
        if self.plc_run == MAX_PLC_FRAMES {
            debug!("连续 {} 帧无下行，停止 PLC 补帧", MAX_PLC_FRAMES);
        }
        match self.decoder.decode_into(&[], true, &mut self.pcm) {
            Ok(()) => {
                self.stats.plc += 1;
                // 恢复帧仍在发声，播放窗口须续期，否则丢包期间抑制会失效。
                self.duplex.touch_play(self.play_tail_ms);
                self.write_pcm(true);
            }
            Err(e) => warn!("PLC 失败: {}", e),
        }
    }

    /// 每迭代执行：2s 诊断输出 + 500ms 漂移校正。
    fn diag_and_drift(&mut self) {
        if self.stats.flush_at.elapsed() >= Duration::from_secs(2) {
            let cb = self.cb_count.swap(0, Ordering::Relaxed);
            let cbs = self.cb_samples.swap(0, Ordering::Relaxed);
            let underruns = self.underruns.swap(0, Ordering::Relaxed);
            let occupancy_ms = (OUTPUT_RING_SAMPLES.saturating_sub(self.out.slots()) as f64
                / self.output_rate as f64)
                * 1000.0;
            debug!(
                "播放诊断: 收帧={}, 解码成功={}, 失败={}, PLC={}, 丢弃={}, 写入={}, pcm={}, needed={}, 产出={}, 零产出={} | 回调={}次, 消费样本={}, 欠载={}次, 占用={:.0}ms/目标={}ms, 漂移={:+.0}ppm",
                self.stats.received,
                self.stats.decoded_ok,
                self.stats.decoded_fail,
                self.stats.plc,
                self.stats.trimmed,
                self.stats.written,
                self.stats.last_pcm_len,
                self.stats.last_needed,
                self.stats.last_produced,
                self.stats.zero_produced,
                cb,
                cbs,
                underruns,
                occupancy_ms,
                self.buf.target_ms(),
                self.drift.current_ppm()
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
                // 占用高于目标 → 降低产出比率（负反馈），低于目标 → 提高。
                let err_ppm = occupancy_ppm(occupancy_ms, target_ms);
                self.drift.move_toward(err_ppm);
            }
            self.last_drift = Instant::now();
        }
    }
}

/// 播放缓冲占用误差 → 重采样比率修正（ppm）：占用高则降速、低则提速（负反馈）。
fn occupancy_ppm(occupancy_ms: f64, target_ms: f64) -> f64 {
    if target_ms <= 0.0 {
        return 0.0;
    }
    ((target_ms - occupancy_ms) / target_ms * 1e6 * 0.5).clamp(-MAX_DRIFT_PPM, MAX_DRIFT_PPM)
}

/// 写入播放环形缓冲，返回实际写入样本数；空间不足时丢弃多余（新）样本以封顶延迟。
fn write_samples(
    out: &mut Producer<f32>,
    samples: &[f32],
    output_rate: u32,
    capacity: usize,
) -> usize {
    let free = out.slots();
    let occupancy_ms = ((capacity - free) as f64 / output_rate as f64) * 1000.0;
    // 占用超 280ms 时整体丢弃本轮，让缓冲自然回落。
    if occupancy_ms > 280.0 {
        return 0;
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
    written
}

#[cfg(test)]
mod tests {
    use super::vad::test_config;
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

    /// 防抖确认后补发起始帧：两帧语音都应编码，首帧不被吞。
    #[tokio::test]
    async fn onset_frames_are_flushed_on_trigger() {
        let duplex = Arc::new(DuplexSignal::new());
        let mut worker = CaptureWorker::new(16_000, test_config(), duplex).unwrap();
        let slot = LatestSlot::<Vec<u8>>::new();
        worker.pending.extend(make_frame(0.02, ENCODER_FRAME_SIZE));
        worker.pending.extend(make_frame(0.02, ENCODER_FRAME_SIZE));
        worker.encode_pending(&slot).await;
        assert_eq!(worker.stats.frames, 2, "防抖期首帧未被补发");
        assert_eq!(worker.stats.speech_frames, 2);
    }

    /// 播放 worker 测试夹具：返回 worker、消息发送端、协同信号与输出环消费者。
    fn playback_fixture() -> (
        PlaybackWorker,
        mpsc::Sender<PlaybackMsg>,
        Arc<DuplexSignal>,
        rtrb::Consumer<f32>,
    ) {
        let (tx, rx) = mpsc::channel(32);
        let (producer, consumer) = rtrb::RingBuffer::new(8192);
        let duplex = Arc::new(DuplexSignal::new());
        let setup = PlaybackSetup {
            out: producer,
            server_rate: 16_000,
            output_rate: 16_000,
            play_tail_ms: 300,
            cb_count: Arc::new(AtomicU64::new(0)),
            cb_samples: Arc::new(AtomicU64::new(0)),
            underruns: Arc::new(AtomicU64::new(0)),
            duplex: duplex.clone(),
        };
        let worker = PlaybackWorker::new(rx, setup).unwrap();
        (worker, tx, duplex, consumer)
    }

    /// 无新包时 PLC 补帧封顶：空闲期不能无限续帧（否则播放窗口永不过期）。
    #[test]
    fn plc_frames_are_capped() {
        let (mut worker, _tx, _duplex, _consumer) = playback_fixture();
        for _ in 0..(MAX_PLC_FRAMES + 5) {
            worker.play_plc();
        }
        assert_eq!(worker.stats.plc, MAX_PLC_FRAMES as u64, "PLC 未按上限封顶");
    }

    /// 下行积压超过上限：丢弃最旧的过期音频，队列压回上限内。
    #[test]
    fn backlog_trim_drops_stale_audio() {
        let (mut worker, tx, _duplex, _consumer) = playback_fixture();
        for _ in 0..10 {
            assert!(tx.try_send(PlaybackMsg::Audio(vec![0; 2])).is_ok());
        }
        worker.trim_backlog();
        assert_eq!(worker.rx.len(), MAX_AUDIO_BACKLOG, "积压未压回上限");
        assert_eq!(worker.stats.trimmed, 5, "丢弃计数异常");
    }

    /// 积压中遇到 TTS 切换：Flush 必须生效，不能被丢帧逻辑吞掉。
    #[test]
    fn backlog_trim_preserves_flush() {
        let (mut worker, tx, duplex, _consumer) = playback_fixture();
        assert!(tx.try_send(PlaybackMsg::Flush).is_ok());
        for _ in 0..10 {
            assert!(tx.try_send(PlaybackMsg::Audio(vec![0; 2])).is_ok());
        }
        worker.trim_backlog();
        assert_eq!(duplex.flush_serial(), 1, "积压中的 Flush 被丢弃");
        assert_eq!(worker.rx.len(), MAX_AUDIO_BACKLOG);
        assert!(worker.stats.trimmed > 0, "未丢弃过期音频");
    }

    /// 播放占用校正必须是负反馈：占用高降速、占用低提速。
    #[test]
    fn occupancy_correction_is_negative_feedback() {
        assert!(occupancy_ppm(120.0, 60.0) < 0.0, "占用高应降速");
        assert!(occupancy_ppm(20.0, 60.0) > 0.0, "占用低应提速");
        assert!(occupancy_ppm(60.0, 60.0).abs() < 1e-9);
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
