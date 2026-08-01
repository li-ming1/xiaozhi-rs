//! 音频采集与播放。重采样采用 Catmull-Rom 三次样条，于块边界维持相位连续。

use anyhow::{anyhow, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, FromSample, SampleFormat, Stream, StreamConfig};
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// 目标采样率（Opus 窄带语音）。
pub const SAMPLE_RATE: u32 = 16000;
pub const FRAME_DURATION_MS: u32 = 20;
pub const FRAME_SIZE: usize = (SAMPLE_RATE * FRAME_DURATION_MS / 1000) as usize;

/// 抖动缓冲容量：25 帧 ≈ 500ms。余量过小会在网络突发/时钟漂移时丢新帧，引入可闻咔哒。
const FRAME_QUEUE_CAP: usize = 25;

/// 入队；溢出时丢最旧帧以封顶延迟，丢帧计数并限频告警（[DROP] 便于 grep）。
fn push_frame(queue: &mut VecDeque<[f32; FRAME_SIZE]>, dropped: &mut u64, frame: [f32; FRAME_SIZE], side: &str) {
    if queue.len() >= FRAME_QUEUE_CAP {
        queue.pop_front();
        *dropped += 1;
        if *dropped % 50 == 1 {
            warn!("[DROP] {}队列溢出（消费端落后），累计丢最旧帧 {} 次", side, dropped);
        }
    }
    queue.push_back(frame);
}

/// Catmull-Rom 三次样条重采样器，跨块复用前一输入末样本作 y0，抑制边界高频伪影。
/// 降采样（ratio<1）前置 sinc+hamming 低通 FIR，滤除高于目标奈奎斯特的成分，
/// 否则高频会混叠回中频带，产生金属感与齿音毛糙——这是降采样音质损失的主因。
struct Resampler {
    prev_last: f32,
    lowpass: Vec<f32>,   // 降采样抗混叠 FIR 系数；ratio>=1 时空，跳过滤波
    filtered: Vec<f32>,  // FIR 输出复用缓冲
}

impl Resampler {
    fn new(ratio: f64) -> Self {
        Self {
            prev_last: 0.0,
            lowpass: if ratio < 1.0 { design_lowpass(ratio) } else { Vec::new() },
            filtered: Vec::new(),
        }
    }

    /// 重采样至复用缓冲（输出长度 = input.len() * ratio），全程零堆分配。
    fn process(&mut self, input: &[f32], ratio: f64, output: &mut Vec<f32>) {
        output.clear();
        if input.is_empty() || ratio <= 0.0 {
            return;
        }

        // 降采样先低通抗混叠；升采样（ratio>=1）无混叠风险，直通。
        let src: &[f32] = if !self.lowpass.is_empty() {
            self.filtered.clear();
            apply_fir(input, &self.lowpass, &mut self.filtered);
            &self.filtered
        } else {
            input
        };

        let n = src.len();
        if n == 0 {
            return;
        }
        let last = n - 1;
        let output_len = (n as f64 * ratio) as usize;
        output.reserve(output_len);

        for i in 0..output_len {
            let src_pos = i as f64 / ratio;
            let src_idx = src_pos.floor() as usize;
            let frac = (src_pos - src_idx as f64) as f32;

            // Catmull-Rom 四点；y0 以 prev_last 封头，y2/y3 越界钳位至末样本。
            let y0 = if src_idx == 0 { self.prev_last } else { src[src_idx - 1] };
            let y1 = src[src_idx];
            let y2 = if src_idx < last { src[src_idx + 1] } else { src[last] };
            let y3 = if src_idx + 1 < last { src[src_idx + 2] } else { src[last] };

            let frac2 = frac * frac;
            let frac3 = frac2 * frac;
            let a0 = -0.5 * y0 + 1.5 * y1 - 1.5 * y2 + 0.5 * y3;
            let a1 = y0 - 2.5 * y1 + 2.0 * y2 - 0.5 * y3;
            let a2 = -0.5 * y0 + 0.5 * y2;

            output.push(a0 * frac3 + a1 * frac2 + a2 * frac + y1);
        }

        self.prev_last = src[last];
    }
}

/// 设计 sinc+hamming 窗低通 FIR，截止设在 0.8×目标奈奎斯特，归一化 DC 增益为 1。
/// ratio = out_rate / in_rate（<1 降采样），cutoff 为相对输入采样率的归一化频率。
fn design_lowpass(ratio: f64) -> Vec<f32> {
    const TAPS: usize = 15; // 奇数，线性相位；旁瓣抑制与算力折中
    let cutoff = ratio * 0.4; // 0.8 × 目标奈奎斯特（ratio/2），归一化相对输入采样率
    let mid = (TAPS - 1) as f64 / 2.0;
    let mut h = [0f32; TAPS];
    for i in 0..TAPS {
        let n = i as f64 - mid;
        let sinc = if n.abs() < 1e-9 {
            2.0 * cutoff
        } else {
            (2.0 * cutoff * std::f64::consts::PI * n).sin() / (std::f64::consts::PI * n)
        };
        let hamming = 0.54 - 0.46 * (2.0 * std::f64::consts::PI * n / (TAPS - 1) as f64).cos();
        h[i] = (sinc * hamming) as f32;
    }
    // 归一化 DC 增益为 1，避免通带电平偏移。
    let sum: f32 = h.iter().sum();
    if sum > 0.0 {
        for v in &mut h {
            *v /= sum;
        }
    }
    h.to_vec()
}

/// FIR 卷积，边界钳位（首尾样本重复），写入复用缓冲。
fn apply_fir(input: &[f32], h: &[f32], output: &mut Vec<f32>) {
    if input.is_empty() {
        return;
    }
    let taps = h.len();
    let half = (taps / 2) as isize;
    let last = input.len() as isize - 1;
    output.reserve(input.len());
    for i in 0..input.len() {
        let mut acc = 0f32;
        let base = i as isize - half;
        for j in 0..taps {
            let idx = (base + j as isize).clamp(0, last) as usize;
            acc += h[j] * input[idx];
        }
        output.push(acc);
    }
}

/// 输入端共享状态：环形缓冲 + 有界帧队列 + 重采样器，单次加锁。
struct InputState {
    buffer: VecDeque<f32>,
    queue: VecDeque<[f32; FRAME_SIZE]>,
    dropped: u64,
    resampler: Resampler,
    mono_buf: Vec<f32>,   // 复用，免每帧分配
    resampled: Vec<f32>,  // 复用，免每帧分配
}

impl InputState {
    fn new(ratio: f64) -> Self {
        Self {
            buffer: VecDeque::with_capacity(FRAME_SIZE * 4),
            queue: VecDeque::with_capacity(FRAME_QUEUE_CAP),
            dropped: 0,
            resampler: Resampler::new(ratio),
            mono_buf: Vec::new(),
            resampled: Vec::with_capacity(FRAME_SIZE * 2),
        }
    }
}

/// 输出端共享状态：单声道缓冲 + 有界帧队列 + 重采样器，单次加锁。
struct OutputState {
    buffer: VecDeque<f32>,  // 单声道，播放时按 channels 展开
    queue: VecDeque<[f32; FRAME_SIZE]>,
    dropped: u64,
    resampler: Resampler,
    resampled: Vec<f32>,
}

impl OutputState {
    fn new(ratio: f64) -> Self {
        Self {
            buffer: VecDeque::with_capacity(4096),
            queue: VecDeque::with_capacity(FRAME_QUEUE_CAP),
            dropped: 0,
            resampler: Resampler::new(ratio),
            resampled: Vec::with_capacity(FRAME_SIZE * 2),
        }
    }
}

/// 音频管理器：封装 cpal 流与双端共享状态。
pub struct AudioManager {
    input_device: Device,
    output_device: Device,
    input_stream: Option<Stream>,
    output_stream: Option<Stream>,
    input_state: Option<Arc<Mutex<InputState>>>,
    output_state: Option<Arc<Mutex<OutputState>>>,
}

impl AudioManager {
    /// 探测默认设备并构造管理器（设备缺失即报错）。
    pub fn new() -> Result<Self> {
        let host = cpal::default_host();

        let input_device = host
            .default_input_device()
            .ok_or_else(|| anyhow!("未找到输入设备"))?;

        let output_device = host
            .default_output_device()
            .ok_or_else(|| anyhow!("未找到输出设备"))?;

        // cpal 0.18 的 Device 实现 Display，{} 即设备名。
        info!("输入设备: {}", input_device);
        info!("输出设备: {}", output_device);

        Ok(Self {
            input_device,
            output_device,
            input_stream: None,
            output_stream: None,
            input_state: None,
            output_state: None,
        })
    }

    /// 开始采集音频
    pub fn start_capture(&mut self) -> Result<()> {
        let supported_config = self
            .input_device
            .default_input_config()
            .map_err(|e| anyhow!("无法获取输入配置: {}", e))?;

        info!("输入配置: {:?}", supported_config);

        let sample_format = supported_config.sample_format();
        let config: StreamConfig = supported_config.into();
        let input_sample_rate = config.sample_rate;
        let input_channels = config.channels as usize;

        let resample_ratio = SAMPLE_RATE as f64 / input_sample_rate as f64;

        let state = Arc::new(Mutex::new(InputState::new(resample_ratio)));
        self.input_state = Some(state.clone());

        let stream = match sample_format {
            SampleFormat::I16 => build_input_stream(
                &self.input_device, config, input_channels,
                |s: i16| s as f32 / 32768.0, resample_ratio, &state,
            )?,
            SampleFormat::F32 => build_input_stream(
                &self.input_device, config, input_channels,
                |s: f32| s, resample_ratio, &state,
            )?,
            _ => return Err(anyhow!("不支持的采样格式: {:?}", sample_format)),
        };

        stream.play()?;
        self.input_stream = Some(stream);
        info!("音频采集已启动 ({}Hz -> {}Hz)", input_sample_rate, SAMPLE_RATE);
        Ok(())
    }

    /// 开始播放音频
    pub fn start_playback(&mut self) -> Result<()> {
        let supported_config = self
            .output_device
            .default_output_config()
            .map_err(|e| anyhow!("无法获取输出配置: {}", e))?;

        info!("输出配置: {:?}", supported_config);

        let sample_format = supported_config.sample_format();
        let config: StreamConfig = supported_config.into();
        let output_sample_rate = config.sample_rate;
        let output_channels = config.channels as usize;

        let resample_ratio = output_sample_rate as f64 / SAMPLE_RATE as f64;

        let state = Arc::new(Mutex::new(OutputState::new(resample_ratio)));
        self.output_state = Some(state.clone());

        let stream = match sample_format {
            SampleFormat::I16 => build_output_stream::<i16>(
                &self.output_device, config, output_channels, resample_ratio, &state,
            )?,
            SampleFormat::F32 => build_output_stream::<f32>(
                &self.output_device, config, output_channels, resample_ratio, &state,
            )?,
            SampleFormat::U16 => build_output_stream::<u16>(
                &self.output_device, config, output_channels, resample_ratio, &state,
            )?,
            _ => return Err(anyhow!("不支持的采样格式: {:?}", sample_format)),
        };

        stream.play()?;
        self.output_stream = Some(stream);
        info!("音频播放已启动 ({}Hz -> {}Hz)", SAMPLE_RATE, output_sample_rate);
        Ok(())
    }

    /// 从输入队列取一帧。
    pub fn read_frame(&self) -> Option<[f32; FRAME_SIZE]> {
        self.input_state.as_ref()?.lock().unwrap().queue.pop_front()
    }

    /// 写入输出队列（满丢最旧帧）。
    pub fn write_frame(&self, frame: [f32; FRAME_SIZE]) {
        if let Some(state) = &self.output_state {
            // 显式 deref 以解锁字段级 disjoint borrow。
            let st = &mut *state.lock().unwrap();
            push_frame(&mut st.queue, &mut st.dropped, frame, "输出");
        }
    }

    /// 停止流并清空队列，避免重连后回放历史积压。
    pub fn stop(&mut self) {
        self.input_stream = None;
        self.output_stream = None;
        if let Some(state) = &self.input_state {
            let mut st = state.lock().unwrap();
            st.queue.clear();
            st.buffer.clear();
        }
        if let Some(state) = &self.output_state {
            let mut st = state.lock().unwrap();
            st.queue.clear();
            st.buffer.clear();
        }
    }
}

/// 构建输入流；泛型样本类型消除 I16/F32 分支。
fn build_input_stream<S>(
    device: &Device,
    config: StreamConfig,
    channels: usize,
    to_f32: fn(S) -> f32,
    ratio: f64,
    state: &Arc<Mutex<InputState>>,
) -> Result<Stream>
where
    S: cpal::SizedSample + Copy + Send + 'static,
{
    let state = state.clone();
    let stream = device.build_input_stream(
        config,
        move |data: &[S], _: &_| {
            process_input_chunk(data, channels, to_f32, ratio, &state);
        },
        |err| warn!("输入流错误: {}", err),
        None,
    )?;
    Ok(stream)
}

/// 构建输出流；泛型样本类型消除 I16/F32/U16 分支。
fn build_output_stream<T>(
    device: &Device,
    config: StreamConfig,
    channels: usize,
    ratio: f64,
    state: &Arc<Mutex<OutputState>>,
) -> Result<Stream>
where
    T: FromF32Sample + cpal::SizedSample + Send + 'static,
{
    let state = state.clone();
    let stream = device.build_output_stream(
        config,
        move |data: &mut [T], _: &_| {
            fill_output(data, channels, ratio, &state);
        },
        |err| warn!("输出流错误: {}", err),
        None,
    )?;
    Ok(stream)
}

/// f32 → 输出样本的转换 trait。继承 Copy：单样本转码一次后展开至 channels 槽，零成本。
trait FromF32Sample: Copy + FromSample<f32> {
    fn from_f32_sample(v: f32) -> Self {
        <Self as FromSample<f32>>::from_sample_(v)
    }
}

impl<T> FromF32Sample for T where T: Copy + FromSample<f32> {}

/// 输入回调：混为单声道 → 重采样 → 入缓冲 → 满帧入队。
fn process_input_chunk<S: Copy>(
    samples: &[S],
    channels: usize,
    to_f32: impl Fn(S) -> f32,
    ratio: f64,
    state: &Arc<Mutex<InputState>>,
) {
    let mut s = state.lock().unwrap();
    // 显式 deref 解锁字段级 disjoint borrow（resampler 与缓冲同属结构体字段）。
    let st = &mut *s;
    st.mono_buf.clear();
    if channels == 1 {
        st.mono_buf.extend(samples.iter().map(|x| to_f32(*x)));
    } else {
        st.mono_buf.extend(
            samples
                .chunks(channels)
                .map(|ch| ch.iter().map(|x| to_f32(*x)).sum::<f32>() / channels as f32),
        );
    }
    st.resampler.process(&st.mono_buf, ratio, &mut st.resampled);
    st.buffer.extend(st.resampled.iter().copied());
    while st.buffer.len() >= FRAME_SIZE {
        // 栈数组收帧，免每帧堆分配；VecDeque::drain 仅移动所需元素。
        let mut frame = [0f32; FRAME_SIZE];
        for (dst, src) in frame.iter_mut().zip(st.buffer.drain(..FRAME_SIZE)) {
            *dst = src;
        }
        push_frame(&mut st.queue, &mut st.dropped, frame, "输入");
    }
}

/// 输出回调：取帧 → 重采样 → 单声道入缓冲 → 按 channels 展开填充。
/// 缓冲仅存单声道（空间减半），展开时每样本转码一次即复制，避免重复转码。
fn fill_output<T: FromF32Sample>(
    data: &mut [T],
    channels: usize,
    ratio: f64,
    state: &Arc<Mutex<OutputState>>,
) {
    let mut s = state.lock().unwrap();
    let st = &mut *s;
    while let Some(frame) = st.queue.pop_front() {
        st.resampler.process(&frame, ratio, &mut st.resampled);
        st.buffer.extend(st.resampled.iter().copied());
    }
    // make_contiguous 后整块拷贝，替代逐样本 pop_front（48k 立体声下每秒约 10 万次调用 → 单次 memcpy）。
    let frames_needed = data.len() / channels;
    let avail = st.buffer.len().min(frames_needed);
    if avail > 0 {
        let contiguous = st.buffer.make_contiguous();
        for (chunk, &v) in data.chunks_mut(channels).zip(contiguous[..avail].iter()) {
            let s = T::from_f32_sample(v);
            for dst in chunk {
                *dst = s;
            }
        }
        st.buffer.drain(..avail);
    }
    // 欠载补静音。
    for dst in &mut data[avail * channels..] {
        *dst = T::from_f32_sample(0.0);
    }
}
