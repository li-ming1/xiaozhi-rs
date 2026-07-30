//! 音频采集和播放
//!
//! 支持高质量重采样（Catmull-Rom 三次样条，带块边界相位连续性）

use anyhow::{anyhow, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, SampleFormat, Stream, StreamConfig};
use log::{info, warn};
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};

/// 音频配置
pub const SAMPLE_RATE: u32 = 16000;
pub const FRAME_DURATION_MS: u32 = 20;
pub const FRAME_SIZE: usize = (SAMPLE_RATE * FRAME_DURATION_MS / 1000) as usize;

/// 高质量重采样器（Catmull-Rom 三次样条，保持跨块相位连续）
///
/// 保存上一次输入的最后 2 个样本，作为下次插值的 y0/y1，
/// 避免块边界处波形不连续产生的高频杂音。
struct Resampler {
    tail: [f32; 2],
}

impl Resampler {
    fn new() -> Self {
        Self { tail: [0.0, 0.0] }
    }

    /// 重采样：输出长度 = input.len() * ratio
    fn process(&mut self, input: &[f32], ratio: f64) -> Vec<f32> {
        if input.is_empty() || ratio <= 0.0 {
            return Vec::new();
        }

        let output_len = (input.len() as f64 * ratio) as usize;
        let mut output = Vec::with_capacity(output_len);

        // 构造带历史前缀的输入：[tail0, tail1, input...]
        // Catmull-Rom 需要当前点的前后各一个点；加前缀保证块起始处也能取到正确 y0/y1
        let mut extended = Vec::with_capacity(input.len() + 2);
        extended.extend_from_slice(&self.tail);
        extended.extend_from_slice(input);

        for i in 0..output_len {
            let src_pos = i as f64 / ratio;
            let src_idx = src_pos.floor() as usize;
            let frac = (src_pos - src_idx as f64) as f32;

            // extended 中 tail 在 [0,2)，input 在 [2, 2+n)
            // 输出 i 对应 input[src_idx]，即 extended[src_idx+2]
            // Catmull-Rom 四点取 extended[src_idx+1..src_idx+5]
            let y0 = extended.get(src_idx + 1).copied().unwrap_or(0.0);
            let y1 = extended.get(src_idx + 2).copied().unwrap_or(0.0);
            let y2 = extended.get(src_idx + 3).copied().unwrap_or(0.0);
            let y3 = extended.get(src_idx + 4).copied().unwrap_or(0.0);

            let frac2 = frac * frac;
            let frac3 = frac2 * frac;
            let a0 = -0.5 * y0 + 1.5 * y1 - 1.5 * y2 + 0.5 * y3;
            let a1 = y0 - 2.5 * y1 + 2.0 * y2 - 0.5 * y3;
            let a2 = -0.5 * y0 + 0.5 * y2;

            output.push(a0 * frac3 + a1 * frac2 + a2 * frac + y1);
        }

        // 更新 tail 为本次输入的最后 2 个样本
        if input.len() >= 2 {
            self.tail = [input[input.len() - 2], input[input.len() - 1]];
        } else if input.len() == 1 {
            self.tail = [self.tail[1], input[0]];
        }

        output
    }
}

/// 输入端共享状态（缓冲 + 重采样器），单次加锁
struct InputState {
    buffer: Vec<f32>,
    resampler: Resampler,
}

impl InputState {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(FRAME_SIZE * 4),
            resampler: Resampler::new(),
        }
    }
}

/// 输出端共享状态（缓冲 + 重采样器），单次加锁
struct OutputState {
    buffer: VecDeque<f32>,
    resampler: Resampler,
}

impl OutputState {
    fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(4096),
            resampler: Resampler::new(),
        }
    }
}

/// 音频管理器
#[allow(dead_code)]
pub struct AudioManager {
    #[allow(dead_code)]
    host: Host,
    input_device: Device,
    output_device: Device,
    input_stream: Option<Stream>,
    output_stream: Option<Stream>,
    input_rx: Option<Receiver<Vec<f32>>>,
    output_tx: Option<Sender<Vec<f32>>>,
}

impl AudioManager {
    /// 创建音频管理器
    pub fn new() -> Result<Self> {
        let host = cpal::default_host();

        let input_device = host
            .default_input_device()
            .ok_or_else(|| anyhow!("未找到输入设备"))?;

        let output_device = host
            .default_output_device()
            .ok_or_else(|| anyhow!("未找到输出设备"))?;

        info!("输入设备: {}", input_device.name().unwrap_or_default());
        info!("输出设备: {}", output_device.name().unwrap_or_default());

        Ok(Self {
            host,
            input_device,
            output_device,
            input_stream: None,
            output_stream: None,
            input_rx: None,
            output_tx: None,
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
        let input_sample_rate = config.sample_rate.0;
        let input_channels = config.channels as usize;

        let (tx, rx) = channel::<Vec<f32>>();
        self.input_rx = Some(rx);

        // 重采样比例：目标 16kHz / 输入采样率
        let resample_ratio = SAMPLE_RATE as f64 / input_sample_rate as f64;

        // 共享状态：缓冲 + 重采样器（单次加锁）
        let state = Arc::new(Mutex::new(InputState::new()));

        let stream = match sample_format {
            SampleFormat::I16 => {
                let state = state.clone();
                self.input_device.build_input_stream(
                    &config,
                    move |data: &[i16], _: &_| {
                        let mono = to_mono_f32(data, input_channels, |s| s as f32 / 32768.0);
                        process_input_chunk(&mono, resample_ratio, &state, &tx);
                    },
                    |err| warn!("输入流错误: {}", err),
                    None,
                )?
            }
            SampleFormat::F32 => {
                let state = state.clone();
                self.input_device.build_input_stream(
                    &config,
                    move |data: &[f32], _: &_| {
                        let mono = to_mono_f32(data, input_channels, |s| s);
                        process_input_chunk(&mono, resample_ratio, &state, &tx);
                    },
                    |err| warn!("输入流错误: {}", err),
                    None,
                )?
            }
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
        let output_sample_rate = config.sample_rate.0;
        let output_channels = config.channels as usize;

        let (tx, rx): (Sender<Vec<f32>>, Receiver<Vec<f32>>) = channel();
        self.output_tx = Some(tx);

        // 重采样比例：输出采样率 / 16kHz
        let resample_ratio = output_sample_rate as f64 / SAMPLE_RATE as f64;

        // 共享状态：VecDeque 缓冲（pop_front O(1)）+ 重采样器
        let state = Arc::new(Mutex::new(OutputState::new()));

        let stream = match sample_format {
            SampleFormat::I16 => {
                let state = state.clone();
                self.output_device.build_output_stream(
                    &config,
                    move |data: &mut [i16], _: &_| {
                        fill_output(data, output_channels, resample_ratio, &state, &rx);
                    },
                    |err| warn!("输出流错误: {}", err),
                    None,
                )?
            }
            SampleFormat::F32 => {
                let state = state.clone();
                self.output_device.build_output_stream(
                    &config,
                    move |data: &mut [f32], _: &_| {
                        fill_output(data, output_channels, resample_ratio, &state, &rx);
                    },
                    |err| warn!("输出流错误: {}", err),
                    None,
                )?
            }
            _ => return Err(anyhow!("不支持的采样格式: {:?}", sample_format)),
        };

        stream.play()?;
        self.output_stream = Some(stream);
        info!("音频播放已启动 ({}Hz -> {}Hz)", SAMPLE_RATE, output_sample_rate);
        Ok(())
    }

    /// 读取音频帧（从输入通道）
    pub fn read_frame(&self) -> Option<Vec<f32>> {
        self.input_rx.as_ref()?.try_recv().ok()
    }

    /// 写入音频帧（到输出通道）
    pub fn write_frame(&self, frame: Vec<f32>) {
        if let Some(tx) = &self.output_tx {
            if tx.send(frame).is_err() {
                warn!("输出通道已关闭");
            }
        }
    }
}

/// 样本到 f32 的转换 trait（消除输出回调 I16/F32 重复）
trait FromF32Sample {
    fn from_f32_sample(v: f32) -> Self;
}
impl FromF32Sample for i16 {
    fn from_f32_sample(v: f32) -> Self {
        (v * 32767.0) as i16
    }
}
impl FromF32Sample for f32 {
    fn from_f32_sample(v: f32) -> Self {
        v
    }
}

/// 多声道样本转 f32 单声道
fn to_mono_f32<S: Copy>(samples: &[S], channels: usize, to_f32: impl Fn(S) -> f32) -> Vec<f32> {
    if channels == 1 {
        samples.iter().map(|s| to_f32(*s)).collect()
    } else {
        samples
            .chunks(channels)
            .map(|ch| ch.iter().map(|s| to_f32(*s)).sum::<f32>() / channels as f32)
            .collect()
    }
}

/// 输入回调共用处理：重采样 → 入缓冲 → 取完整帧发送
fn process_input_chunk(
    mono: &[f32],
    ratio: f64,
    state: &Arc<Mutex<InputState>>,
    tx: &Sender<Vec<f32>>,
) {
    let mut s = state.lock().unwrap();
    let resampled = s.resampler.process(mono, ratio);
    s.buffer.extend_from_slice(&resampled);
    while s.buffer.len() >= FRAME_SIZE {
        let frame: Vec<f32> = s.buffer.drain(..FRAME_SIZE).collect();
        if tx.send(frame).is_err() {
            warn!("音频通道已关闭");
            return;
        }
    }
}

/// 输出回调：拉取数据 → 重采样 → 单声道展开 → 填充输出（pop_front O(1)）
fn fill_output<T: FromF32Sample>(
    data: &mut [T],
    channels: usize,
    ratio: f64,
    state: &Arc<Mutex<OutputState>>,
    rx: &Receiver<Vec<f32>>,
) {
    let mut s = state.lock().unwrap();
    // 拉取所有待播放数据，重采样后单声道展开入缓冲
    while let Ok(chunk) = rx.try_recv() {
        let resampled = s.resampler.process(&chunk, ratio);
        for sample in resampled {
            for _ in 0..channels {
                s.buffer.push_back(sample);
            }
        }
    }
    // 填充输出缓冲（pop_front O(1)，替代旧实现的 O(n) remove(0)）
    for sample in data.iter_mut() {
        *sample = T::from_f32_sample(s.buffer.pop_front().unwrap_or(0.0));
    }
}
