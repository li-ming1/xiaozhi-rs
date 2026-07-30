//! 音频采集和播放
//!
//! 支持重采样（48000Hz → 16000Hz）

use anyhow::{anyhow, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, SampleFormat, Stream, StreamConfig};
use log::{info, warn};
use std::sync::mpsc::{channel, Receiver, Sender};

/// 音频配置
pub const SAMPLE_RATE: u32 = 16000;
pub const FRAME_DURATION_MS: u32 = 20;
pub const FRAME_SIZE: usize = (SAMPLE_RATE * FRAME_DURATION_MS / 1000) as usize;

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

        // 创建通道
        let (tx, rx) = channel::<Vec<f32>>();
        self.input_rx = Some(rx);

        // 重采样比例
        let resample_ratio = SAMPLE_RATE as f64 / input_sample_rate as f64;

        let stream = match sample_format {
            SampleFormat::I16 => {
                self.input_device.build_input_stream(
                    &config,
                    move |data: &[i16], _: &_| {
                        // 转换为 f32
                        let samples: Vec<f32> = data
                            .iter()
                            .map(|s| *s as f32 / 32768.0)
                            .collect();
                        
                        // 多声道转单声道
                        let mono: Vec<f32> = samples
                            .chunks(input_channels)
                            .map(|ch| ch.iter().sum::<f32>() / input_channels as f32)
                            .collect();
                        
                        // 重采样
                        let resampled = resample(&mono, resample_ratio);
                        
                        // 发送到通道
                        if tx.send(resampled).is_err() {
                            warn!("音频通道已关闭");
                        }
                    },
                    |err| warn!("输入流错误: {}", err),
                    None,
                )?
            }
            SampleFormat::F32 => {
                self.input_device.build_input_stream(
                    &config,
                    move |data: &[f32], _: &_| {
                        // 多声道转单声道
                        let mono: Vec<f32> = data
                            .chunks(input_channels)
                            .map(|ch| ch.iter().sum::<f32>() / input_channels as f32)
                            .collect();
                        
                        // 重采样
                        let resampled = resample(&mono, resample_ratio);
                        
                        // 发送到通道
                        if tx.send(resampled).is_err() {
                            warn!("音频通道已关闭");
                        }
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

        // 创建通道
        let (tx, rx): (Sender<Vec<f32>>, Receiver<Vec<f32>>) = channel();
        self.output_tx = Some(tx);

        // 重采样比例（16kHz → 输出采样率）
        let resample_ratio = output_sample_rate as f64 / SAMPLE_RATE as f64;

        // 缓冲区
        let mut buffer: Vec<f32> = Vec::with_capacity(4096);

        let stream = match sample_format {
            SampleFormat::I16 => {
                self.output_device.build_output_stream(
                    &config,
                    move |data: &mut [i16], _: &_| {
                        // 尝试从通道获取数据
                        while let Ok(chunk) = rx.try_recv() {
                            // 重采样到输出采样率
                            let resampled = resample(&chunk, resample_ratio);
                            // 单声道转多声道
                            for sample in resampled {
                                for _ in 0..output_channels {
                                    buffer.push(sample);
                                }
                            }
                        }
                        
                        // 填充输出
                        for sample in data.iter_mut() {
                            *sample = if buffer.is_empty() {
                                0
                            } else {
                                (buffer.remove(0) * 32767.0) as i16
                            };
                        }
                    },
                    |err| warn!("输出流错误: {}", err),
                    None,
                )?
            }
            SampleFormat::F32 => {
                self.output_device.build_output_stream(
                    &config,
                    move |data: &mut [f32], _: &_| {
                        // 尝试从通道获取数据
                        while let Ok(chunk) = rx.try_recv() {
                            // 重采样到输出采样率
                            let resampled = resample(&chunk, resample_ratio);
                            // 单声道转多声道
                            for sample in resampled {
                                for _ in 0..output_channels {
                                    buffer.push(sample);
                                }
                            }
                        }
                        
                        // 填充输出
                        for sample in data.iter_mut() {
                            *sample = if buffer.is_empty() { 0.0 } else { buffer.remove(0) };
                        }
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

    /// 停止采集
    #[allow(dead_code)]
    pub fn stop_capture(&mut self) {
        self.input_stream = None;
        self.input_rx = None;
        info!("音频采集已停止");
    }

    /// 停止播放
    #[allow(dead_code)]
    pub fn stop_playback(&mut self) {
        self.output_stream = None;
        self.output_tx = None;
        info!("音频播放已停止");
    }
}

/// 简单线性插值重采样
fn resample(input: &[f32], ratio: f64) -> Vec<f32> {
    if input.is_empty() || ratio <= 0.0 {
        return Vec::new();
    }

    let output_len = (input.len() as f64 * ratio) as usize;
    let mut output = Vec::with_capacity(output_len);

    for i in 0..output_len {
        let src_pos = i as f64 / ratio;
        let src_idx = src_pos as usize;
        
        if src_idx + 1 < input.len() {
            // 线性插值
            let frac = src_pos - src_idx as f64;
            let sample = input[src_idx] as f64 * (1.0 - frac) + input[src_idx + 1] as f64 * frac;
            output.push(sample as f32);
        } else if src_idx < input.len() {
            output.push(input[src_idx]);
        } else {
            output.push(0.0);
        }
    }

    output
}