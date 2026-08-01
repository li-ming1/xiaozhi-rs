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
/// 播放缓冲目标深度（帧数）：稳态约 60ms 存量，抗到达抖动；启动/突发期一次补足，避免首字欠载卡顿。
const TARGET_FRAMES: usize = 4;
/// 输出缓冲读游标压缩阈值（样本）：head 达到该值才搬移一次，替代稳态下逐回调搬移。
const COMPACT_THRESHOLD: usize = 8192;

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

/// Catmull-Rom 四点插值（Horner 求值，与原展开式数学等价，乘法由 5 次降为 3 次）。
#[inline(always)]
fn catmull_rom(y0: f32, y1: f32, y2: f32, y3: f32, t: f32) -> f32 {
    let a0 = -0.5 * y0 + 1.5 * y1 - 1.5 * y2 + 0.5 * y3;
    let a1 = y0 - 2.5 * y1 + 2.0 * y2 - 0.5 * y3;
    let a2 = -0.5 * y0 + 0.5 * y2;
    ((a0 * t + a1) * t + a2) * t + y1
}

/// Catmull-Rom 三次样条重采样器，跨块复用前一输入末样本作 y0，抑制边界高频伪影。
struct Resampler {
    prev_last: f32,
}

impl Resampler {
    fn new() -> Self {
        Self { prev_last: 0.0 }
    }

    /// 重采样至复用缓冲（输出长度 = input.len() * ratio），全程零堆分配。
    /// 源位置以增量累加免每样本除法；主循环剥离边界分支，利于自动向量化。
    fn process(&mut self, input: &[f32], ratio: f64, output: &mut Vec<f32>) {
        let n = input.len();
        let output_len = if n == 0 || ratio <= 0.0 { 0 } else { (n as f64 * ratio) as usize };
        output.clear();
        if output_len == 0 {
            return;
        }
        output.reserve(output_len);

        let last = n - 1;
        let inv_ratio = 1.0 / ratio;
        let mut src_pos = 0.0f64;
        let mut i = 0usize;

        // 前导段：src_idx == 0，y0 取上一块末样本封头（至多 ceil(ratio) 个输出样本）。
        while i < output_len && (src_pos as usize) == 0 {
            let t = (src_pos - (src_pos as usize) as f64) as f32;
            output.push(catmull_rom(
                self.prev_last,
                input[0],
                input[1.min(last)],
                input[2.min(last)],
                t,
            ));
            i += 1;
            src_pos += inv_ratio;
        }

        // 主循环：src_idx ∈ [1, n-3]，四点均在界内，无边界分支。
        while i < output_len {
            let src_idx = src_pos as usize;
            if src_idx + 2 >= n {
                break;
            }
            let t = (src_pos - src_idx as f64) as f32;
            output.push(catmull_rom(
                input[src_idx - 1],
                input[src_idx],
                input[src_idx + 1],
                input[src_idx + 2],
                t,
            ));
            i += 1;
            src_pos += inv_ratio;
        }

        // 尾部段：src_idx ∈ [n-2, n-1]，y2/y3 越界钳位至末样本。
        while i < output_len {
            let src_idx = src_pos as usize;
            let t = (src_pos - src_idx as f64) as f32;
            let y0 = if src_idx == 0 { self.prev_last } else { input[src_idx - 1] };
            let y1 = input[src_idx.min(last)];
            let y2 = if src_idx < last { input[src_idx + 1] } else { input[last] };
            let y3 = if src_idx + 1 < last { input[src_idx + 2] } else { input[last] };
            output.push(catmull_rom(y0, y1, y2, y3, t));
            i += 1;
            src_pos += inv_ratio;
        }

        self.prev_last = input[last];
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
    fn new() -> Self {
        Self {
            buffer: VecDeque::with_capacity(FRAME_SIZE * 4),
            queue: VecDeque::with_capacity(FRAME_QUEUE_CAP),
            dropped: 0,
            resampler: Resampler::new(),
            mono_buf: Vec::with_capacity(FRAME_SIZE * 4),
            resampled: Vec::with_capacity(FRAME_SIZE * 4),
        }
    }
}

/// 输出端共享状态：单声道连续缓冲（读游标 head 延迟压缩）+ 有界帧队列 + 重采样器 + 欠载防咔哒状态，单次加锁。
struct OutputState {
    buffer: Vec<f32>,     // 单声道连续存储，播放时按 channels 展开；head 为已消费游标
    head: usize,          // 读游标：有效数据为 buffer[head..]，避免每回调搬移
    queue: VecDeque<[f32; FRAME_SIZE]>,
    dropped: u64,
    resampler: Resampler,
    resampled: Vec<f32>,
    last_out: f32,        // 最近一次正常输出的样本
    ramp_pending: bool,   // 正常输出后置 true，欠载时启动斜坡
    ramp_left: usize,     // 剩余斜坡样本数
    ramp_len: usize,      // 斜坡总长（5ms 按输出采样率折算），首次调用初始化
    target_samples: usize, // 目标缓冲深度（稳态恒定），首次调用缓存
}

impl OutputState {
    fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096),
            head: 0,
            queue: VecDeque::with_capacity(FRAME_QUEUE_CAP),
            dropped: 0,
            resampler: Resampler::new(),
            resampled: Vec::with_capacity(FRAME_SIZE * 4),
            last_out: 0.0,
            ramp_pending: false,
            ramp_left: 0,
            ramp_len: 0,
            target_samples: 0,
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

        let state = Arc::new(Mutex::new(InputState::new()));
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

        let state = Arc::new(Mutex::new(OutputState::new()));
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

    /// 清空播放缓冲并重置斜坡/重采样状态（TTS 开始前调用，防尾帧串扰与首字卡顿）。
    pub fn clear_playback(&self) {
        if let Some(state) = &self.output_state {
            let mut st = state.lock().unwrap();
            st.queue.clear();
            st.buffer.clear();
            st.head = 0;
            st.ramp_pending = false;
            st.ramp_left = 0;
            st.last_out = 0.0;
            st.resampler = Resampler::new();
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
            st.head = 0;
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
    } else if channels == 2 {
        // 立体声特化：chunks_exact 免边界检查，平均以 0.5 乘法替代除法。
        st.mono_buf.extend(
            samples
                .chunks_exact(2)
                .map(|ch| (to_f32(ch[0]) + to_f32(ch[1])) * 0.5),
        );
    } else {
        st.mono_buf.extend(
            samples
                .chunks(channels)
                .map(|ch| ch.iter().map(|x| to_f32(*x)).sum::<f32>() * (1.0 / channels as f32)),
        );
    }
    st.resampler.process(&st.mono_buf, ratio, &mut st.resampled);
    st.buffer.extend(st.resampled.iter().copied());
    while st.buffer.len() >= FRAME_SIZE {
        // 栈数组收帧：make_contiguous 后单次 memcpy（替代 drain 逐元素），免每帧堆分配。
        let mut frame = [0f32; FRAME_SIZE];
        let contig = st.buffer.make_contiguous();
        frame.copy_from_slice(&contig[..FRAME_SIZE]);
        st.buffer.drain(..FRAME_SIZE);
        push_frame(&mut st.queue, &mut st.dropped, frame, "输入");
    }
}

/// 输出回调：取帧 → 重采样 → 单声道入缓冲 → 按 channels 展开填充。
/// 缓冲仅存单声道（空间减半），展开时每样本转码一次即复制，避免重复转码。
/// 按需补充到目标深度、读游标 head 延迟压缩（稳态约 17 次回调才搬移一次）；
/// 欠载时自上次样本线性斜坡到 0，消除硬切静音的咔哒。
fn fill_output<T: FromF32Sample>(
    data: &mut [T],
    channels: usize,
    ratio: f64,
    state: &Arc<Mutex<OutputState>>,
) {
    let mut s = state.lock().unwrap();
    let st = &mut *s;
    if st.ramp_len == 0 {
        // 5ms 按输出采样率折算（16k 基准为 80 样本）；目标深度一并缓存（稳态恒定）。
        st.ramp_len = ((80.0 * ratio) as usize).max(1);
        st.target_samples = (FRAME_SIZE as f64 * ratio * TARGET_FRAMES as f64) as usize;
    }
    // 按需补充到目标缓冲深度（≈ TARGET_FRAMES 帧）：稳态每回调约补一帧，
    // 启动/突发期一次补足目标深度，抗到达抖动，避免首字欠载卡顿。
    while st.buffer.len() - st.head < st.target_samples {
        match st.queue.pop_front() {
            Some(frame) => {
                st.resampler.process(&frame, ratio, &mut st.resampled);
                st.buffer.extend(st.resampled.iter().copied());
            }
            None => break,
        }
    }
    // 读游标切片整块拷贝（免逐样本 pop_front）；head 后移，不立即搬移。
    let frames_needed = data.len() / channels;
    let avail = (st.buffer.len() - st.head).min(frames_needed);
    if avail > 0 {
        if channels == 1 {
            for (dst, &v) in data.iter_mut().zip(st.buffer[st.head..st.head + avail].iter()) {
                *dst = T::from_f32_sample(v);
                st.last_out = v;
            }
        } else if channels == 2 {
            // 立体声特化：chunks_exact_mut + 定长切片模式解构，免索引边界检查。
            for (pair, &v) in data.chunks_exact_mut(2).zip(st.buffer[st.head..st.head + avail].iter()) {
                let s = T::from_f32_sample(v);
                if let [l, r] = pair {
                    *l = s;
                    *r = s;
                }
                st.last_out = v;
            }
        } else {
            for (chunk, &v) in data.chunks_mut(channels).zip(st.buffer[st.head..st.head + avail].iter()) {
                let s = T::from_f32_sample(v);
                for dst in chunk {
                    *dst = s;
                }
                st.last_out = v;
            }
        }
        st.head += avail;
        st.ramp_pending = true;
        st.ramp_left = 0;
    }
    // 延迟压缩：head 达阈值才搬移一次（稳态 48k 下约每 17 回调一次，替代逐回调 make_contiguous）。
    if st.head >= COMPACT_THRESHOLD {
        st.buffer.drain(..st.head);
        st.head = 0;
    }
    // 欠载：斜坡段逐样本衰减，其余批量补 0（单次 fill 替代逐样本写，48k 立体声每回调最多省 960 次转码）。
    let tail = &mut data[avail * channels..];
    let mut written = 0usize;
    if !tail.is_empty() {
        if st.ramp_left > 0 {
            // 续上次未完成的斜坡。
            let take = st.ramp_left.min(tail.len());
            for dst in tail.iter_mut().take(take) {
                st.ramp_left -= 1;
                *dst = T::from_f32_sample(st.last_out * (st.ramp_left as f32 / st.ramp_len as f32));
            }
            written = take;
        } else if st.ramp_pending {
            st.ramp_pending = false;
            if st.last_out.abs() > 1e-4 {
                st.ramp_left = st.ramp_len - 1;
                let take = st.ramp_left.min(tail.len());
                for dst in tail.iter_mut().take(take) {
                    st.ramp_left -= 1;
                    *dst = T::from_f32_sample(st.last_out * (st.ramp_left as f32 / st.ramp_len as f32));
                }
                written = take;
            }
        }
        if written < tail.len() {
            let zero = T::from_f32_sample(0.0);
            tail[written..].fill(zero);
        }
    }
}
