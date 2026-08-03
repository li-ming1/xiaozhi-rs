//! 高质量重采样：rubato 4.0 `Async`（sinc，256-tap BlackmanHarris2）。
//!
//! 采用异步（Async）重采样器以便实时调整比率补偿时钟漂移（±1000ppm）。
//! 输入侧固定（`FixedAsync::Input`）：每次调用消费 `input_frames_next()` 输入帧。
//! 内部用 `VecDeque` 累积输入，凑足一帧即处理。

use std::collections::VecDeque;

use rubato::audioadapter_buffers::direct::InterleavedSlice;
use rubato::{
    Adjustable, Async, FixedAsync, Resampler, SincInterpolationParameters, WindowFunction,
};

use crate::error::{Result, VoiceError};

/// 相对比率最大偏差（1.01 → ±1%）。
const MAX_RELATIVE_RATIO: f64 = 1.01;
/// sinc 滤波器长度（256-tap，自动截止频率）。
const SINC_LEN: usize = 256;

/// 异步重采样器（单声道）。
pub struct AsyncResampler {
    resampler: Async<f32>,
    in_queue: VecDeque<f32>,
    in_scratch: Vec<f32>,
    out_scratch: Vec<f32>,
    ratio: f64,
}

impl AsyncResampler {
    /// `nominal_ratio` = 输出采样率 / 输入采样率；`chunk_size` = 输入侧固定块帧数。
    pub fn new(nominal_ratio: f64, chunk_size: usize) -> Result<Self> {
        let params = SincInterpolationParameters::new(SINC_LEN, WindowFunction::BlackmanHarris2);
        let resampler = Async::<f32>::new_sinc(
            nominal_ratio,
            MAX_RELATIVE_RATIO,
            &params,
            chunk_size,
            1,
            FixedAsync::Input,
        )
        .map_err(|e| VoiceError::InvalidConfig(format!("重采样器创建失败: {}", e)))?;
        Ok(Self {
            resampler,
            in_queue: VecDeque::with_capacity(chunk_size * 2),
            in_scratch: Vec::with_capacity(chunk_size + 8),
            out_scratch: Vec::with_capacity(chunk_size * 2 + 16),
            ratio: nominal_ratio,
        })
    }

    /// 喂入输入样本并尝试产出，结果追加到 `out`。
    /// `ratio` 为当前目标比率（输出/输入），随漂移控制调整。
    pub fn process(&mut self, input: &[f32], ratio: f64, out: &mut Vec<f32>) {
        self.in_queue.extend(input.iter().copied());
        if (ratio - self.ratio).abs() > 1e-9 {
            let _ = self.resampler.set_resample_ratio(ratio, true);
            self.ratio = ratio;
        }
        loop {
            let needed = self.resampler.input_frames_next();
            if self.in_queue.len() < needed {
                break;
            }
            self.in_scratch.clear();
            self.in_scratch.extend(self.in_queue.drain(..needed));
            let Ok(input_adapter) = InterleavedSlice::new(&self.in_scratch, 1, needed) else {
                break;
            };
            let out_frames = self.resampler.output_frames_max();
            self.out_scratch.clear();
            self.out_scratch.resize(out_frames, 0.0);
            let Ok(mut output_adapter) =
                InterleavedSlice::new_mut(&mut self.out_scratch, 1, out_frames)
            else {
                break;
            };
            let Ok((_, written)) = self
                .resampler
                .process_into_buffer(&input_adapter, &mut output_adapter, None)
            else {
                break;
            };
            out.extend_from_slice(&self.out_scratch[..written]);
        }
    }

    /// 当前需要一个块的输入帧数。
    pub fn input_frames_next(&self) -> usize {
        self.resampler.input_frames_next()
    }

    /// 当前比例。
    pub fn ratio(&self) -> f64 {
        self.ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resample_48k_to_16k_produces_expected_ratio() {
        // 48k → 16k，输入 480 帧（10ms），应产出约 160 帧。
        let mut r = AsyncResampler::new(16_000.0 / 48_000.0, 480).unwrap();
        let input = vec![0.5f32; 480];
        let mut out = Vec::new();
        r.process(&input, 16_000.0 / 48_000.0, &mut out);
        assert_eq!(r.input_frames_next(), 480);
        assert!(
            out.len() >= 150 && out.len() <= 170,
            "out.len={}",
            out.len()
        );
        // 滤波器瞬态在前 ~1.5 个滤波器长度（sinc 256-tap 对应 ~50 输出样本），
        // 后一半应为稳态直流 0.5。
        let steady = &out[out.len() / 2..];
        assert!(
            steady.iter().all(|s| (*s - 0.5).abs() < 0.01),
            "稳态偏差超限: first={:?}",
            steady
        );
    }

    #[test]
    fn ratio_change_within_bounds() {
        let mut r = AsyncResampler::new(2.0, 960).unwrap();
        let input = vec![0.1f32; 960];
        let mut out = Vec::new();
        // 千分之一漂移内调整不应失败。
        r.process(&input, 2.0 * 1.001, &mut out);
        assert!(!out.is_empty());
    }
}
