//! Opus 编解码：opusic-sys 内置绑定（bundled，构建期 cmake 静态链接）。
//! 编码固定 16kHz/单声道/60ms；解码采样率服从服务器 hello.audio_params（16/24/48kHz）。

use log::{debug, info, warn};
use opusic_sys::{
    opus_decode_float, opus_decoder_create, opus_decoder_destroy, opus_encode_float,
    opus_encoder_create, opus_encoder_destroy, opus_get_version_string, opus_strerror,
    OpusDecoder, OpusEncoder, OPUS_APPLICATION_VOIP, OPUS_OK, OPUS_SET_BITRATE_REQUEST,
    OPUS_SET_COMPLEXITY_REQUEST, OPUS_SET_DTX_REQUEST, OPUS_SET_INBAND_FEC_REQUEST,
    OPUS_SET_PACKET_LOSS_PERC_REQUEST, OPUS_SET_VBR_CONSTRAINT_REQUEST, OPUS_SET_VBR_REQUEST,
};
use std::ffi::{c_int, CStr};
use std::sync::Once;

/// 进程级：内置库版本信息只打印一次。
static OPUS_LIB_INFO_ONCE: Once = Once::new();

use crate::error::{Result, VoiceError};

pub const ENCODER_RATE: u32 = 16000;
pub const FRAME_MS: u32 = 60;
/// 编码帧样本数（60ms @ 16k）。
pub const ENCODER_FRAME_SIZE: usize = (ENCODER_RATE as usize * FRAME_MS as usize) / 1000;
const MAX_PACKET_SIZE: usize = 1500;
// 解码帧长按解码器采样率计算（16k→960 / 24k→1440 / 48k→2880）。
// 注意：opus_decode_float 的 frame_size 参数在 PLC（空包）时决定输出样本数，
// 若按 48k 上限 2880 传，16k 解码器的 PLC 帧会变成 3 倍长，导致播放时序崩坏。

// opus_encoder_ctl 是 C 变参函数（`...`），stable Rust 无法直接调用；
// 这里按固定 3 参数重新声明同一符号（本项目仅使用 3 参数 ctl 请求）。
unsafe extern "C" {
    #[link_name = "opus_encoder_ctl"]
    fn opus_encoder_ctl_fixed(st: *mut OpusEncoder, request: c_int, arg: c_int) -> c_int;
}

/// 网络质量分级，驱动编码策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkGrade {
    /// 32kbps VBR / FEC off / DTX off。
    Good,
    /// 28kbps + FEC。
    Fair,
    /// 20kbps constrained-VBR / FEC / DTX。
    Poor,
}

pub struct OpusCodec {
    encoder: *mut OpusEncoder,
    decoder: *mut OpusDecoder,
    /// 解码帧长（decode_rate 的 60ms 样本数）：正常帧与 PLC 帧的统一输出长度。
    decode_frame_size: c_int,
    encode_buf: Vec<u8>,
    decode_buf: Vec<f32>,
    grade: NetworkGrade,
}

// Opus 句柄非线程安全，仅允许在 worker 间移动（Send）；所有方法均 &mut self 独占，不共享引用。
unsafe impl Send for OpusCodec {}

fn log_opus_version() {
    OPUS_LIB_INFO_ONCE.call_once(|| {
        let v = unsafe { opus_get_version_string() };
        if !v.is_null() {
            let s = unsafe { CStr::from_ptr(v) };
            info!(
                "已加载内置 Opus 库（opusic-sys bundled 静态链接）: {}",
                s.to_string_lossy()
            );
        } else {
            info!("已加载内置 Opus 库（opusic-sys bundled 静态链接）");
        }
    });
}

impl OpusCodec {
    /// 创建编解码器：编码 16k/1ch，解码按 `decode_rate`。
    pub fn new(decode_rate: u32) -> Result<Self> {
        log_opus_version();

        let mut error: c_int = 0;
        let encoder = unsafe {
            opus_encoder_create(
                ENCODER_RATE as c_int,
                1,
                OPUS_APPLICATION_VOIP,
                &mut error,
            )
        };
        if error != OPUS_OK || encoder.is_null() {
            return Err(VoiceError::Opus(format!(
                "创建 Opus 编码器失败: {}",
                opus_error_desc(error)
            )));
        }

        let mut error: c_int = 0;
        let decoder = unsafe { opus_decoder_create(decode_rate as c_int, 1, &mut error) };
        if error != OPUS_OK || decoder.is_null() {
            return Err(VoiceError::Opus(format!(
                "创建 Opus 解码器失败: {}",
                opus_error_desc(error)
            )));
        }

        let decode_frame_size = (decode_rate as usize * FRAME_MS as usize) / 1000;
        let mut codec = Self {
            encoder,
            decoder,
            decode_frame_size: decode_frame_size as c_int,
            encode_buf: vec![0u8; MAX_PACKET_SIZE],
            decode_buf: vec![0f32; decode_frame_size],
            grade: NetworkGrade::Good,
        };
        codec.apply_encoder_config(NetworkGrade::Good);
        debug!("Opus 编解码器初始化成功（解码 {}Hz）", decode_rate);
        Ok(codec)
    }

    /// 按网络分级调整编码参数（32/28/20kbps + FEC/DTX）。
    pub fn set_network_grade(&mut self, grade: NetworkGrade) {
        if self.grade == grade {
            return;
        }
        info!(
            "网络分级变化 {:?} -> {:?}，调整编码策略",
            self.grade, grade
        );
        self.grade = grade;
        self.apply_encoder_config(grade);
    }

    fn apply_encoder_config(&mut self, grade: NetworkGrade) {
        // loss_perc（预期丢包率）是带内 FEC 的前提：不设置时编码器不会添加冗余，FEC 形同虚设。
        let (bitrate, vbr, constrained, fec, dtx, loss_perc) = match grade {
            NetworkGrade::Good => (32_000, 1, 0, 0, 0, 0),
            NetworkGrade::Fair => (28_000, 1, 0, 1, 0, 10),
            NetworkGrade::Poor => (20_000, 0, 1, 1, 1, 15),
        };
        let set = |request: c_int, value: c_int| {
            let ret = unsafe { opus_encoder_ctl_fixed(self.encoder, request, value) };
            if ret != OPUS_OK {
                warn!("opus_encoder_ctl({}) 失败: {}", request, opus_error_desc(ret));
            }
        };
        set(OPUS_SET_BITRATE_REQUEST, bitrate);
        set(OPUS_SET_VBR_REQUEST, vbr);
        set(OPUS_SET_VBR_CONSTRAINT_REQUEST, constrained);
        set(OPUS_SET_INBAND_FEC_REQUEST, fec);
        set(OPUS_SET_PACKET_LOSS_PERC_REQUEST, loss_perc);
        set(OPUS_SET_DTX_REQUEST, dtx);
    }

    /// 设置编码复杂度（0-10），默认 10。
    pub fn set_complexity(&mut self, complexity: c_int) {
        let ret = unsafe { opus_encoder_ctl_fixed(self.encoder, OPUS_SET_COMPLEXITY_REQUEST, complexity) };
        if ret != OPUS_OK {
            warn!("设置 Opus 复杂度失败: {}（使用默认值）", opus_error_desc(ret));
        }
    }

    /// 编码单帧 f32 PCM（16k 单声道，960 样本 = 60ms）为 Opus 包。
    pub fn encode(&mut self, input: &[f32]) -> Result<Vec<u8>> {
        if input.len() != ENCODER_FRAME_SIZE {
            return Err(VoiceError::Opus(format!(
                "编码帧大小不正确: {} (期望 {})",
                input.len(),
                ENCODER_FRAME_SIZE
            )));
        }
        let len = unsafe {
            opus_encode_float(
                self.encoder,
                input.as_ptr(),
                ENCODER_FRAME_SIZE as c_int,
                self.encode_buf.as_mut_ptr(),
                MAX_PACKET_SIZE as c_int,
            )
        };
        if len < 0 {
            return Err(VoiceError::Opus(format!(
                "Opus 编码失败: {}",
                opus_error_desc(len)
            )));
        }
        Ok(self.encode_buf[..len as usize].to_vec())
    }

    /// 解码 Opus 包为 f32 PCM。`input` 为空时执行 PLC；`fec` 为 true 时尝试带内 FEC 恢复。
    pub fn decode(&mut self, input: &[u8], fec: bool) -> Result<Vec<f32>> {
        // 空包走 PLC（调用方恒传 fec=false，此处分支显式置 0 更稳健）。
        let (ptr, fec) = if input.is_empty() {
            (std::ptr::null(), 0)
        } else {
            (input.as_ptr(), fec as c_int)
        };
        let samples = unsafe {
            opus_decode_float(
                self.decoder,
                ptr,
                input.len() as c_int,
                self.decode_buf.as_mut_ptr(),
                self.decode_frame_size,
                fec,
            )
        };
        if samples < 0 {
            return Err(VoiceError::Opus(format!(
                "Opus 解码失败: {}",
                opus_error_desc(samples)
            )));
        }
        Ok(self.decode_buf[..samples as usize].to_vec())
    }
}

impl Drop for OpusCodec {
    fn drop(&mut self) {
        if !self.encoder.is_null() {
            unsafe { opus_encoder_destroy(self.encoder) };
        }
        if !self.decoder.is_null() {
            unsafe { opus_decoder_destroy(self.decoder) };
        }
    }
}

fn opus_error_desc(code: c_int) -> String {
    let s = unsafe { opus_strerror(code) };
    if s.is_null() {
        format!("opus 错误码 {}", code)
    } else {
        let desc = unsafe { CStr::from_ptr(s) }.to_string_lossy();
        format!("{} ({})", desc, code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_frame_size_is_960() {
        assert_eq!(ENCODER_FRAME_SIZE, 960);
        assert_eq!(ENCODER_RATE * FRAME_MS / 1000, 960);
    }

    /// 丢包恢复路径：空包 + fec=true 应产出完整一帧（60ms @16k = 960），而非空/报错。
    #[test]
    fn decode_empty_requests_plc_frame() {
        let mut codec = OpusCodec::new(16_000).unwrap();
        let plc = codec.decode(&[], true).unwrap();
        assert_eq!(plc.len(), 960, "PLC 输出帧长异常: {}", plc.len());
    }

    /// 正常包解码（fec=false）应还原信号能量，验证 FEC 参数不再破坏当前帧。
    #[test]
    fn encode_decode_roundtrip_preserves_energy() {
        let mut codec = OpusCodec::new(16_000).unwrap();
        // 400Hz 正弦波，幅度 0.5。
        let frame: Vec<f32> = (0..ENCODER_FRAME_SIZE)
            .map(|i| 0.5 * (2.0 * std::f32::consts::PI * 400.0 * i as f32 / 16_000.0).sin())
            .collect();
        let pkt = codec.encode(&frame).unwrap();
        assert!(!pkt.is_empty());
        let out = codec.decode(&pkt, false).unwrap();
        assert_eq!(out.len(), 960, "解码帧长异常: {}", out.len());
        let rms = (out.iter().map(|s| s * s).sum::<f32>() / out.len() as f32).sqrt();
        assert!(rms > 0.1, "解码后能量过低: RMS={:.4}", rms);
    }
}
