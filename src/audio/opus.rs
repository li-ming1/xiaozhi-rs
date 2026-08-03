//! Opus 编解码：opusic-sys 内置绑定（bundled，构建期用 cmake 编译 libopus 静态链接）。
//!
//! 编码器固定 16kHz/单声道/60ms（官方上行标准）；解码器采样率可配置
//! （下行服从服务器 hello.audio_params，支持 16/24/48kHz）。
//! 不再动态加载外部 opus 库：libopus 由 opusic-sys 的 bundled 特性在构建期
//! 通过 cmake 源码编译并直接静态链接进可执行文件，运行时无需部署 opus dll。

use log::{debug, info, warn};
use opusic_sys::{
    opus_decode_float, opus_decoder_create, opus_decoder_destroy, opus_encode_float,
    opus_encoder_create, opus_encoder_destroy, opus_get_version_string, opus_strerror,
    OpusDecoder, OpusEncoder, OPUS_APPLICATION_VOIP, OPUS_OK, OPUS_SET_BITRATE_REQUEST,
    OPUS_SET_COMPLEXITY_REQUEST, OPUS_SET_DTX_REQUEST, OPUS_SET_INBAND_FEC_REQUEST,
    OPUS_SET_VBR_CONSTRAINT_REQUEST, OPUS_SET_VBR_REQUEST,
};
use std::ffi::{c_int, CStr};
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Once;

/// 崩溃诊断：最近一次执行的 opus 操作（供未处理异常过滤器读取）。
pub(crate) static LAST_OPUS_CALL: AtomicU8 = AtomicU8::new(0);
pub(crate) const OPUS_CALL_NONE: u8 = 0;
pub(crate) const OPUS_CALL_ENCODE: u8 = 1;
pub(crate) const OPUS_CALL_DECODE: u8 = 2;
pub(crate) const OPUS_CALL_CTL: u8 = 3;
pub(crate) const OPUS_CALL_DESTROY: u8 = 4;
/// 崩溃诊断：最近一次 opus_encoder_ctl 的请求号。
pub(crate) static LAST_OPUS_CTL_REQUEST: AtomicU32 = AtomicU32::new(0);
/// 进程级：内置库版本信息只打印一次（编码器/解码器各实例化一次，避免刷屏）。
static OPUS_LIB_INFO_ONCE: Once = Once::new();

use crate::error::{Result, VoiceError};

/// 上行采样率（官方固定）。
pub const ENCODER_RATE: u32 = 16000;
/// 上行帧长 60ms（官方标准）。
pub const FRAME_MS: u32 = 60;
/// 编码帧样本数（60ms @ 16k）。
pub const ENCODER_FRAME_SIZE: usize = (ENCODER_RATE as usize * FRAME_MS as usize) / 1000;
const MAX_PACKET_SIZE: usize = 1500;
/// 解码最大输出（60ms @ 48k 单声道）。
const DECODE_MAX_SAMPLES: usize = 48_000 / 1000 * 60;

// opus_encoder_ctl 在 opusic-sys 中是 C 变参函数（`...`），stable Rust 无法直接调用；
// 这里按固定 3 参数重新声明同一符号（x86-64/ARM64 调用约定下与变参调用 ABI 兼容，
// 是本项目仅使用的一类请求：3 参数 ctl）。
unsafe extern "C" {
    #[link_name = "opus_encoder_ctl"]
    fn opus_encoder_ctl_fixed(st: *mut OpusEncoder, request: c_int, arg: c_int) -> c_int;
}

/// 网络质量分级，驱动编码策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkGrade {
    /// 良好：32kbps VBR / FEC off / DTX off。
    Good,
    /// 中等弱网：28kbps + FEC。
    Fair,
    /// 严重弱网：20kbps constrained-VBR / FEC / DTX。
    Poor,
}

pub struct OpusCodec {
    encoder: *mut OpusEncoder,
    decoder: *mut OpusDecoder,
    encode_buf: Vec<u8>,
    decode_buf: Vec<f32>,
    /// 下行采样率（解码器按此创建）。
    decode_rate: i32,
    /// 当前网络分级。
    grade: NetworkGrade,
}

unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

/// 打印内置 libopus 版本（进程级一次）。
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

        let mut codec = Self {
            encoder,
            decoder,
            encode_buf: vec![0u8; MAX_PACKET_SIZE],
            decode_buf: vec![0f32; DECODE_MAX_SAMPLES],
            decode_rate: decode_rate as i32,
            grade: NetworkGrade::Good,
        };
        codec.apply_encoder_config(NetworkGrade::Good);
        debug!("Opus 编解码器初始化成功（解码 {}Hz）", decode_rate);
        Ok(codec)
    }

    /// 按网络分级调整编码参数。
    ///
    /// 注意：当前禁用运行时调整 —— 旧捆绑/系统 opus.dll 在 `opus_encoder_ctl`
    /// 的 FEC/DTX/VBR 约束请求上确定性崩溃（offset=0x8ce6）。现切换为 opusic-sys
    /// 内置编译，ctl 请求号采用 libopus 官方定义，可择机恢复运行时调整；
    /// 上行暂时保持 Good（32kbps VBR，FEC/DTX 关闭）。
    pub fn set_network_grade(&mut self, grade: NetworkGrade) {
        if self.grade == grade {
            return;
        }
        warn!(
            "网络分级变化 {:?} -> {:?}（当前禁用运行时调整，避免 opus_ctl 崩溃）",
            self.grade, grade
        );
        self.grade = grade;
    }

    fn apply_encoder_config(&mut self, grade: NetworkGrade) {
        let (bitrate, vbr, constrained, fec, dtx) = match grade {
            NetworkGrade::Good => (32_000, 1, 0, 0, 0),
            NetworkGrade::Fair => (28_000, 1, 0, 1, 0),
            NetworkGrade::Poor => (20_000, 0, 1, 1, 1),
        };
        LAST_OPUS_CALL.store(OPUS_CALL_CTL, Ordering::Relaxed);
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_BITRATE_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { opus_encoder_ctl_fixed(self.encoder, OPUS_SET_BITRATE_REQUEST, bitrate) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_VBR_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { opus_encoder_ctl_fixed(self.encoder, OPUS_SET_VBR_REQUEST, vbr) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_VBR_CONSTRAINT_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe {
            opus_encoder_ctl_fixed(self.encoder, OPUS_SET_VBR_CONSTRAINT_REQUEST, constrained)
        };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_INBAND_FEC_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { opus_encoder_ctl_fixed(self.encoder, OPUS_SET_INBAND_FEC_REQUEST, fec) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_DTX_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { opus_encoder_ctl_fixed(self.encoder, OPUS_SET_DTX_REQUEST, dtx) };
        LAST_OPUS_CTL_REQUEST.store(0, Ordering::Relaxed);
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
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
        LAST_OPUS_CALL.store(OPUS_CALL_ENCODE, Ordering::Relaxed);
        let len = unsafe {
            opus_encode_float(
                self.encoder,
                input.as_ptr(),
                ENCODER_FRAME_SIZE as c_int,
                self.encode_buf.as_mut_ptr(),
                MAX_PACKET_SIZE as c_int,
            )
        };
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
        if len < 0 {
            return Err(VoiceError::Opus(format!(
                "Opus 编码失败: {}",
                opus_error_desc(len)
            )));
        }
        Ok(self.encode_buf[..len as usize].to_vec())
    }

    /// 解码 Opus 包为 f32 PCM（长度随帧时长与采样率变化）。
    /// `input` 为空时执行 PLC；`fec` 为 true 时尝试带内 FEC 前向错误恢复。
    pub fn decode(&mut self, input: &[u8], fec: bool) -> Result<Vec<f32>> {
        LAST_OPUS_CALL.store(OPUS_CALL_DECODE, Ordering::Relaxed);
        let samples = if input.is_empty() {
            unsafe {
                opus_decode_float(
                    self.decoder,
                    std::ptr::null(),
                    0,
                    self.decode_buf.as_mut_ptr(),
                    DECODE_MAX_SAMPLES as c_int,
                    0,
                )
            }
        } else {
            unsafe {
                opus_decode_float(
                    self.decoder,
                    input.as_ptr(),
                    input.len() as c_int,
                    self.decode_buf.as_mut_ptr(),
                    DECODE_MAX_SAMPLES as c_int,
                    fec as c_int,
                )
            }
        };
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
        if samples < 0 {
            return Err(VoiceError::Opus(format!(
                "Opus 解码失败: {}",
                opus_error_desc(samples)
            )));
        }
        Ok(self.decode_buf[..samples as usize].to_vec())
    }

    pub fn decode_rate(&self) -> u32 {
        self.decode_rate as u32
    }
}

impl Drop for OpusCodec {
    fn drop(&mut self) {
        LAST_OPUS_CALL.store(OPUS_CALL_DESTROY, Ordering::Relaxed);
        if !self.encoder.is_null() {
            unsafe { opus_encoder_destroy(self.encoder) };
        }
        if !self.decoder.is_null() {
            unsafe { opus_decoder_destroy(self.decoder) };
        }
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
    }
}

/// 将 opus 返回码转换为可读错误描述。
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
}
