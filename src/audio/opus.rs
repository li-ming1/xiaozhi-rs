//! Opus 编解码：libloading 动态链接（opus.dll / libopus.dylib / libopus.so）。
//!
//! 编码器固定 16kHz/单声道/60ms（官方上行标准）；解码器采样率可配置
//! （下行服从服务器 hello.audio_params，支持 16/24/48kHz）。
//! 保留动态加载以支持跨平台 6 份预编译库。

use libloading::{Library, Symbol};
use log::{info, warn};
use std::ffi::c_int;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;

/// 崩溃诊断：最近一次执行的 opus 操作（供未处理异常过滤器读取）。
pub(crate) static LAST_OPUS_CALL: AtomicU8 = AtomicU8::new(0);
pub(crate) const OPUS_CALL_NONE: u8 = 0;
pub(crate) const OPUS_CALL_ENCODE: u8 = 1;
pub(crate) const OPUS_CALL_DECODE: u8 = 2;
pub(crate) const OPUS_CALL_CTL: u8 = 3;
pub(crate) const OPUS_CALL_DESTROY: u8 = 4;
/// 崩溃诊断：最近一次 opus_encoder_ctl 的请求号。
pub(crate) static LAST_OPUS_CTL_REQUEST: AtomicU32 = AtomicU32::new(0);

use crate::error::{Result, VoiceError};

const OPUS_APPLICATION_VOIP: c_int = 2048;
const OPUS_SET_COMPLEXITY_REQUEST: c_int = 4010;
const OPUS_SET_BITRATE_REQUEST: c_int = 4002;
const OPUS_SET_VBR_REQUEST: c_int = 4006;
const OPUS_SET_VBR_CONSTRAINT_REQUEST: c_int = 4005;
const OPUS_SET_DTX_REQUEST: c_int = 4003;
const OPUS_SET_INBAND_FEC_REQUEST: c_int = 4012;
const OPUS_OK: c_int = 0;

/// 上行采样率（官方固定）。
pub const ENCODER_RATE: u32 = 16000;
/// 上行帧长 60ms（官方标准）。
pub const FRAME_MS: u32 = 60;
/// 编码帧样本数（60ms @ 16k）。
pub const ENCODER_FRAME_SIZE: usize = (ENCODER_RATE as usize * FRAME_MS as usize) / 1000;
const MAX_PACKET_SIZE: usize = 1500;
/// 解码最大输出（60ms @ 48k 单声道）。
const DECODE_MAX_SAMPLES: usize = 48_000 / 1000 * 60;

type OpusEncoderCreate = extern "C" fn(i32, i32, c_int, *mut c_int) -> *mut std::ffi::c_void;
type OpusEncoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusEncodeFloat = extern "C" fn(*mut std::ffi::c_void, *const f32, c_int, *mut u8, c_int) -> c_int;
type OpusDecoderCreate = extern "C" fn(i32, i32, *mut c_int) -> *mut std::ffi::c_void;
type OpusDecoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusDecodeFloat =
    extern "C" fn(*mut std::ffi::c_void, *const u8, c_int, *mut f32, c_int, c_int) -> c_int;
type OpusEncoderCtl = unsafe extern "C" fn(*mut std::ffi::c_void, c_int, ...) -> c_int;

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
    _library: Arc<Library>,
    encoder: *mut std::ffi::c_void,
    decoder: *mut std::ffi::c_void,
    encode_float: Symbol<'static, OpusEncodeFloat>,
    decode_float: Symbol<'static, OpusDecodeFloat>,
    encoder_destroy: Symbol<'static, OpusEncoderDestroy>,
    decoder_destroy: Symbol<'static, OpusDecoderDestroy>,
    encoder_ctl: Option<Symbol<'static, OpusEncoderCtl>>,
    encode_buf: Vec<u8>,
    decode_buf: Vec<f32>,
    /// 下行采样率（解码器按此创建）。
    decode_rate: i32,
    /// 当前网络分级。
    grade: NetworkGrade,
}

unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

impl OpusCodec {
    /// 创建编解码器：编码 16k/1ch，解码按 `decode_rate`。
    pub fn new(decode_rate: u32) -> Result<Self> {
        let library = Arc::new(load_opus_library()?);

        let encoder_create: Symbol<OpusEncoderCreate> =
            unsafe { load_symbol(&library, b"opus_encoder_create\0")? };
        let decoder_create: Symbol<OpusDecoderCreate> =
            unsafe { load_symbol(&library, b"opus_decoder_create\0")? };
        let encode_float: Symbol<OpusEncodeFloat> =
            unsafe { load_symbol(&library, b"opus_encode_float\0")? };
        let decode_float: Symbol<OpusDecodeFloat> =
            unsafe { load_symbol(&library, b"opus_decode_float\0")? };
        let encoder_destroy: Symbol<OpusEncoderDestroy> =
            unsafe { load_symbol(&library, b"opus_encoder_destroy\0")? };
        let decoder_destroy: Symbol<OpusDecoderDestroy> =
            unsafe { load_symbol(&library, b"opus_decoder_destroy\0")? };
        let encoder_ctl: Option<Symbol<'static, OpusEncoderCtl>> =
            unsafe { load_symbol(&library, b"opus_encoder_ctl\0").ok() };

        let mut error: c_int = 0;
        let encoder = encoder_create(ENCODER_RATE as i32, 1, OPUS_APPLICATION_VOIP, &mut error);
        if error != OPUS_OK || encoder.is_null() {
            return Err(VoiceError::Opus(format!("创建 Opus 编码器失败: {}", error)));
        }

        let mut error: c_int = 0;
        let decoder = decoder_create(decode_rate as i32, 1, &mut error);
        if error != OPUS_OK || decoder.is_null() {
            return Err(VoiceError::Opus(format!("创建 Opus 解码器失败: {}", error)));
        }

        // 版本校验：打印实际加载的 libopus 版本，便于诊断崩溃来源。
        if let Ok(version_fn) =
            unsafe { load_symbol::<extern "C" fn() -> *const std::ffi::c_char>(&library, b"opus_get_version_string\0") }
        {
            let v = version_fn();
            if !v.is_null() {
                let cstr = unsafe { std::ffi::CStr::from_ptr(v) };
                info!("Opus 库版本: {}", cstr.to_string_lossy());
            }
        }

        let mut codec = Self {
            _library: library,
            encoder,
            decoder,
            encode_float,
            decode_float,
            encoder_destroy,
            decoder_destroy,
            encoder_ctl,
            encode_buf: vec![0u8; MAX_PACKET_SIZE],
            decode_buf: vec![0f32; DECODE_MAX_SAMPLES],
            decode_rate: decode_rate as i32,
            grade: NetworkGrade::Good,
        };
        codec.apply_encoder_config(NetworkGrade::Good);
        info!("Opus 编解码器初始化成功（解码 {}Hz）", decode_rate);
        Ok(codec)
    }

    /// 按网络分级调整编码参数。
    ///
    /// 注意：当前禁用运行时调整 —— 捆绑/系统 opus.dll 在 `opus_encoder_ctl`
    /// 的 FEC/DTX/VBR 约束请求上确定性崩溃（offset=0x8ce6）。待确认具体请求
    /// 后再针对性恢复。上行保持 Good（32kbps VBR，FEC/DTX 关闭）。
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
        let Some(ctl) = &self.encoder_ctl else { return };
        let (bitrate, vbr, constrained, fec, dtx) = match grade {
            NetworkGrade::Good => (32_000, 1, 0, 0, 0),
            NetworkGrade::Fair => (28_000, 1, 0, 1, 0),
            NetworkGrade::Poor => (20_000, 0, 1, 1, 1),
        };
        LAST_OPUS_CALL.store(OPUS_CALL_CTL, Ordering::Relaxed);
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_BITRATE_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { ctl(self.encoder, OPUS_SET_BITRATE_REQUEST, bitrate) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_VBR_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { ctl(self.encoder, OPUS_SET_VBR_REQUEST, vbr) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_VBR_CONSTRAINT_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { ctl(self.encoder, OPUS_SET_VBR_CONSTRAINT_REQUEST, constrained) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_INBAND_FEC_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { ctl(self.encoder, OPUS_SET_INBAND_FEC_REQUEST, fec) };
        LAST_OPUS_CTL_REQUEST.store(OPUS_SET_DTX_REQUEST as u32, Ordering::Relaxed);
        let _ = unsafe { ctl(self.encoder, OPUS_SET_DTX_REQUEST, dtx) };
        LAST_OPUS_CTL_REQUEST.store(0, Ordering::Relaxed);
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
    }

    /// 设置编码复杂度（0-10），默认 10。
    pub fn set_complexity(&mut self, complexity: c_int) {
        if let Some(ctl) = &self.encoder_ctl {
            let ret = unsafe { ctl(self.encoder, OPUS_SET_COMPLEXITY_REQUEST, complexity) };
            if ret != OPUS_OK {
                warn!("设置 Opus 复杂度失败: {}（使用默认值）", ret);
            }
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
        let len = (self.encode_float)(
            self.encoder,
            input.as_ptr(),
            ENCODER_FRAME_SIZE as c_int,
            self.encode_buf.as_mut_ptr(),
            MAX_PACKET_SIZE as c_int,
        );
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
        if len < 0 {
            return Err(VoiceError::Opus(format!("Opus 编码失败: {}", len)));
        }
        Ok(self.encode_buf[..len as usize].to_vec())
    }

    /// 解码 Opus 包为 f32 PCM（长度随帧时长与采样率变化）。
    /// `input` 为空时执行 PLC；`fec` 为 true 时尝试带内 FEC 前向错误恢复。
    pub fn decode(&mut self, input: &[u8], fec: bool) -> Result<Vec<f32>> {
        LAST_OPUS_CALL.store(OPUS_CALL_DECODE, Ordering::Relaxed);
        let samples = if input.is_empty() {
            (self.decode_float)(
                self.decoder,
                std::ptr::null(),
                0,
                self.decode_buf.as_mut_ptr(),
                DECODE_MAX_SAMPLES as c_int,
                0,
            )
        } else {
            (self.decode_float)(
                self.decoder,
                input.as_ptr(),
                input.len() as c_int,
                self.decode_buf.as_mut_ptr(),
                DECODE_MAX_SAMPLES as c_int,
                fec as c_int,
            )
        };
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
        if samples < 0 {
            return Err(VoiceError::Opus(format!("Opus 解码失败: {}", samples)));
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
            (self.encoder_destroy)(self.encoder);
        }
        if !self.decoder.is_null() {
            (self.decoder_destroy)(self.decoder);
        }
        LAST_OPUS_CALL.store(OPUS_CALL_NONE, Ordering::Relaxed);
    }
}

#[allow(clippy::missing_transmute_annotations)]
unsafe fn load_symbol<T>(library: &Library, name: &'static [u8]) -> Result<Symbol<'static, T>> {
    let sym: Symbol<T> = unsafe { library.get(name) }
        .map_err(|e| VoiceError::Opus(format!("无法加载 {}: {}", String::from_utf8_lossy(name), e)))?;
    Ok(unsafe { std::mem::transmute(sym) })
}

fn load_opus_library() -> Result<Library> {
    let exe_dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|p| p.to_path_buf()));

    #[cfg(all(windows, target_arch = "x86_64"))]
    const BUNDLED: &str = "libs/libopus/win/x64/opus.dll";
    #[cfg(all(windows, target_arch = "aarch64"))]
    const BUNDLED: &str = "libs/libopus/win/arm64/opus.dll";
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    const BUNDLED: &str = "libs/libopus/mac/x64/libopus.dylib";
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    const BUNDLED: &str = "libs/libopus/mac/arm64/libopus.dylib";
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    const BUNDLED: &str = "libs/libopus/linux/x64/libopus.so";
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    const BUNDLED: &str = "libs/libopus/linux/arm64/libopus.so";

    #[cfg(windows)]
    const SYSTEM_NAMES: &[&str] = &["opus.dll"];
    #[cfg(target_os = "macos")]
    const SYSTEM_NAMES: &[&str] = &["libopus.dylib"];
    #[cfg(target_os = "linux")]
    const SYSTEM_NAMES: &[&str] = &["libopus.so.0", "libopus.so"];

    #[cfg(target_os = "macos")]
    const EXTRA_PATHS: &[&str] = &["/opt/homebrew/lib", "/usr/local/lib"];
    #[cfg(not(target_os = "macos"))]
    const EXTRA_PATHS: &[&str] = &[];

    // 候选路径优先级：
    // 1) exe 同目录的捆绑库（部署形态：库与 exe 平级）；
    // 2) 从 exe 目录向上逐级找仓库根下的 libs/libopus/...（开发形态）；
    // 3) 当前目录；
    // 4) 系统库。
    let mut candidates: Vec<std::path::PathBuf> = Vec::new();
    if let Some(dir) = &exe_dir {
        candidates.push(dir.join(BUNDLED));
        candidates.push(dir.join("opus.dll"));
        // 从 exe 目录向上找仓库根。
        let mut cur = dir.as_path().parent();
        while let Some(d) = cur {
            candidates.push(d.join(BUNDLED));
            cur = d.parent();
        }
    }
    candidates.push(std::path::PathBuf::from(BUNDLED));
    candidates.push(std::path::PathBuf::from("opus.dll"));
    for extra in EXTRA_PATHS {
        candidates.push(std::path::PathBuf::from(extra).join(BUNDLED));
    }

    for path in &candidates {
        if path.exists()
            && let Ok(lib) = unsafe { Library::new(path) }
        {
            info!("已加载 Opus 库: {}", path.display());
            return Ok(lib);
        }
    }
    for name in SYSTEM_NAMES {
        if let Ok(lib) = unsafe { Library::new(*name) } {
            warn!("未找到捆绑库，已加载系统 Opus 库: {}（建议部署捆绑库）", name);
            return Ok(lib);
        }
    }
    Err(VoiceError::Opus(format!(
        "未找到 opus 库，请确保 {} 存在或安装系统 opus 库",
        BUNDLED
    )))
}

// 保留 anyhow 以兼容旧错误路径。
#[allow(unused_imports)]
use anyhow as _anyhow;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_frame_size_is_960() {
        assert_eq!(ENCODER_FRAME_SIZE, 960);
        assert_eq!(ENCODER_RATE * FRAME_MS / 1000, 960);
    }
}
