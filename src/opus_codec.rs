//! Opus 编解码
//!
//! 使用 libloading 动态加载 opus 库（跨平台：opus.dll / libopus.dylib / libopus.so）

use anyhow::{anyhow, Result};
use libloading::{Library, Symbol};
use log::info;
use std::ffi::c_int;
use std::sync::Arc;

/// Opus 常量
const OPUS_APPLICATION_VOIP: c_int = 2048;
const OPUS_OK: c_int = 0;
const SAMPLE_RATE: i32 = 16000;
const CHANNELS: i32 = 1;
const FRAME_SIZE: usize = 320; // 20ms @ 16kHz
const MAX_PACKET_SIZE: usize = 4000;

/// Opus FFI 函数签名
type OpusEncoderCreate = extern "C" fn(i32, i32, c_int, *mut c_int) -> *mut std::ffi::c_void;
type OpusEncoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusEncodeFloat = extern "C" fn(*mut std::ffi::c_void, *const f32, c_int, *mut u8, c_int) -> c_int;
type OpusDecoderCreate = extern "C" fn(i32, i32, *mut c_int) -> *mut std::ffi::c_void;
type OpusDecoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusDecodeFloat = extern "C" fn(*mut std::ffi::c_void, *const u8, c_int, *mut f32, c_int, c_int) -> c_int;

/// Opus 编解码器
pub struct OpusCodec {
    _library: Arc<Library>,
    encoder: *mut std::ffi::c_void,
    decoder: *mut std::ffi::c_void,
    encode_float: Symbol<'static, OpusEncodeFloat>,
    decode_float: Symbol<'static, OpusDecodeFloat>,
    encoder_destroy: Symbol<'static, OpusEncoderDestroy>,
    decoder_destroy: Symbol<'static, OpusDecoderDestroy>,
    // 复用编码缓冲（Opus 输出大小可变，需 buffer）；解码直接写入栈数组
    encode_buf: Vec<u8>,
}

// 确保线程安全
unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

impl OpusCodec {
    /// 创建编解码器
    #[allow(clippy::missing_transmute_annotations)]
    pub fn new() -> Result<Self> {
        let library = Arc::new(load_opus_library()?);

        // 加载函数
        let encoder_create: Symbol<OpusEncoderCreate> = unsafe {
            library.get(b"opus_encoder_create\0")
                .map_err(|e| anyhow!("无法加载 opus_encoder_create: {}", e))?
        };
        let decoder_create: Symbol<OpusDecoderCreate> = unsafe {
            library.get(b"opus_decoder_create\0")
                .map_err(|e| anyhow!("无法加载 opus_decoder_create: {}", e))?
        };

        let encode_float: Symbol<OpusEncodeFloat> = unsafe {
            library.get(b"opus_encode_float\0")
                .map_err(|e| anyhow!("无法加载 opus_encode_float: {}", e))?
        };
        let decode_float: Symbol<OpusDecodeFloat> = unsafe {
            library.get(b"opus_decode_float\0")
                .map_err(|e| anyhow!("无法加载 opus_decode_float: {}", e))?
        };
        let encoder_destroy: Symbol<OpusEncoderDestroy> = unsafe {
            library.get(b"opus_encoder_destroy\0")
                .map_err(|e| anyhow!("无法加载 opus_encoder_destroy: {}", e))?
        };
        let decoder_destroy: Symbol<OpusDecoderDestroy> = unsafe {
            library.get(b"opus_decoder_destroy\0")
                .map_err(|e| anyhow!("无法加载 opus_decoder_destroy: {}", e))?
        };

        // 创建编码器
        let mut error: c_int = 0;
        #[allow(unused_unsafe)]
        let encoder = unsafe { encoder_create(SAMPLE_RATE, CHANNELS, OPUS_APPLICATION_VOIP, &mut error) };
        if error != OPUS_OK || encoder.is_null() {
            return Err(anyhow!("创建 Opus 编码器失败: {}", error));
        }

        // 创建解码器
        let mut error: c_int = 0;
        #[allow(unused_unsafe)]
        let decoder = unsafe { decoder_create(SAMPLE_RATE, CHANNELS, &mut error) };
        if error != OPUS_OK || decoder.is_null() {
            return Err(anyhow!("创建 Opus 解码器失败: {}", error));
        }

        info!("Opus 编解码器初始化成功");

        // 泄露符号的生命周期（因为 library 是 Arc，存活不短于本结构体）
        let encode_float = unsafe { std::mem::transmute(encode_float) };
        let decode_float = unsafe { std::mem::transmute(decode_float) };
        let encoder_destroy = unsafe { std::mem::transmute(encoder_destroy) };
        let decoder_destroy = unsafe { std::mem::transmute(decoder_destroy) };

        Ok(Self {
            _library: library,
            encoder,
            decoder,
            encode_float,
            decode_float,
            encoder_destroy,
            decoder_destroy,
            encode_buf: vec![0u8; MAX_PACKET_SIZE],
        })
    }

    /// 编码 PCM 数据为 Opus
    ///
    /// input: f32 PCM 数据（16kHz, 单声道, 320样本/帧）
    /// 返回: Opus 压缩数据
    pub fn encode(&mut self, input: &[f32]) -> Result<Vec<u8>> {
        if input.len() != FRAME_SIZE {
            return Err(anyhow!("输入帧大小不正确: {} (期望 {})", input.len(), FRAME_SIZE));
        }

        // 复用 encode_buf，避免每帧分配 4000 字节
        // Rust 2021 调用 extern fn 是 safe 的，无需 unsafe 块
        let len = (self.encode_float)(
            self.encoder,
            input.as_ptr(),
            FRAME_SIZE as c_int,
            self.encode_buf.as_mut_ptr(),
            MAX_PACKET_SIZE as c_int,
        );

        if len < 0 {
            return Err(anyhow!("Opus 编码失败: {}", len));
        }

        Ok(self.encode_buf[..len as usize].to_vec())
    }

    /// 解码 Opus 数据为 PCM
    ///
    /// input: Opus 压缩数据
    /// 返回: f32 PCM 数据（16kHz, 单声道, 320样本/帧）
    pub fn decode(&mut self, input: &[u8]) -> Result<[f32; FRAME_SIZE]> {
        // 直接写入栈数组，无需复用缓冲（数组 Copy，栈上传递零堆分配）
        let mut out = [0f32; FRAME_SIZE];
        let samples = (self.decode_float)(
            self.decoder,
            input.as_ptr(),
            input.len() as c_int,
            out.as_mut_ptr(),
            FRAME_SIZE as c_int,
            0, // decode_fec
        );

        if samples < 0 {
            return Err(anyhow!("Opus 解码失败: {}", samples));
        }

        // samples <= FRAME_SIZE，out[..samples] 已由 FFI 填充，剩余保持 0（初始化值）
        Ok(out)
    }
}

impl Drop for OpusCodec {
    fn drop(&mut self) {
        // 直接调用已加载的 destroy 函数，无需每次重新 get 符号
        if !self.encoder.is_null() {
            (self.encoder_destroy)(self.encoder);
        }
        if !self.decoder.is_null() {
            (self.decoder_destroy)(self.decoder);
        }
    }
}

/// 加载 Opus 动态库（跨平台）
///
/// 搜索顺序：exe 同级 bundled 路径 → 当前目录 bundled 路径 → 系统库（dlopen 默认搜索）
fn load_opus_library() -> Result<Library> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));

    // 编译目标对应的 bundled 库相对路径（libs/libopus/{os}/{arch}/{libname}）
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

    // 系统库名（dlopen 搜索 LD_LIBRARY_PATH / 默认路径）
    #[cfg(windows)]
    const SYSTEM_NAMES: &[&str] = &["opus.dll"];
    #[cfg(target_os = "macos")]
    const SYSTEM_NAMES: &[&str] = &["libopus.dylib"];
    #[cfg(target_os = "linux")]
    const SYSTEM_NAMES: &[&str] = &["libopus.so.0", "libopus.so"];

    // macOS 额外搜索 Homebrew 路径
    #[cfg(target_os = "macos")]
    const EXTRA_PATHS: &[&str] = &["/opt/homebrew/lib", "/usr/local/lib"];
    #[cfg(not(target_os = "macos"))]
    const EXTRA_PATHS: &[&str] = &[];

    // 搜索目录：exe 同级 → 当前目录 → 额外路径
    let mut search_dirs: Vec<std::path::PathBuf> = Vec::new();
    if let Some(dir) = &exe_dir {
        search_dirs.push(dir.clone());
    }
    search_dirs.push(std::path::PathBuf::from("."));
    for extra in EXTRA_PATHS {
        search_dirs.push(std::path::PathBuf::from(extra));
    }

    // 1. 搜索 bundled 库（libs/libopus/{os}/{arch}/...）
    for dir in &search_dirs {
        let path = dir.join(BUNDLED);
        if path.exists() {
            if let Ok(lib) = unsafe { Library::new(&path) } {
                info!("已加载 Opus 库: {}", path.display());
                return Ok(lib);
            }
        }
    }

    // 2. 搜索系统库（dlopen 搜索 LD_LIBRARY_PATH / 默认路径）
    for name in SYSTEM_NAMES {
        if let Ok(lib) = unsafe { Library::new(*name) } {
            info!("已加载系统 Opus 库: {}", name);
            return Ok(lib);
        }
    }

    Err(anyhow!("未找到 opus 库，请确保 {} 存在或安装系统 opus 库", BUNDLED))
}