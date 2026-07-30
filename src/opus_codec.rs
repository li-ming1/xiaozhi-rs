//! Opus 编解码
//!
//! 使用 libloading 加载预编译的 opus.dll

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
    // 复用编码/解码缓冲，避免每帧分配（20ms 一帧 = 每秒 50 次分配）
    encode_buf: Vec<u8>,
    decode_buf: Vec<f32>,
}

// 确保线程安全
unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

impl OpusCodec {
    /// 创建编解码器
    pub fn new() -> Result<Self> {
        // 加载 opus.dll（Windows）
        #[cfg(windows)]
        let library = {
            // 获取程序所在目录
            let exe_path = std::env::current_exe()
                .map(|p| p.parent().map(|p| p.to_path_buf()).unwrap_or_default())
                .unwrap_or_default();
            
            // 尝试从多个路径加载
            let paths = [
                exe_path.join("opus.dll"),
                std::path::PathBuf::from("opus.dll"),
                std::path::PathBuf::from("libs/libopus/win/x64/opus.dll"),
                std::path::PathBuf::from("./opus.dll"),
            ];

            let mut lib = None;
            for path in &paths {
                if path.exists() {
                    if let Ok(l) = unsafe { Library::new(path) } {
                        lib = Some(l);
                        info!("已加载 Opus 库: {}", path.display());
                        break;
                    }
                }
            }

            lib.ok_or_else(|| anyhow!("未找到 opus.dll，请将 opus.dll 复制到程序目录: {}", exe_path.display()))?
        };

        #[cfg(not(windows))]
        let library = Library::new("libopus.so.0")
            .or_else(|_| Library::new("libopus.so"))
            .map_err(|e| anyhow!("无法加载 opus 库: {}", e))?;

        let library = Arc::new(library);

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
            decode_buf: vec![0.0f32; FRAME_SIZE],
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
        #[allow(unused_unsafe)]
        let len = unsafe {
            (self.encode_float)(
                self.encoder,
                input.as_ptr(),
                FRAME_SIZE as c_int,
                self.encode_buf.as_mut_ptr(),
                MAX_PACKET_SIZE as c_int,
            )
        };

        if len < 0 {
            return Err(anyhow!("Opus 编码失败: {}", len));
        }

        Ok(self.encode_buf[..len as usize].to_vec())
    }

    /// 解码 Opus 数据为 PCM
    ///
    /// input: Opus 压缩数据
    /// 返回: f32 PCM 数据（16kHz, 单声道, 320样本/帧）
    pub fn decode(&mut self, input: &[u8]) -> Result<Vec<f32>> {
        // 复用 decode_buf，避免每帧分配 1280 字节
        #[allow(unused_unsafe)]
        let samples = unsafe {
            (self.decode_float)(
                self.decoder,
                input.as_ptr(),
                input.len() as c_int,
                self.decode_buf.as_mut_ptr(),
                FRAME_SIZE as c_int,
                0, // decode_fec
            )
        };

        if samples < 0 {
            return Err(anyhow!("Opus 解码失败: {}", samples));
        }

        if samples as usize != FRAME_SIZE {
            // 不匹配时补零到 FRAME_SIZE
            self.decode_buf[samples as usize..].fill(0.0);
        }

        Ok(self.decode_buf.clone())
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