//! Opus 编解码：libloading 动态链接（opus.dll / libopus.dylib / libopus.so），跨平台。

use anyhow::{anyhow, Result};
use libloading::{Library, Symbol};
use log::{info, warn};
use std::ffi::c_int;
use std::sync::Arc;

/// Opus 常量
const OPUS_APPLICATION_VOIP: c_int = 2048;
const OPUS_SET_COMPLEXITY_REQUEST: c_int = 4010;
const OPUS_SET_BITRATE_REQUEST: c_int = 4002;
const OPUS_SET_SIGNAL_REQUEST: c_int = 4024;
const OPUS_SIGNAL_VOICE: c_int = 3001;
const OPUS_SET_INBAND_FEC_REQUEST: c_int = 4012;
const OPUS_SET_DTX_REQUEST: c_int = 4016;
const OPUS_OK: c_int = 0;
const SAMPLE_RATE: i32 = 16000;
const CHANNELS: i32 = 1;
const FRAME_SIZE: usize = 320; // 20ms @ 16kHz
const MAX_PACKET_SIZE: usize = 1500; // 覆盖 1275B 理论上限，余量充足
/// 编码复杂度 5（默认 10）：窄带单声道语音听感无差，SILK 主导路径 CPU 约降 40~50%。
const COMPLEXITY: c_int = 5;
/// 比特率 48 kbps：16kHz 语音接近听感透明（默认 ~24-32kbps 齿音/摩擦音有损）。
const BITRATE: c_int = 48000;

/// Opus FFI 函数签名
type OpusEncoderCreate = extern "C" fn(i32, i32, c_int, *mut c_int) -> *mut std::ffi::c_void;
type OpusEncoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusEncodeFloat = extern "C" fn(*mut std::ffi::c_void, *const f32, c_int, *mut u8, c_int) -> c_int;
type OpusDecoderCreate = extern "C" fn(i32, i32, *mut c_int) -> *mut std::ffi::c_void;
type OpusDecoderDestroy = extern "C" fn(*mut std::ffi::c_void);
type OpusDecodeFloat = extern "C" fn(*mut std::ffi::c_void, *const u8, c_int, *mut f32, c_int, c_int) -> c_int;
// C 可变参数：opus_encoder_ctl(st, request, value)
type OpusEncoderCtl = unsafe extern "C" fn(*mut std::ffi::c_void, c_int, ...) -> c_int;

/// Opus 编解码器：保有库与符号，编码缓冲复用，解码直写栈数组。
pub struct OpusCodec {
    _library: Arc<Library>,
    encoder: *mut std::ffi::c_void,
    decoder: *mut std::ffi::c_void,
    encode_float: Symbol<'static, OpusEncodeFloat>,
    decode_float: Symbol<'static, OpusDecodeFloat>,
    encoder_destroy: Symbol<'static, OpusEncoderDestroy>,
    decoder_destroy: Symbol<'static, OpusDecoderDestroy>,
    encode_buf: Vec<u8>,
}

// 原生指针经 Arc<Library> 绑定生命周期，跨线程安全。
unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

impl OpusCodec {
    /// 加载符号、建链编解码器并下调编码复杂度。
    pub fn new() -> Result<Self> {
        let library = Arc::new(load_opus_library()?);

        // transmute 擦除符号生命周期；library 由 Arc 持有，存活不短于本结构体。
        let encoder_create: Symbol<OpusEncoderCreate> = unsafe { load_symbol(&library, b"opus_encoder_create\0")? };
        let decoder_create: Symbol<OpusDecoderCreate> = unsafe { load_symbol(&library, b"opus_decoder_create\0")? };
        let encode_float = unsafe { load_symbol(&library, b"opus_encode_float\0")? };
        let decode_float = unsafe { load_symbol(&library, b"opus_decode_float\0")? };
        let encoder_destroy = unsafe { load_symbol(&library, b"opus_encoder_destroy\0")? };
        let decoder_destroy = unsafe { load_symbol(&library, b"opus_decoder_destroy\0")? };

        let mut error: c_int = 0;
        #[allow(unused_unsafe)]
        let encoder = unsafe { encoder_create(SAMPLE_RATE, CHANNELS, OPUS_APPLICATION_VOIP, &mut error) };
        if error != OPUS_OK || encoder.is_null() {
            return Err(anyhow!("创建 Opus 编码器失败: {}", error));
        }

        // 编码器调参：复杂度/比特率/信号类型/前向纠错/DTX 一次性下发。
        // - 复杂度 5（默认 10）：听感无损、算力减半
        // - 比特率 48kbps：16kHz 语音接近听感透明
        // - 信号类型 VOICE：指导编码器对语音优化决策
        // - inband FEC=1：把前一帧低码率冗余编入当前帧，服务器侧丢包可恢复
        // - DTX=0：关闭静音段停止发送，避免说话间隙恢复时的前导 artifacts
        if let Ok(ctl) = unsafe { library.get::<OpusEncoderCtl>(b"opus_encoder_ctl\0") } {
            let apply = |req: c_int, val: c_int, name: &str| {
                let ret = unsafe { ctl(encoder, req, val) };
                if ret != OPUS_OK {
                    warn!("设置 Opus {} 失败: {}", name, ret);
                }
            };
            apply(OPUS_SET_COMPLEXITY_REQUEST, COMPLEXITY, "复杂度");
            apply(OPUS_SET_BITRATE_REQUEST, BITRATE, "比特率");
            apply(OPUS_SET_SIGNAL_REQUEST, OPUS_SIGNAL_VOICE, "信号类型");
            apply(OPUS_SET_INBAND_FEC_REQUEST, 1, "前向纠错");
            apply(OPUS_SET_DTX_REQUEST, 0, "DTX");
        }

        let mut error: c_int = 0;
        #[allow(unused_unsafe)]
        let decoder = unsafe { decoder_create(SAMPLE_RATE, CHANNELS, &mut error) };
        if error != OPUS_OK || decoder.is_null() {
            return Err(anyhow!("创建 Opus 解码器失败: {}", error));
        }

        info!("Opus 编解码器初始化成功");

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

    /// 编码单帧 f32 PCM（16kHz 单声道，320 样本）为 Opus 包。
    pub fn encode(&mut self, input: &[f32]) -> Result<Vec<u8>> {
        if input.len() != FRAME_SIZE {
            return Err(anyhow!("输入帧大小不正确: {} (期望 {})", input.len(), FRAME_SIZE));
        }

        // 复用 encode_buf，免每帧分配。
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

    /// 解码 Opus 包为单帧 f32 PCM，直写栈数组（零堆分配）。
    pub fn decode(&mut self, input: &[u8]) -> Result<[f32; FRAME_SIZE]> {
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

        Ok(out)
    }

    /// 丢包隐藏（PLC）：无数据到达时基于解码器内部状态外推一帧补偿，
    /// 替代静音填充，避免断流处的可闻咔哒/空洞。
    pub fn decode_plc(&mut self) -> Result<[f32; FRAME_SIZE]> {
        let mut out = [0f32; FRAME_SIZE];
        let samples = (self.decode_float)(
            self.decoder,
            std::ptr::null(),
            0,
            out.as_mut_ptr(),
            FRAME_SIZE as c_int,
            0,
        );

        if samples < 0 {
            return Err(anyhow!("Opus PLC 失败: {}", samples));
        }

        Ok(out)
    }
}

impl Drop for OpusCodec {
    fn drop(&mut self) {
        // 调用已加载的销毁函数即可，无需重新解析符号。
        if !self.encoder.is_null() {
            (self.encoder_destroy)(self.encoder);
        }
        if !self.decoder.is_null() {
            (self.decoder_destroy)(self.decoder);
        }
    }
}

/// 解析符号并将生命周期擦除为 'static（调用方须保证 library 存活不短于返回值）。
#[allow(clippy::missing_transmute_annotations)]
unsafe fn load_symbol<T>(library: &Library, name: &'static [u8]) -> Result<Symbol<'static, T>> {
    let sym: Symbol<T> = unsafe { library.get(name) }
        .map_err(|e| anyhow!("无法加载 {}: {}", String::from_utf8_lossy(name), e))?;
    Ok(unsafe { std::mem::transmute(sym) })
}

/// 加载 Opus 动态库：bundled 路径优先，回退系统库（dlopen 默认搜索）。
fn load_opus_library() -> Result<Library> {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));

    // 按编译目标选择 bundled 相对路径。
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

    // 系统库名。
    #[cfg(windows)]
    const SYSTEM_NAMES: &[&str] = &["opus.dll"];
    #[cfg(target_os = "macos")]
    const SYSTEM_NAMES: &[&str] = &["libopus.dylib"];
    #[cfg(target_os = "linux")]
    const SYSTEM_NAMES: &[&str] = &["libopus.so.0", "libopus.so"];

    // macOS 额外搜索 Homebrew 路径。
    #[cfg(target_os = "macos")]
    const EXTRA_PATHS: &[&str] = &["/opt/homebrew/lib", "/usr/local/lib"];
    #[cfg(not(target_os = "macos"))]
    const EXTRA_PATHS: &[&str] = &[];

    let mut search_dirs: Vec<std::path::PathBuf> = Vec::new();
    if let Some(dir) = &exe_dir {
        search_dirs.push(dir.clone());
    }
    search_dirs.push(std::path::PathBuf::from("."));
    for extra in EXTRA_PATHS {
        search_dirs.push(std::path::PathBuf::from(extra));
    }

    // 1. bundled 路径优先。
    for dir in &search_dirs {
        let path = dir.join(BUNDLED);
        if path.exists() {
            if let Ok(lib) = unsafe { Library::new(&path) } {
                info!("已加载 Opus 库: {}", path.display());
                return Ok(lib);
            }
        }
    }

    // 2. 回退系统库。
    for name in SYSTEM_NAMES {
        if let Ok(lib) = unsafe { Library::new(*name) } {
            info!("已加载系统 Opus 库: {}", name);
            return Ok(lib);
        }
    }

    Err(anyhow!("未找到 opus 库，请确保 {} 存在或安装系统 opus 库", BUNDLED))
}