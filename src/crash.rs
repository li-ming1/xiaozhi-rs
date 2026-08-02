//! Windows 未处理异常过滤器：捕获原生崩溃（访问冲突等），
//! 将异常码、出错地址与所在模块写入 `xiaozhi-crash.log`。
//!
//! Rust panic 不经过此路径（由全局 panic 钩子处理）；只有 C/原生层
//! 段错误/访问冲突会触发这里。用于定位 cpal/WASAPI、opus FFI 等原生崩溃。

#![cfg(windows)]

use windows_sys::Win32::System::Diagnostics::Debug::{SetUnhandledExceptionFilter, EXCEPTION_POINTERS};
use windows_sys::Win32::System::LibraryLoader::{
    GetModuleFileNameA, GetModuleHandleExA, GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
};

/// 安装崩溃过滤器（幂等）。
pub fn install() {
    unsafe {
        SetUnhandledExceptionFilter(Some(crash_handler));
    }
}

unsafe extern "system" fn crash_handler(ep: *const EXCEPTION_POINTERS) -> i32 {
    // EXCEPTION_CONTINUE_SEARCH：写完日志后交还系统默认处理。
    let mut line = String::new();
    unsafe {
        if !ep.is_null() {
            let ep_ref = &*ep;
            if !ep_ref.ExceptionRecord.is_null() {
                let er = &*ep_ref.ExceptionRecord;
                let addr = er.ExceptionAddress as usize;
                line.push_str(&format!(
                    "CRASH ExceptionCode=0x{:08x} at 0x{:x}",
                    er.ExceptionCode, addr
                ));
                // 出错地址所在模块（HMODULE 即模块基址，可算偏移）。
                let mut hmod: windows_sys::Win32::Foundation::HMODULE = std::ptr::null_mut();
                GetModuleHandleExA(
                    GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS,
                    addr as *const u8,
                    &mut hmod,
                );
                if !hmod.is_null() {
                    let base = hmod as usize;
                    let mut buf = [0u8; 512];
                    let len = GetModuleFileNameA(hmod, buf.as_mut_ptr(), buf.len() as u32);
                    if len > 0 {
                        let name = String::from_utf8_lossy(&buf[..len as usize]);
                        line.push_str(&format!(
                            "  module={}  base=0x{:x}  offset=0x{:x}",
                            name,
                            base,
                            addr.saturating_sub(base)
                        ));
                    }
                }
                // 最近一次 opus 操作（区分 encode/decode/ctl/destroy）。
                use crate::audio::opus::{
                    OPUS_CALL_CTL, OPUS_CALL_DECODE, OPUS_CALL_DESTROY, OPUS_CALL_ENCODE,
                    LAST_OPUS_CALL,
                };
                let op = match LAST_OPUS_CALL.load(std::sync::atomic::Ordering::Relaxed) {
                    OPUS_CALL_ENCODE => "opus_encode",
                    OPUS_CALL_DECODE => "opus_decode",
                    OPUS_CALL_CTL => "opus_ctl",
                    OPUS_CALL_DESTROY => "opus_destroy",
                    _ => "none",
                };
                let ctl_req = crate::audio::opus::LAST_OPUS_CTL_REQUEST
                    .load(std::sync::atomic::Ordering::Relaxed);
                if ctl_req != 0 {
                    line.push_str(&format!("  last_opus_call={}  last_ctl_request={}", op, ctl_req));
                } else {
                    line.push_str(&format!("  last_opus_call={}", op));
                }
            }
        }
    }
    line.push('\n');
    let _ = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("xiaozhi-crash.log")
        .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
    0
}
