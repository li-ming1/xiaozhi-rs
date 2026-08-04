//! xiaozhi-rs 库根：模块声明与公共导出，顶层入口为 [`supervisor::RealtimeVoice`]。

pub mod audio;
pub mod crypto;
pub mod error;
pub mod identity;
pub mod ota;
pub mod protocol;
pub mod supervisor;

pub use supervisor::RealtimeVoice;
