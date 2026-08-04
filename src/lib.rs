//! xiaozhi-rs 库根：模块声明与公共导出。
//!
//! 顶层入口为 [`supervisor::RealtimeVoice`]，仅暴露构造与运行接口；
//! 网络瞬断、设备热插拔与临时服务故障均在内部恢复。

pub mod audio;
pub mod crash;
pub mod crypto;
pub mod error;
pub mod identity;
pub mod ota;
pub mod protocol;
pub mod supervisor;

pub use supervisor::RealtimeVoice;
