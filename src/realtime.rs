//! `RealtimeVoice`：对外唯一入口，只暴露构造与运行接口。
//!
//! 网络瞬断、设备热插拔与临时服务故障均在内部由 `VoiceSupervisor` 恢复，
//! 调用方无需感知。构造需 `DeviceIdentity` 与 `OtaConfig`（见 `identity`/`ota`）。

use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::identity::DeviceIdentity;
use crate::ota::OtaConfig;
use crate::supervisor::VoiceSupervisor;

/// 实时语音客户端入口。
pub struct RealtimeVoice {
    identity: DeviceIdentity,
    ota: OtaConfig,
}

impl RealtimeVoice {
    /// 由设备身份与 OTA 配置构造。
    pub fn from_ota(identity: DeviceIdentity, ota: OtaConfig) -> Result<Self> {
        Ok(Self { identity, ota })
    }

    /// 运行至 `shutdown` 取消或不可恢复错误。
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<()> {
        let mut supervisor = VoiceSupervisor::new(self.identity, self.ota, shutdown);
        supervisor.run().await
    }
}
