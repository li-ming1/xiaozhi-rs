//! 设备身份：MAC 派生、UUID、序列号/HMAC 生成与 efuse 持久化。

use anyhow::Result;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// 设备身份凭证，同时作为 WebSocket 握手载荷。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceIdentity {
    pub device_id: String,
    pub client_id: String,
    pub serial_number: String,
    pub hmac_key: String,
    pub activation_status: bool,
    #[serde(skip)]
    pub efuse_path: PathBuf,
}

/// efuse 持久化结构；缺字段反序列化为空串/false，向后兼容旧文件。
#[derive(Debug, Serialize, Deserialize, Default)]
struct EfuseData {
    #[serde(default)]
    mac_address: String,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    serial_number: String,
    #[serde(default)]
    hmac_key: String,
    #[serde(default)]
    activation_status: bool,
}

fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

/// 应用配置目录（跨平台）：
/// Windows `%APPDATA%\xiaozhi-rs`；macOS `~/Library/Application Support/xiaozhi-rs`；
/// Linux `$XDG_CONFIG_HOME/xiaozhi-rs` 或 `~/.config/xiaozhi-rs`。
fn app_config_dir() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("APPDATA")
            .map(|p| PathBuf::from(p).join("xiaozhi-rs"))
            .unwrap_or_else(|| PathBuf::from("."))
    }
    #[cfg(target_os = "macos")]
    {
        std::env::var_os("HOME")
            .map(|h| {
                PathBuf::from(h)
                    .join("Library")
                    .join("Application Support")
                    .join("xiaozhi-rs")
            })
            .unwrap_or_else(|| PathBuf::from("."))
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
            .map(|d| d.join("xiaozhi-rs"))
            .unwrap_or_else(|| PathBuf::from("."))
    }
    #[cfg(not(any(windows, target_os = "macos", unix)))]
    {
        PathBuf::from(".")
    }
}

fn hostname_str() -> String {
    #[cfg(windows)]
    let host = std::env::var("COMPUTERNAME").ok();
    #[cfg(target_os = "macos")]
    let host = std::env::var("HOSTNAME").ok();
    #[cfg(all(unix, not(target_os = "macos")))]
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    #[cfg(not(any(windows, target_os = "macos", unix)))]
    let host = None;
    host.unwrap_or_else(|| "unknown".to_string())
}

impl DeviceIdentity {
    /// 定位配置目录，存在则加载，否则生成并落盘。
    pub fn load_or_create() -> Result<Self> {
        let config_dir = app_config_dir();

        fs::create_dir_all(&config_dir)?;
        let efuse_path = config_dir.join("efuse.json");

        if efuse_path.exists() {
            Self::load_from_file(&efuse_path)
        } else {
            Self::create_new(&efuse_path)
        }
    }

    fn load_from_file(efuse_path: &Path) -> Result<Self> {
        let content = fs::read_to_string(efuse_path)?;

        if content.trim().is_empty() {
            return Self::create_new(efuse_path);
        }

        let data: EfuseData = match serde_json::from_str(&content) {
            Ok(d) => d,
            Err(e) => {
                // 损坏文件先留档再重建，避免直接覆盖掉可恢复的设备身份。
                let backup = efuse_path.with_extension("corrupt.json");
                let _ = fs::copy(efuse_path, &backup);
                warn!(
                    "efuse 解析失败({})，已备份到 {} 并重新生成身份",
                    e,
                    backup.display()
                );
                return Self::create_new(efuse_path);
            }
        };

        let client_id_missing = data.client_id.is_empty();
        let client_id = if client_id_missing {
            Uuid::new_v4().to_string()
        } else {
            data.client_id
        };
        let device_id = if data.mac_address.is_empty() {
            Self::generate_mac_address()
        } else {
            data.mac_address
        };
        // 旧版 efuse 可能缺 serial/hmac：按与首次创建相同的规则补齐并落盘；
        // 已有字段原样保留，避免改动可用的序列号。
        let credentials_missing = data.serial_number.is_empty() || data.hmac_key.is_empty();
        let (serial_number, hmac_key) = if credentials_missing {
            let (serial, hmac) = derive_credentials(&device_id, &client_id);
            (
                if data.serial_number.is_empty() {
                    serial
                } else {
                    data.serial_number
                },
                if data.hmac_key.is_empty() {
                    hmac
                } else {
                    data.hmac_key
                },
            )
        } else {
            (data.serial_number, data.hmac_key)
        };

        let identity = DeviceIdentity {
            device_id,
            client_id,
            serial_number,
            hmac_key,
            activation_status: data.activation_status,
            efuse_path: efuse_path.to_path_buf(),
        };

        // 补出的字段必须落盘，否则每次启动都会重新生成。
        if client_id_missing || credentials_missing {
            identity.save()?;
            info!("已补齐设备身份字段: {}", identity.device_id);
        }

        info!("已加载设备身份: {}", identity.device_id);
        Ok(identity)
    }

    /// 首次运行：派生全部凭证并原子落盘。
    fn create_new(efuse_path: &Path) -> Result<Self> {
        info!("首次运行，正在生成设备身份...");

        let device_id = Self::generate_mac_address();
        let client_id = Uuid::new_v4().to_string();
        let (serial_number, hmac_key) = derive_credentials(&device_id, &client_id);

        let identity = DeviceIdentity {
            device_id,
            client_id,
            serial_number,
            hmac_key,
            activation_status: false,
            efuse_path: efuse_path.to_path_buf(),
        };

        identity.save()?;

        info!("已创建设备身份: {}", identity.device_id);
        Ok(identity)
    }

    /// 跨平台 MAC：取 UUID v4 随机字节，置本地管理/清多播位，规避全局地址冲突。
    fn generate_mac_address() -> String {
        let bytes = *Uuid::new_v4().as_bytes();
        let b0 = (bytes[0] & 0xFE) | 0x02;
        format!(
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            b0, bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
        )
    }

    /// 测试用 MAC（服务器自动授权）。
    pub const TEST_MAC: &str = "00:00:00:00:00:00";

    pub fn is_activated(&self) -> bool {
        self.activation_status
    }

    /// 以 HMAC-SHA256 对挑战码签名。
    pub fn generate_hmac_signature(&self, challenge: &str) -> String {
        use hmac::{KeyInit, Mac, SimpleHmac};
        type HmacSha256 = SimpleHmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(self.hmac_key.as_bytes()).expect("HMAC初始化失败");
        mac.update(challenge.as_bytes());
        to_hex(&mac.finalize().into_bytes())
    }

    /// 原子写入：临时文件 rename 覆盖，避免半写损坏。
    fn save(&self) -> Result<()> {
        let data = EfuseData {
            mac_address: self.device_id.clone(),
            client_id: self.client_id.clone(),
            serial_number: self.serial_number.clone(),
            hmac_key: self.hmac_key.clone(),
            activation_status: self.activation_status,
        };

        let content = serde_json::to_string_pretty(&data)?;
        let tmp_path = self.efuse_path.with_extension("tmp");
        fs::write(&tmp_path, content)?;
        fs::rename(&tmp_path, &self.efuse_path)?;

        Ok(())
    }

    /// 标记已激活并落盘。
    pub fn set_activated(&mut self) -> Result<()> {
        self.activation_status = true;
        self.save()?;
        Ok(())
    }
}

/// 序列号 = SN-{hash[:8]}-{mac_clean}；HMAC 密钥绑定主机名，防止凭证跨机复用。
/// 首次创建与旧版 efuse 补字段共用同一规则，保证同一设备派生结果一致。
fn derive_credentials(device_id: &str, client_id: &str) -> (String, String) {
    let mac_clean = device_id.replace(':', "").to_lowercase();
    let hash = to_hex(&Sha256::digest(mac_clean.as_bytes()));
    let serial_number = format!("SN-{}-{}", hash[..8].to_uppercase(), mac_clean);
    let hmac_input = format!("{}||{}||{}", hostname_str(), device_id, client_id);
    let hmac_key = to_hex(&Sha256::digest(hmac_input.as_bytes()));
    (serial_number, hmac_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同一 MAC/client_id 必须派生稳定且格式正确的凭证。
    #[test]
    fn derive_credentials_is_stable_and_well_formed() {
        let (serial, key) = derive_credentials("02:00:00:00:00:01", "client-1");
        assert!(serial.starts_with("SN-"), "序列号前缀异常: {}", serial);
        assert!(
            serial.ends_with("020000000001"),
            "序列号后缀异常: {}",
            serial
        );
        assert_eq!(key.len(), 64, "HMAC 密钥应为 32 字节 hex");
        assert_eq!(
            (serial.clone(), key.clone()),
            derive_credentials("02:00:00:00:00:01", "client-1")
        );
    }

    /// 损坏的 efuse 必须先备份，再重建身份，避免静默丢失 MAC/序列号/HMAC。
    #[test]
    fn corrupt_efuse_is_backed_up_before_recreate() {
        let dir = std::env::temp_dir().join(format!("xiaozhi-rs-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("efuse.json");
        std::fs::write(&path, "{not-json").unwrap();

        let identity = DeviceIdentity::load_from_file(&path).unwrap();
        assert!(!identity.device_id.is_empty());
        let backup = path.with_extension("corrupt.json");
        assert_eq!(
            std::fs::read_to_string(&backup).unwrap(),
            "{not-json",
            "损坏文件未备份"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
