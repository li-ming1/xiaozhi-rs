//! 设备身份管理
//!
//! 负责：MAC地址获取、UUID生成、序列号/HMAC生成、efuse缓存

use anyhow::Result;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::fs;
use uuid::Uuid;

/// 设备身份信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceIdentity {
    /// MAC地址（Device-Id）
    pub device_id: String,
    /// 客户端ID（UUID v4）
    pub client_id: String,
    /// 序列号
    pub serial_number: String,
    /// HMAC密钥
    pub hmac_key: String,
    /// 激活状态
    pub activation_status: bool,
    /// efuse文件路径（不序列化）
    #[serde(skip)]
    pub efuse_path: PathBuf,
}

/// efuse JSON 结构（平铺字段）
#[derive(Debug, Serialize, Deserialize, Default)]
struct EfuseData {
    mac_address: Option<String>,
    serial_number: Option<String>,
    hmac_key: Option<String>,
    activation_status: bool,
}

impl DeviceIdentity {
    /// 加载或创建设备身份
    pub fn load_or_create() -> Result<Self> {
        let config_dir = directories::ProjectDirs::from("com", "xiaozhi", "xiaozhi-rs")
            .map(|dirs| dirs.config_dir().to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));

        fs::create_dir_all(&config_dir)?;
        let efuse_path = config_dir.join("efuse.json");

        if efuse_path.exists() {
            Self::load_from_file(&efuse_path)
        } else {
            Self::create_new(&efuse_path)
        }
    }

    /// 从文件加载
    fn load_from_file(efuse_path: &PathBuf) -> Result<Self> {
        let content = fs::read_to_string(efuse_path)?;
        let data: EfuseData = serde_json::from_str(&content)?;

        let device_id = data.mac_address.clone().unwrap_or_else(Self::generate_mac_address);
        let client_id = Uuid::new_v4().to_string();
        let serial_number = data.serial_number.clone().unwrap_or_default();
        let hmac_key = data.hmac_key.clone().unwrap_or_default();

        let identity = DeviceIdentity {
            device_id,
            client_id,
            serial_number,
            hmac_key,
            activation_status: data.activation_status,
            efuse_path: efuse_path.clone(),
        };

        info!("已加载设备身份: {}", identity.device_id);
        Ok(identity)
    }

    /// 创建新身份
    fn create_new(efuse_path: &PathBuf) -> Result<Self> {
        info!("首次运行，正在生成设备身份...");

        // 获取MAC地址
        let device_id = Self::generate_mac_address();

        // 生成UUID
        let client_id = Uuid::new_v4().to_string();

        // 生成序列号: SN-{MD5(mac)[:8]}-{mac_clean}
        let mac_clean = device_id.replace(":", "").to_lowercase();
        let hash = format!("{:x}", Sha256::digest(&mac_clean.as_bytes()));
        let serial_number = format!("SN-{}-{}", &hash[..8].to_uppercase(), mac_clean);

        // 生成HMAC密钥
        let hostname = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let hmac_input = format!("{}||{}||{}", hostname, device_id, client_id);
        let hmac_key = format!("{:x}", Sha256::digest(hmac_input.as_bytes()));

        let identity = DeviceIdentity {
            device_id,
            client_id,
            serial_number,
            hmac_key,
            activation_status: false,
            efuse_path: efuse_path.clone(),
        };

        // 保存到文件
        identity.save()?;

        info!("已创建设备身份: {}", identity.device_id);
        Ok(identity)
    }

    /// 获取MAC地址（Windows实现）
    fn generate_mac_address() -> String {
        #[cfg(windows)]
        {
            Self::get_windows_mac_address()
        }
        #[cfg(not(windows))]
        {
            Self::get_unix_mac_address()
        }
    }

    /// Windows: 获取第一个非回环网卡的MAC
    #[cfg(windows)]
    fn get_windows_mac_address() -> String {
        // 使用 GetAdaptersInfo 获取MAC地址
        // 这里简化实现，实际应该调用Win32 API
        // 暂时返回一个伪MAC（实际项目应该调用windows crate的API）
        warn!("Windows MAC地址获取未实现，使用伪MAC");
        "00:00:00:00:00:00".to_string()
    }

    /// Unix: 获取MAC地址
    #[cfg(not(windows))]
    fn get_unix_mac_address() -> String {
        // 使用nix库获取网络接口
        warn!("Unix MAC地址获取未实现，使用伪MAC");
        "00:00:00:00:00:00".to_string()
    }

    /// 检查是否已激活
    pub fn is_activated(&self) -> bool {
        self.activation_status
    }

    /// 设置激活状态
    #[allow(dead_code)]
    pub fn set_activated(&mut self, status: bool) -> Result<()> {
        self.activation_status = status;
        self.save()?;
        Ok(())
    }

    /// 生成HMAC签名
    pub fn generate_hmac_signature(&self, challenge: &str) -> String {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(self.hmac_key.as_bytes())
            .expect("HMAC初始化失败");
        mac.update(challenge.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// 保存到文件
    fn save(&self) -> Result<()> {
        let data = EfuseData {
            mac_address: Some(self.device_id.clone()),
            serial_number: Some(self.serial_number.clone()),
            hmac_key: Some(self.hmac_key.clone()),
            activation_status: self.activation_status,
        };

        let content = serde_json::to_string_pretty(&data)?;
        let tmp_path = self.efuse_path.with_extension("tmp");
        fs::write(&tmp_path, content)?;
        fs::rename(&tmp_path, &self.efuse_path)?;

        Ok(())
    }
}