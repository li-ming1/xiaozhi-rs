//! AES-128-CTR 加密与 UDP 音频包 16 字节头编解码。
//!
//! ## IV / 包头设计（与官方 ESP32 `mqtt_protocol.cc` byte-for-byte 对齐）
//!
//! 服务器在 hello 响应 `udp.nonce` 下发 16 字节 base nonce。每包取该 base nonce
//! 并覆写以下字段（网络字节序）构成 16 字节缓冲：
//!
//! ```text
//! offset  字段          覆写?
//! 0       type(0x01)    否（base nonce[0]，服务器保证为 0x01）
//! 1       flags         否（base nonce[1]）
//! 2..4    payload_len   是（加密载荷长度，BE）
//! 4..8    ssrc          否（base nonce[4..8]，会话内常量）
//! 8..12   timestamp     是（BE）
//! 12..16  sequence      是（BE）
//! ```
//!
//! 该 16 字节既作为 UDP 包头发送，又作为 AES-CTR 的 IV —— 接收方取前 16 字节
//! 即可还原 IV，无需额外传输。由于 sequence 单调递增，每包 IV 唯一，无计数器复用。
//! AES-CTR 为流密码，加密与解密是同一 XOR 操作。

use aes::cipher::{Array, KeyIvInit, StreamCipher};
use aes::Aes128;
use ctr::Ctr128BE;

type AesCtr = Ctr128BE<Aes128>;

/// UDP 音频包头固定 16 字节。
pub const HEADER_SIZE: usize = 16;
/// type 字段固定值。
pub const TYPE_AUDIO: u8 = 0x01;

/// AES-128-CTR 会话加密器：持有密钥，每包用唯一 IV（即包头）。
#[derive(Clone)]
pub struct AesCtrCipher {
    key: [u8; 16],
}

impl AesCtrCipher {
    /// 从 hex 字符串构造（服务器 hello 响应 `udp.key`，32 字符）。
    pub fn from_hex_key(key_hex: &str) -> Result<Self, CryptoError> {
        let key = hex_decode_16(key_hex)?;
        Ok(Self { key })
    }

    /// 原地执行 CTR 流加密/解密（同一操作）。`iv` 即 16 字节包头。
    pub fn apply_keystream(&self, iv: &[u8; 16], data: &mut [u8]) {
        let key = Array::from(self.key);
        let iv_arr = Array::from(*iv);
        let mut cipher = AesCtr::new(&key, &iv_arr);
        cipher.apply_keystream(data);
    }
}

/// UDP 音频包头（16 字节，网络字节序字段）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpAudioHeader {
    pub type_: u8,
    pub flags: u8,
    pub payload_len: u16,
    pub ssrc: u32,
    pub timestamp: u32,
    pub sequence: u32,
}

impl UdpAudioHeader {
    /// 由 base nonce + 当前包字段构造完整 16 字节（同时作为 IV）。
    /// 仅覆写 payload_len/timestamp/sequence，与官方 SendAudio 对齐。
    pub fn build_iv(&self, base_nonce: &[u8; 16]) -> [u8; 16] {
        let mut iv = *base_nonce;
        iv[2..4].copy_from_slice(&self.payload_len.to_be_bytes());
        iv[8..12].copy_from_slice(&self.timestamp.to_be_bytes());
        iv[12..16].copy_from_slice(&self.sequence.to_be_bytes());
        iv
    }

    /// 从 16 字节包头解析（接收侧）。
    pub fn parse(buf: &[u8; 16]) -> Self {
        Self {
            type_: buf[0],
            flags: buf[1],
            payload_len: u16::from_be_bytes([buf[2], buf[3]]),
            ssrc: u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]),
            timestamp: u32::from_be_bytes([buf[8], buf[9], buf[10], buf[11]]),
            sequence: u32::from_be_bytes([buf[12], buf[13], buf[14], buf[15]]),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("hex 长度错误: 期望 32 字符, 得 {0}")]
    BadHexLen(usize),
    #[error("hex 解码失败: {0}")]
    BadHex(String),
}

/// 解码 32 字符 hex 为 16 字节。
pub fn hex_decode_16(s: &str) -> Result<[u8; 16], CryptoError> {
    if s.len() != 32 {
        return Err(CryptoError::BadHexLen(s.len()));
    }
    let mut out = [0u8; 16];
    let b = s.as_bytes();
    for i in 0..16 {
        let hi = hex_val(b[i * 2])?;
        let lo = hex_val(b[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(c: u8) -> Result<u8, CryptoError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(CryptoError::BadHex(format!("非法 hex 字符: {}", c as char))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_build_and_parse_roundtrip() {
        let base = [0x01u8, 0x00, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF,
                    0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let h = UdpAudioHeader {
            type_: 0x01,
            flags: 0x00,
            payload_len: 0x1234,
            ssrc: 0, // 由 base nonce[4..8] 决定，不在 build_iv 覆写
            timestamp: 0xDEADBEEF,
            sequence: 0x00000007,
        };
        let iv = h.build_iv(&base);
        // type/flags/ssrc 区保留 base nonce
        assert_eq!(iv[0], 0x01);
        assert_eq!(iv[1], 0x00);
        assert_eq!(&iv[4..8], &base[4..8]);
        // 覆写区
        assert_eq!(&iv[2..4], &[0x12, 0x34]);
        assert_eq!(&iv[8..12], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(&iv[12..16], &[0x00, 0x00, 0x00, 0x07]);

        let parsed = UdpAudioHeader::parse(&iv);
        assert_eq!(parsed.type_, 0x01);
        assert_eq!(parsed.payload_len, 0x1234);
        assert_eq!(parsed.timestamp, 0xDEADBEEF);
        assert_eq!(parsed.sequence, 7);
    }

    #[test]
    fn aes_ctr_roundtrip() {
        let key = [0u8; 16];
        let cipher = AesCtrCipher { key };
        let iv = [0u8; 16];
        let mut data = b"hello xiaozhi udp audio".to_vec();
        let original = data.clone();
        cipher.apply_keystream(&iv, &mut data);
        assert_ne!(data, original);
        cipher.apply_keystream(&iv, &mut data);
        assert_eq!(data, original);
    }

    #[test]
    fn hex_decode_ok_and_bad() {
        assert!(hex_decode_16("0123456789ABCDEF0123456789ABCDEF").is_ok());
        assert!(hex_decode_16("short").is_err());
        assert!(hex_decode_16("ZZ23456789ABCDEF0123456789ABCDEF").is_err());
    }
}
