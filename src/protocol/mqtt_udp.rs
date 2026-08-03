//! MQTT+UDP 混合传输（主链路）。
//!
//! 协议要点（来自官方 mqtt-udp_zh.md）：
//! - 控制通道 MQTT：QoS 0、clean session、keepalive 240s；hello `version=3`、`transport="udp"`。
//! - 数据通道 UDP：16 字节包头（= AES-CTR IV），载荷为加密 Opus。
//! - 服务器 hello 响应携带 `udp.server/port/key/nonce`（key/nonce 为 32 字符 hex）。
//! - 包头与 IV 同体，见 [`crate::crypto`]。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::{debug, info, warn};
use rumqttc::{
    AsyncClient, Event, EventLoop, Incoming, MqttOptions, NetworkOptions, QoS, TlsConfiguration,
    Transport,
};
use rustls::ClientConfig;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use crate::crypto::{hex_decode_16, AesCtrCipher, UdpAudioHeader, HEADER_SIZE, TYPE_AUDIO};
use crate::error::{Result, VoiceError};
use crate::protocol::message::{AudioParams, ClientMessage, ServerMessage};

use super::{now_ms, ConnectParams, IncomingEvent, MqttParams, TransportHandles};

const MQTT_KEEP_ALIVE: Duration = Duration::from_secs(240);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const CONTROL_CHANNEL_CAP: usize = 16;
const INCOMING_CHANNEL_CAP: usize = 64;
const REQUEST_CAP: usize = 64;
/// UDP 接收缓冲上限（Opus 最大包 + 包头，留余量）。
const UDP_MAX_PACKET: usize = 2048;

/// MQTT+UDP 传输。
pub struct MqttUdpTransport;

/// hello 响应的完整协商结果。
struct Negotiated {
    session_id: String,
    server_audio: AudioParams,
    udp: crate::protocol::message::UdpChannel,
}

impl MqttUdpTransport {
    pub async fn connect(self, params: &ConnectParams) -> Result<TransportHandles> {
        let mqtt = params
            .mqtt
            .as_ref()
            .ok_or_else(|| VoiceError::InvalidConfig("缺少 MQTT 参数".into()))?;

        // ---------- 1. MQTT 连接 ----------
        let opts = build_mqtt_options(mqtt)?;
        let (client, mut eventloop) = AsyncClient::new(opts, REQUEST_CAP);
        // 网络层：连接超时 10s、TCP_NODELAY（官方实现一致）。
        let mut netopts = NetworkOptions::new();
        netopts.set_connection_timeout(10);
        netopts.set_tcp_nodelay(true);
        eventloop.set_network_options(netopts);

        client
            .subscribe(mqtt.subscribe_topic.clone(), QoS::AtMostOnce)
            .await
            .map_err(|e| VoiceError::Transport(format!("MQTT 订阅失败: {}", e)))?;

        // 发布 hello（version=3, transport=udp）。
        let hello = ClientMessage::hello_mqtt_udp();
        let hello_json = serde_json::to_string(&hello)?;
        client
            .publish(
                mqtt.publish_topic.clone(),
                QoS::AtMostOnce,
                false,
                hello_json,
            )
            .await
            .map_err(|e| VoiceError::Transport(format!("MQTT 发布 hello 失败: {}", e)))?;

        // ---------- 2. 等待 hello 响应，提取 UDP 参数 ----------
        let negotiated = tokio::time::timeout(HELLO_TIMEOUT, wait_hello(&mut eventloop))
            .await
            .map_err(|_| VoiceError::Timeout("MQTT hello 响应超时".into()))??;

        let base_nonce = hex_decode_16(&negotiated.udp.nonce)
            .map_err(|e| VoiceError::Crypto(e.to_string()))?;
        let cipher = AesCtrCipher::from_hex_key(&negotiated.udp.key)
            .map_err(|e| VoiceError::Crypto(e.to_string()))?;
        info!(
            "MQTT hello 成功，UDP {}:{}，下行 {}Hz/{}ms",
            negotiated.udp.server,
            negotiated.udp.port,
            negotiated.server_audio.sample_rate,
            negotiated.server_audio.frame_duration
        );

        // ---------- 3. UDP 套接字 ----------
        let udp_socket = Arc::new(
            UdpSocket::bind("0.0.0.0:0")
                .await
                .map_err(|e| VoiceError::Transport(format!("UDP 绑定失败: {}", e)))?,
        );
        let addr: SocketAddr = format!("{}:{}", negotiated.udp.server, negotiated.udp.port)
            .parse()
            .map_err(|e| VoiceError::InvalidConfig(format!("UDP 地址无效: {}", e)))?;
        udp_socket
            .connect(addr)
            .await
            .map_err(|e| VoiceError::Transport(format!("UDP 连接失败: {}", e)))?;

        // ---------- 4. 通道与任务 ----------
        let (control_tx, control_rx) = mpsc::channel(CONTROL_CHANNEL_CAP);
        let (audio_tx, audio_rx) = super::LatestSlot::<Vec<u8>>::new().pipe();
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_CHANNEL_CAP);
        let (close_tx, _close_rx) = mpsc::channel(1);

        // 出站任务：控制走 MQTT，音频走 UDP（加密）。
        let mqtt_send_client = client.clone();
        let mqtt_send_params = mqtt.clone();
        tokio::spawn(send_loop(
            mqtt_send_client,
            mqtt_send_params,
            control_rx,
            audio_rx,
            udp_socket.clone(),
            cipher.clone(),
            base_nonce,
        ));

        // MQTT 事件循环：hello 之后的下行 JSON 路由。
        tokio::spawn(mqtt_event_loop(eventloop, incoming_tx.clone()));

        // UDP 接收：解包头 → 防重放 → 解密 → Audio。
        tokio::spawn(udp_recv_loop(udp_socket, cipher, incoming_tx.clone()));

        Ok(TransportHandles {
            session_id: negotiated.session_id,
            server_audio: negotiated.server_audio,
            control_tx,
            audio_tx,
            incoming_rx,
            close_tx,
        })
    }
}

/// 构建 MQTT 选项（TLS 用 ring provider 注入的 ClientConfig）。
fn build_mqtt_options(mqtt: &MqttParams) -> Result<MqttOptions> {
    let mut opts = MqttOptions::new(&mqtt.mqtt_client_id, &mqtt.host, mqtt.port);
    opts.set_keep_alive(MQTT_KEEP_ALIVE);
    opts.set_inflight(1);
    opts.set_credentials(&mqtt.username, &mqtt.password);
    opts.set_clean_session(true);
    opts.set_request_channel_capacity(REQUEST_CAP);
    if mqtt.tls {
        let config = build_rustls_config()?;
        opts.set_transport(Transport::Tls(TlsConfiguration::Rustls(Arc::new(config))));
    } else {
        opts.set_transport(Transport::Tcp);
    }
    Ok(opts)
}

/// 构建 ring provider 的 rustls ClientConfig（webpki-roots 根证书）。
fn build_rustls_config() -> Result<ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(config)
}

/// 轮询 MQTT 直到收到 hello 响应，返回完整协商结果。
async fn wait_hello(eventloop: &mut EventLoop) -> Result<Negotiated> {
    loop {
        let ev = eventloop
            .poll()
            .await
            .map_err(|e| VoiceError::Transport(format!("MQTT 轮询失败: {}", e)))?;
        match ev {
            Event::Incoming(Incoming::Publish(publish)) => {
                let text = String::from_utf8_lossy(&publish.payload);
                debug!("MQTT 收到: {}", text);
                match serde_json::from_str::<ServerMessage>(&text) {
                    Ok(ServerMessage::Hello(h)) => {
                        // 跳过无 udp 的 hello（可能是自己发布消息的回显）。
                        let Some(udp) = h.udp else { continue };
                        return Ok(Negotiated {
                            session_id: h.session_id,
                            server_audio: h.audio_params.unwrap_or_default(),
                            udp,
                        });
                    }
                    Ok(_) => continue,
                    Err(_) => continue,
                }
            }
            Event::Incoming(Incoming::ConnAck(_)) => continue,
            _ => continue,
        }
    }
}

/// 出站：控制（MQTT publish JSON）+ 音频（UDP 加密发送）。
#[allow(clippy::too_many_arguments)]
async fn send_loop(
    client: AsyncClient,
    mqtt: MqttParams,
    mut control_rx: mpsc::Receiver<ClientMessage>,
    audio_rx: super::LatestSlot<Vec<u8>>,
    udp: Arc<UdpSocket>,
    cipher: AesCtrCipher,
    base_nonce: [u8; 16],
) {
    let mut local_sequence: u32 = 0;
    // 加密与组包缓冲常驻复用，避免每帧堆分配。
    let mut packet: Vec<u8> = Vec::with_capacity(UDP_MAX_PACKET);
    loop {
        tokio::select! {
            biased;
            ctrl = control_rx.recv() => {
                let Some(msg) = ctrl else { break };
                let payload = match serde_json::to_string(&msg) {
                    Ok(p) => p,
                    Err(e) => { warn!("控制消息序列化失败: {}", e); continue; }
                };
                if let Err(e) = client
                    .publish(&mqtt.publish_topic, QoS::AtMostOnce, false, payload)
                    .await
                {
                    warn!("MQTT 控制发布失败: {}", e);
                    break;
                }
            }
            audio = audio_rx.take() => {
                local_sequence = local_sequence.wrapping_add(1);
                let hdr = UdpAudioHeader {
                    type_: TYPE_AUDIO,
                    flags: 0,
                    payload_len: audio.len() as u16,
                    ssrc: 0,
                    timestamp: now_ms(),
                    sequence: local_sequence,
                };
                let iv = hdr.build_iv(&base_nonce);
                let mut encrypted = audio;
                cipher.apply_keystream(&iv, &mut encrypted);
                packet.clear();
                packet.extend_from_slice(&iv);
                packet.extend_from_slice(&encrypted);
                if let Err(e) = udp.send(&packet).await {
                    warn!("UDP 发送失败: {}", e);
                    // UDP 黑洞：连续失败由监督层计数熔断，这里不自行退出。
                }
            }
        }
    }
}

/// MQTT 事件循环：hello 之后的下行 JSON 路由到 incoming。
async fn mqtt_event_loop(mut eventloop: EventLoop, incoming_tx: mpsc::Sender<IncomingEvent>) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                let text = String::from_utf8_lossy(&publish.payload);
                match serde_json::from_str::<ServerMessage>(&text) {
                    Ok(msg) => {
                        if incoming_tx.send(IncomingEvent::Json(msg)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => warn!("MQTT JSON 解析失败: {} (payload={})", e, text),
                }
            }
            Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                debug!("MQTT ConnAck");
            }
            Ok(_) => {}
            Err(e) => {
                warn!("MQTT 连接中断: {}", e);
                let _ = incoming_tx.send(IncomingEvent::Closed).await;
                break;
            }
        }
    }
}

/// UDP 接收循环：校验包头 → 防重放 → 解密 → Audio。
async fn udp_recv_loop(
    udp: Arc<UdpSocket>,
    cipher: AesCtrCipher,
    incoming_tx: mpsc::Sender<IncomingEvent>,
) {
    let mut buf = vec![0u8; UDP_MAX_PACKET];
    let mut remote_sequence: u32 = 0;
    let mut stats = UdpRecvStats::new();
    loop {
        match udp.recv(&mut buf).await {
            Ok(n) => {
                stats.received += 1;
                if n < HEADER_SIZE {
                    stats.short += 1;
                    continue;
                }
                let iv: [u8; HEADER_SIZE] = buf[..HEADER_SIZE].try_into().unwrap();
                let hdr = UdpAudioHeader::parse(&iv);
                // 类型校验。
                if hdr.type_ != TYPE_AUDIO {
                    stats.type_dropped += 1;
                    continue;
                }
                // 长度校验。
                if HEADER_SIZE + hdr.payload_len as usize != n {
                    stats.len_dropped += 1;
                    debug!("UDP 长度不匹配: 实际 {} 声明 {}", n, hdr.payload_len);
                    continue;
                }
                // 防重放：序列号必须单调递增（允许跳跃，丢弃过期包）。
                if hdr.sequence <= remote_sequence {
                    stats.seq_dropped += 1;
                    debug!("UDP 序列号过期/重放: {} <= {}", hdr.sequence, remote_sequence);
                    continue;
                }
                if hdr.sequence != remote_sequence.wrapping_add(1) {
                    warn!("UDP 序列号跳跃: 期望 {} 得 {}", remote_sequence + 1, hdr.sequence);
                }
                remote_sequence = hdr.sequence;

                let mut payload = buf[HEADER_SIZE..n].to_vec();
                cipher.apply_keystream(&iv, &mut payload);
                stats.ok += 1;
                if incoming_tx.send(IncomingEvent::Audio(payload)).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                warn!("UDP 接收错误: {}", e);
                let _ = incoming_tx.send(IncomingEvent::Closed).await;
                break;
            }
        }

        if stats.flush_at.elapsed() >= Duration::from_secs(2) {
            debug!(
                "UDP 下行诊断: 收包={}, 过短={}, type丢弃={}, 长度丢弃={}, 序列号丢弃={}, 解密成功={}",
                stats.received,
                stats.short,
                stats.type_dropped,
                stats.len_dropped,
                stats.seq_dropped,
                stats.ok
            );
            stats = UdpRecvStats::new();
        }
    }
}

/// UDP 接收统计（诊断用）。
struct UdpRecvStats {
    flush_at: Instant,
    received: u64,
    short: u64,
    type_dropped: u64,
    len_dropped: u64,
    seq_dropped: u64,
    ok: u64,
}

impl UdpRecvStats {
    fn new() -> Self {
        Self {
            flush_at: Instant::now(),
            received: 0,
            short: 0,
            type_dropped: 0,
            len_dropped: 0,
            seq_dropped: 0,
            ok: 0,
        }
    }
}
