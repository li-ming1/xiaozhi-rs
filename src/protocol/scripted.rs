//! Scripted 传输：测试注入用。不属于运行时网络路径，仅用于监督状态机与
//! 音频管线的确定性测试（丢包/乱序/超时/断连注入）。

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use crate::error::Result;
use crate::protocol::message::{AudioParams, ClientMessage, ServerMessage};

use super::{ConnectParams, IncomingEvent, TransportHandles};

/// 脚本化测试传输。
pub struct ScriptedTransport {
    pub script: Script,
}

/// 测试脚本：建连后按序执行。
#[derive(Clone, Debug)]
pub struct Script {
    pub session_id: String,
    pub server_audio: AudioParams,
    pub steps: Vec<ScriptStep>,
}

#[derive(Clone, Debug)]
pub enum ScriptStep {
    Json(ServerMessage),
    Audio(Vec<u8>),
    /// 停止出站发送（模拟 UDP 黑洞 / 传输卡死）：等价于停顿。
    StallOutbound,
    /// 暂停一段时间。
    Pause(Duration),
    /// 立即断开。
    Close,
}

/// 记录发送侧观测值，供测试断言。
#[derive(Clone, Debug)]
pub enum SentItem {
    Control(ClientMessage),
    Audio(Vec<u8>),
}

impl ScriptedTransport {
    pub async fn connect(self, _params: &ConnectParams) -> Result<TransportHandles> {
        let script = self.script;
        let session_id = script.session_id.clone();
        let server_audio = script.server_audio.clone();

        let (control_tx, mut control_rx) = mpsc::channel::<ClientMessage>(16);
        let audio_slot = super::LatestSlot::<Vec<u8>>::new();
        let audio_rx = audio_slot.clone();
        let (incoming_tx, incoming_rx) = mpsc::channel::<IncomingEvent>(64);
        let (close_tx, mut close_rx) = mpsc::channel::<()>(1);

        let sent: Arc<Mutex<Vec<SentItem>>> = Arc::new(Mutex::new(Vec::new()));
        let sent_rec = sent.clone();

        // 脚本播放：只产出入站事件。
        tokio::spawn(async move {
            for step in script.steps {
                match step {
                    ScriptStep::Json(srv) => {
                        if incoming_tx.send(IncomingEvent::Json(srv)).await.is_err() {
                            break;
                        }
                    }
                    ScriptStep::Audio(a) => {
                        if incoming_tx.send(IncomingEvent::Audio(a)).await.is_err() {
                            break;
                        }
                    }
                    ScriptStep::Pause(d) => tokio::time::sleep(d).await,
                    ScriptStep::StallOutbound => tokio::time::sleep(Duration::from_secs(1)).await,
                    ScriptStep::Close => {
                        let _ = incoming_tx.send(IncomingEvent::Closed).await;
                        break;
                    }
                }
            }
        });

        // 出站记录：控制入日志，音频 latest-slot 入日志。
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    ctrl = control_rx.recv() => {
                        match ctrl {
                            Some(msg) => sent_rec.lock().await.push(SentItem::Control(msg)),
                            None => break,
                        }
                    }
                    audio = audio_rx.take() => {
                        sent_rec.lock().await.push(SentItem::Audio(audio));
                    }
                    _ = close_rx.recv() => break,
                }
            }
        });

        Ok(TransportHandles {
            session_id,
            server_audio,
            control_tx,
            audio_tx: audio_slot,
            incoming_rx,
            close_tx,
        })
    }

    /// 返回已记录的出站项（测试断言用）。
    pub async fn sent_items(sent: &Arc<Mutex<Vec<SentItem>>>) -> Vec<SentItem> {
        sent.lock().await.clone()
    }
}
