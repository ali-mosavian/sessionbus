//! The wire: newline-delimited JSON requests in, frames out.
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::db::{BusErr, Meta, Msg};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Req {
    Hello {
        name: String,
        sig: String,
        #[serde(default)]
        listen: bool,
    },
    List,
    Send(SendReq),
    Status {
        ids: Vec<i64>,
    },
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
    Topics,
    /// Keeps a listening connection from being dropped as silent.
    Ping,
    Ack {
        id: i64,
    },
}

#[derive(Deserialize)]
pub struct SendReq {
    pub to: String,
    pub body: String,
    #[serde(flatten)]
    pub meta: Meta,
}

pub fn msg_frame(m: &Msg) -> Value {
    json!({
        "op": "msg", "id": m.id, "from": m.from, "to": m.to, "topic": m.topic, "body": m.body, "at": m.at,
        "reply_to": m.reply_to, "status": m.status, "priority": m.priority, "summary": m.summary,
    })
}

pub fn err_frame(e: &BusErr) -> Value {
    let mut v = json!({"op": "error", "error": e.msg, "code": e.code});
    if let Some(ms) = e.retry_ms {
        v["retry_after_ms"] = json!(ms);
    }
    v
}

pub async fn put<W: AsyncWrite + Unpin>(
    w: &mut W,
    v: &Value,
) -> std::io::Result<()> {
    let mut s = v.to_string();
    s.push('\n');
    w.write_all(s.as_bytes()).await?;
    w.flush().await
}
