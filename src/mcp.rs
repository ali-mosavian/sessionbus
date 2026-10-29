//! MCP server on stdio (newline-delimited JSON-RPC 2.0) exposing the relay as
//! tools. Each tool call uses its own relay connection, and requests run
//! concurrently so a long `inbox` wait never blocks a `send`.
use std::{io, sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    sync::Mutex,
    task::JoinSet,
};

use crate::client::{Client, Config};

const MAX_WAIT: u64 = 120;
const DEFAULT_VERSION: &str = "2025-06-18";
const INSTRUCTIONS: &str = "Message other Claude sessions, possibly on other machines. Addresses are session \
names; `#topic` addresses every subscriber of a topic. Incoming messages only arrive when you call `inbox`; \
pass wait_seconds to block until one arrives. Delivery is at-least-once: an unacked message can repeat, so \
dedupe by id.";

fn tools() -> Value {
    let obj = |props: Value, req: &[&str]| json!({"type": "object", "properties": props, "required": req});
    json!([
        {"name": "list_sessions", "description": "List known sessions and whether each is online.",
         "inputSchema": obj(json!({}), &[])},
        {"name": "send_message",
         "description": "Send a message to a session name, or to every subscriber of a topic with `#topic`.",
         "inputSchema": obj(json!({
             "to": {"type": "string", "description": "Session name, or #topic"},
             "message": {"type": "string"}}), &["to", "message"])},
        {"name": "read_inbox",
         "description": "Return queued messages, oldest first, and mark them read. With wait_seconds, \
    blocks until a message arrives or the time runs out.",
         "inputSchema": obj(json!({
             "wait_seconds": {"type": "integer", "minimum": 0, "maximum": MAX_WAIT}}), &[])},
        {"name": "subscribe", "description": "Subscribe this session to a topic.",
         "inputSchema": obj(json!({"topic": {"type": "string"}}), &["topic"])},
        {"name": "unsubscribe", "description": "Unsubscribe this session from a topic.",
         "inputSchema": obj(json!({"topic": {"type": "string"}}), &["topic"])},
        {"name": "list_topics", "description": "List topics and their subscriber counts.",
         "inputSchema": obj(json!({}), &[])},
    ])
}

fn text(
    s: String,
    is_error: bool,
) -> Value {
    json!({"content": [{"type": "text", "text": s}], "isError": is_error})
}

fn arg<'a>(
    args: &'a Value,
    key: &str,
) -> Result<&'a str, String> {
    args[key].as_str().ok_or_else(|| format!("missing string argument `{key}`"))
}

/// A connection and the id to ack on it, once the result has been delivered.
type Ack = Option<(Client, i64)>;

async fn call_tool(
    cfg: &Config,
    name: &str,
    args: &Value,
) -> Result<(String, Ack), String> {
    let relay = |req: Value| async move { cfg.connect(false).await?.call(req).await.map(|v| (v.to_string(), None)) };
    match name {
        "list_sessions" => relay(json!({"op": "list"})).await,
        "list_topics" => relay(json!({"op": "topics"})).await,
        "subscribe" => relay(json!({"op": "sub", "topic": arg(args, "topic")?})).await,
        "unsubscribe" => relay(json!({"op": "unsub", "topic": arg(args, "topic")?})).await,
        "send_message" => relay(json!({"op": "send", "to": arg(args, "to")?, "body": arg(args, "message")?})).await,
        "read_inbox" => {
            let wait = args["wait_seconds"].as_u64().unwrap_or(0).min(MAX_WAIT);
            let mut c = cfg.connect(true).await?;
            let msgs = c.inbox(Duration::from_secs(wait)).await?;
            let last = msgs.last().and_then(|m| m["id"].as_i64());
            let body = if msgs.is_empty() { "no messages".into() } else { Value::Array(msgs).to_string() };
            Ok((body, last.map(|id| (c, id))))
        }
        _ => Err(format!("unknown tool `{name}`")),
    }
}

async fn handle(
    cfg: &Config,
    req: &Value,
) -> (Value, Ack) {
    let params = &req["params"];
    match req["method"].as_str().unwrap_or("") {
        "initialize" => {
            let version = params["protocolVersion"].as_str().unwrap_or(DEFAULT_VERSION);
            let result = json!({
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "sessionbus", "version": env!("CARGO_PKG_VERSION")},
                "instructions": INSTRUCTIONS,
            });
            (json!({"result": result}), None)
        }
        "ping" => (json!({"result": {}}), None),
        "tools/list" => (json!({"result": {"tools": tools()}}), None),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            match call_tool(cfg, name, &params["arguments"]).await {
                Ok((body, ack)) => (json!({"result": text(body, false)}), ack),
                Err(e) => (json!({"result": text(e, true)}), None),
            }
        }
        m => (json!({"error": {"code": -32601, "message": format!("method not found: {m}")}}), None),
    }
}

/// Serves until `rd` closes, then waits for in-flight requests (and their
/// acks).
pub async fn serve<R, W>(
    cfg: Arc<Config>,
    rd: R,
    wr: W,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let wr = Arc::new(Mutex::new(wr));
    let mut lines = BufReader::new(rd).lines();
    let mut tasks = JoinSet::new();
    while let Some(line) = lines.next_line().await? {
        while tasks.try_join_next().is_some() {}
        if line.trim().is_empty() {
            continue;
        }
        let (cfg, wr) = (cfg.clone(), wr.clone());
        tasks.spawn(async move {
            let (id, reply, ack) = match serde_json::from_str::<Value>(&line) {
                Ok(req) if req.get("id").is_none() => return, // notification
                Ok(req) => {
                    let (reply, ack) = handle(&cfg, &req).await;
                    (req["id"].clone(), reply, ack)
                }
                Err(e) => (Value::Null, json!({"error": {"code": -32700, "message": e.to_string()}}), None),
            };
            let mut out = json!({"jsonrpc": "2.0", "id": id});
            out.as_object_mut().expect("object").extend(reply.as_object().expect("object").clone());
            let mut s = out.to_string();
            s.push('\n');
            {
                let mut w = wr.lock().await;
                if w.write_all(s.as_bytes()).await.is_err() || w.flush().await.is_err() {
                    return; // never delivered, so leave the messages queued
                }
            }
            if let Some((mut c, id)) = ack {
                let _ = c.ack_sync(id).await;
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}
