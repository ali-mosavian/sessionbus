//! Agent-to-agent protocol: newline JSON over TCP. The sender pushes `msg`,
//! `receipt` and `dir` items; the receiver answers each with an `ack`
//! (`dir` is answered with its own record, so one connection exchanges both).
use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};

use super::{Agent, store};
use crate::db::{Res, now};

const MAX_LINE: usize = 1 << 20;

pub async fn accept(
    agent: Arc<Agent>,
    l: TcpListener,
) {
    loop {
        let Ok((s, remote)) = l.accept().await else { continue };
        if !(agent.cfg.auth)(remote.ip()) {
            continue;
        }
        tokio::spawn(serve(agent.clone(), s));
    }
}

async fn serve(
    agent: Arc<Agent>,
    s: TcpStream,
) {
    let (r, mut w) = s.into_split();
    let mut r = BufReader::new(r).take(MAX_LINE as u64);
    let mut line = String::new();
    loop {
        line.clear();
        r.set_limit(MAX_LINE as u64);
        match r.read_line(&mut line).await {
            Ok(n) if n > 0 && line.ends_with('\n') => {}
            _ => return,
        }
        let reply = match serde_json::from_str::<Value>(&line).map_err(Into::into).and_then(|m| handle(&agent, &m)) {
            Ok(v) => v,
            Err(e) => json!({"t": "err", "code": e.code, "message": e.msg}),
        };
        if w.write_all(format!("{reply}\n").as_bytes()).await.is_err() {
            return;
        }
    }
}

fn handle(
    agent: &Agent,
    m: &Value,
) -> Res<Value> {
    match m["t"].as_str().unwrap_or("") {
        "dir" => {
            agent.learn(&m["record"])?;
            Ok(json!({"t": "dir", "record": agent.record()?}))
        }
        "msg" => {
            let e: store::Envelope = serde_json::from_value(m["env"].clone())?;
            if store::deliver(&agent.db(), &e)? {
                agent.wake_inbox();
            }
            Ok(json!({"t": "ack", "id": e.id}))
        }
        "receipt" => {
            let id = m["id"].as_str().unwrap_or_default();
            store::set_sent_state(&agent.db(), id, m["state"].as_str().unwrap_or("read"))?;
            Ok(json!({"t": "ack", "id": id}))
        }
        t => Err(crate::db::BusErr::bad_request(format!("unknown type {t:?}"))),
    }
}

/// Retries queued items to their hosts until acked or expired.
pub async fn pump(agent: Arc<Agent>) {
    loop {
        let _ = timeout(agent.cfg.retry, agent.kick.notified()).await;
        let _ = store::expire(&agent.db());
        let hosts = store::hosts_with_outbox(&agent.db()).unwrap_or_default();
        for h in hosts {
            if let Some(addr) = agent.peer_addr(&h) {
                let _ = push_to(&agent, &h, &addr).await;
            }
        }
    }
}

async fn exchange(
    r: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    w: &mut tokio::net::tcp::OwnedWriteHalf,
    m: Value,
) -> Option<Value> {
    w.write_all(format!("{m}\n").as_bytes()).await.ok()?;
    let mut line = String::new();
    timeout(Duration::from_secs(5), r.read_line(&mut line)).await.ok()?.ok().filter(|n| *n > 0)?;
    serde_json::from_str(&line).ok()
}

async fn push_to(
    agent: &Agent,
    host: &str,
    addr: &str,
) -> Option<()> {
    let s = timeout(Duration::from_secs(2), TcpStream::connect(addr)).await.ok()?.ok()?;
    let (r, mut w) = s.into_split();
    let mut r = BufReader::new(r);
    let mine = agent.record().ok()?;
    let theirs = exchange(&mut r, &mut w, json!({"t": "dir", "record": mine})).await?;
    agent.learn(&theirs["record"]).ok()?;
    loop {
        let items = store::due_outbox(&agent.db(), host, 50).ok()?;
        if items.is_empty() {
            return Some(());
        }
        for it in items {
            let m = match it.kind.as_str() {
                "msg" => {
                    let mut e: Value = serde_json::from_str(&it.payload).ok()?;
                    e["ttl_s"] = json!((it.expires - now()).max(1));
                    json!({"t": "msg", "env": e})
                }
                "receipt" => {
                    let mut m: Value = serde_json::from_str(&it.payload).ok()?;
                    m["t"] = json!("receipt");
                    m
                }
                _ => json!({"t": "dir", "record": serde_json::from_str::<Value>(&it.payload).ok()?}),
            };
            let reply = exchange(&mut r, &mut w, m).await?;
            if reply["t"] == "err" && reply["code"] != "bad_request" {
                return None;
            }
            // a request the receiver can never accept would block the queue for ever
            store::outbox_done(&agent.db(), host, &it.id, &it.kind, &it.payload).ok()?;
        }
        sleep(Duration::from_millis(1)).await;
    }
}
