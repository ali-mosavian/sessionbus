use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, Read},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::Deserialize;
use serde_json::{Value, json};
use ssh_key::{PublicKey, SshSig};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, WriteHalf},
    net::TcpListener,
    sync::mpsc,
    time::timeout,
};

use crate::db::{self, BusErr, Db, Msg};

pub const NAMESPACE: &str = "sessionbus";
const MAX_BODY: usize = 1 << 20;
const BATCH: i64 = 500;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Req {
    Hello {
        name: String,
        sig: String,
        #[serde(default)]
        listen: bool,
    },
    List,
    Send {
        to: String,
        body: String,
    },
    Sub {
        topic: String,
    },
    Unsub {
        topic: String,
    },
    Topics,
    Ack {
        id: i64,
    },
}

pub struct Hub {
    db: Db,
    keys: PathBuf,
    conns: Mutex<HashMap<String, Vec<(u64, mpsc::UnboundedSender<()>)>>>,
    next: AtomicU64,
}

/// What a client signs to prove it holds the key registered for `name`.
pub fn signed_payload(
    nonce: &str,
    name: &str,
) -> Vec<u8> {
    format!("sessionbus-v1:{nonce}:{name}").into_bytes()
}

impl Hub {
    pub fn new(
        db: Db,
        authorized_keys: PathBuf,
    ) -> Arc<Hub> {
        Arc::new(Hub { db, keys: authorized_keys, conns: Mutex::default(), next: AtomicU64::new(1) })
    }

    fn conns(&self) -> std::sync::MutexGuard<'_, HashMap<String, Vec<(u64, mpsc::UnboundedSender<()>)>>> {
        self.conns.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn notify(
        &self,
        name: &str,
    ) {
        for (_, tx) in self.conns().get(name).into_iter().flatten() {
            let _ = tx.send(());
        }
    }

    fn online(
        &self,
        name: &str,
    ) -> bool {
        self.conns().get(name).is_some_and(|v| !v.is_empty())
    }

    /// Re-read per hello so keys can be added without restarting the relay.
    fn verify(
        &self,
        name: &str,
        nonce: &str,
        sig_pem: &str,
    ) -> bool {
        let Ok(sig) = SshSig::from_pem(sig_pem) else { return false };
        let Ok(text) = std::fs::read_to_string(&self.keys) else { return false };
        let msg = signed_payload(nonce, name);
        text.lines()
            .filter_map(|l| l.trim().split_once(char::is_whitespace))
            .filter(|(n, _)| *n == name)
            .filter_map(|(_, k)| PublicKey::from_openssh(k.trim()).ok())
            .any(|k| k.verify(NAMESPACE, &msg, &sig).is_ok())
    }
}

fn nonce() -> io::Result<String> {
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

fn valid_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && !n.starts_with('#') && !n.contains(char::is_whitespace)
}

fn err(e: impl Display) -> Value {
    json!({"op": "error", "error": e.to_string()})
}

async fn put<W: AsyncWrite + Unpin>(
    w: &mut W,
    v: &Value,
) -> io::Result<()> {
    let mut s = v.to_string();
    s.push('\n');
    w.write_all(s.as_bytes()).await?;
    w.flush().await
}

pub async fn run(
    listener: TcpListener,
    hub: Arc<Hub>,
) {
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                tokio::spawn(serve_conn(hub.clone(), sock));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

pub async fn serve_conn<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    hub: Arc<Hub>,
    s: S,
) {
    let _ = handle(&hub, s).await;
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    hub: &Arc<Hub>,
    s: S,
) -> io::Result<()> {
    let (rd, mut wr) = tokio::io::split(s);
    let mut br = BufReader::new(rd);
    let nonce = nonce()?;
    put(&mut wr, &json!({"op": "challenge", "nonce": nonce})).await?;

    // Unauthenticated peers get a small, time-boxed first line.
    let mut first = String::new();
    let n = timeout(Duration::from_secs(10), (&mut br).take(8192).read_line(&mut first))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    if n == 0 {
        return Ok(());
    }
    let (name, listen) = match serde_json::from_str::<Req>(&first) {
        Ok(Req::Hello { name, sig, listen }) if valid_name(&name) && hub.verify(&name, &nonce, &sig) => (name, listen),
        _ => return put(&mut wr, &err("auth failed")).await,
    };

    let id = hub.next.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::unbounded_channel::<()>();
    if listen {
        hub.conns().entry(name.clone()).or_default().push((id, tx));
    }
    let res = session(hub, &name, listen, br, &mut wr, &mut rx).await;
    if let Some(v) = hub.conns().get_mut(&name) {
        v.retain(|(i, _)| *i != id);
    }
    let n = name.clone();
    let _ = hub.db.run(move |c| db::touch(c, &n)).await;
    res
}

async fn session<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    hub: &Arc<Hub>,
    name: &str,
    listen: bool,
    br: BufReader<R>,
    wr: &mut WriteHalf<W>,
    rx: &mut mpsc::UnboundedReceiver<()>,
) -> io::Result<()> {
    let n = name.to_string();
    if let Err(e) = hub.db.run(move |c| db::touch(c, &n)).await {
        return put(wr, &err(e)).await;
    }
    put(wr, &json!({"op": "ok"})).await?;

    let mut lines = br.lines();
    let mut cursor = 0;
    if listen {
        cursor = flush(hub, name, cursor, wr).await?;
        put(wr, &json!({"op": "ready"})).await?;
    }
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { return Ok(()) };
                let reply = match serde_json::from_str::<Req>(&line) {
                    Ok(r) => request(hub, name, r).await,
                    Err(e) => Some(err(e)),
                };
                if let Some(v) = reply {
                    put(wr, &v).await?;
                }
            }
            Some(_) = rx.recv(), if listen => cursor = flush(hub, name, cursor, wr).await?,
        }
    }
}

/// Sends every queued message after `cursor`; messages stay queued until the
/// client acks.
async fn flush<W: AsyncWrite + Unpin>(
    hub: &Arc<Hub>,
    name: &str,
    mut cursor: i64,
    wr: &mut W,
) -> io::Result<i64> {
    loop {
        let n = name.to_string();
        let msgs: Vec<Msg> = match hub.db.run(move |c| db::pending(c, &n, cursor, BATCH)).await {
            Ok(m) => m,
            Err(e) => {
                put(wr, &err(e)).await?;
                return Ok(cursor);
            }
        };
        for m in &msgs {
            put(wr, &json!({"op": "msg", "id": m.id, "from": m.from, "to": m.to, "topic": m.topic, "body": m.body, "at": m.at}))
                .await?;
            cursor = m.id;
        }
        if (msgs.len() as i64) < BATCH {
            return Ok(cursor);
        }
    }
}

fn topic_name(t: &str) -> Result<String, BusErr> {
    let t = t.strip_prefix('#').unwrap_or(t);
    if t.is_empty() || t.len() > 64 || t.contains(char::is_whitespace) {
        return Err(BusErr::new("bad topic"));
    }
    Ok(t.to_string())
}

async fn request(
    hub: &Arc<Hub>,
    me: &str,
    r: Req,
) -> Option<Value> {
    let me = me.to_string();
    let out: Result<Value, BusErr> = async {
        Ok(match r {
            Req::Hello { .. } => return Err(BusErr::new("already authenticated")),
            Req::List => {
                let rows = hub.db.run(|c| db::agents(c)).await?;
                let agents: Vec<Value> = rows
                    .into_iter()
                    .map(|(n, t)| json!({"name": n, "online": hub.online(&n), "last_seen": t}))
                    .collect();
                json!({"op": "agents", "agents": agents})
            }
            Req::Topics => {
                let rows = hub.db.run(|c| db::topics(c)).await?;
                let t: Vec<Value> = rows.into_iter().map(|(t, n)| json!({"topic": t, "subscribers": n})).collect();
                json!({"op": "topics", "topics": t})
            }
            Req::Sub { topic } => {
                let t = topic_name(&topic)?;
                hub.db.run(move |c| db::subscribe(c, &t, &me)).await?;
                json!({"op": "ok"})
            }
            Req::Unsub { topic } => {
                let t = topic_name(&topic)?;
                hub.db.run(move |c| db::unsubscribe(c, &t, &me)).await?;
                json!({"op": "ok"})
            }
            Req::Send { to, body } => {
                if to.is_empty() || body.is_empty() || body.len() > MAX_BODY {
                    return Err(BusErr::new("need to and a body of 1..1MiB"));
                }
                let recips = hub.db.run(move |c| db::enqueue(c, &me, &to, &body)).await?;
                for r in &recips {
                    hub.notify(r);
                }
                json!({"op": "sent", "delivered": recips.len()})
            }
            Req::Ack { id } => {
                hub.db.run(move |c| db::ack(c, &me, id)).await?;
                return Ok(Value::Null);
            }
        })
    }
    .await;
    match out {
        Ok(Value::Null) => None,
        Ok(v) => Some(v),
        Err(e) => Some(err(e)),
    }
}
