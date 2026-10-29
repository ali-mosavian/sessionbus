use std::{
    env,
    path::{Path, PathBuf},
    time::Duration,
};

use serde_json::{Value, json};
use ssh_key::{HashAlg, LineEnding, PrivateKey};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf},
    net::TcpStream,
    time::{Instant, timeout_at},
};

use crate::server::{NAMESPACE, signed_payload};

pub struct Client {
    rd: Lines<BufReader<ReadHalf<TcpStream>>>,
    wr: WriteHalf<TcpStream>,
    ping_every: Duration,
}

pub fn load_key(path: &Path) -> Result<PrivateKey, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let key = PrivateKey::from_openssh(text).map_err(|e| format!("{}: {e}", path.display()))?;
    if key.is_encrypted() {
        return Err(format!("{}: encrypted keys are not supported", path.display()));
    }
    Ok(key)
}

/// Connection settings from SESSIONBUS_NAME, SESSIONBUS_ADDR (default
/// 127.0.0.1:8787) and SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
pub struct Config {
    pub addr: String,
    pub name: Option<String>,
    pub key_path: PathBuf,
}

impl Config {
    pub fn from_env() -> Config {
        Config {
            addr: env::var("SESSIONBUS_ADDR").unwrap_or("127.0.0.1:8787".into()),
            name: env::var("SESSIONBUS_NAME").ok(),
            key_path: env::var("SESSIONBUS_KEY")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(env::var("HOME").unwrap_or_default()).join(".ssh/id_ed25519")),
        }
    }

    /// Fails per call, not at startup, so a misconfigured MCP server still
    /// answers `initialize` and reports the problem from the tool.
    pub async fn connect(
        &self,
        listen: bool,
    ) -> Result<Client, String> {
        let name = self.name.as_deref().ok_or("SESSIONBUS_NAME not set")?;
        Client::connect(&self.addr, name, &load_key(&self.key_path)?, listen).await
    }
}

/// How often a waiting listener tells the relay it is alive. The relay drops a
/// listener silent for 45 s.
const PING_EVERY: Duration = Duration::from_secs(15);

impl Client {
    /// Overrides the keep-alive interval.
    pub fn with_ping_every(
        mut self,
        every: Duration,
    ) -> Self {
        self.ping_every = every;
        self
    }

    /// Connects and authenticates as `name`. `listen` clients receive pushed
    /// messages.
    pub async fn connect(
        addr: &str,
        name: &str,
        key: &PrivateKey,
        listen: bool,
    ) -> Result<Client, String> {
        let s = TcpStream::connect(addr).await.map_err(|e| format!("{addr}: {e}"))?;
        let (rd, wr) = tokio::io::split(s);
        let mut c = Client { rd: BufReader::new(rd).lines(), wr, ping_every: PING_EVERY };
        let ch = c.next().await?.ok_or("closed before challenge")?;
        let nonce = ch["nonce"].as_str().ok_or("bad challenge")?;
        let sig = key
            .sign(NAMESPACE, HashAlg::Sha512, &signed_payload(nonce, name))
            .and_then(|s| s.to_pem(LineEnding::LF))
            .map_err(|e| e.to_string())?;
        c.send(&json!({"op": "hello", "name": name, "sig": sig, "listen": listen})).await?;
        match c.next().await? {
            Some(v) if v["op"] == "ok" => Ok(c),
            Some(v) => Err(v["error"].as_str().unwrap_or("rejected").to_string()),
            None => Err("auth failed".into()),
        }
    }

    pub async fn send(
        &mut self,
        v: &Value,
    ) -> Result<(), String> {
        let mut s = v.to_string();
        s.push('\n');
        self.wr.write_all(s.as_bytes()).await.map_err(|e| e.to_string())
    }

    /// Next frame, or `None` when the relay closed the connection.
    pub async fn next(&mut self) -> Result<Option<Value>, String> {
        match self.rd.next_line().await.map_err(|e| e.to_string())? {
            Some(l) => serde_json::from_str(&l).map(Some).map_err(|e| e.to_string()),
            None => Ok(None),
        }
    }

    /// One request, one reply. Not for `listen` connections, whose stream also
    /// carries pushes.
    pub async fn call(
        &mut self,
        v: Value,
    ) -> Result<Value, String> {
        self.send(&v).await?;
        let r = self.next().await?.ok_or("connection closed")?;
        if r["op"] == "error" {
            return Err(r["error"].as_str().unwrap_or("error").to_string());
        }
        Ok(r)
    }

    pub async fn ack(
        &mut self,
        id: i64,
    ) -> Result<(), String> {
        self.send(&json!({"op": "ack", "id": id})).await
    }

    /// Like `ack`, but returns once the relay has applied it: requests on one
    /// connection run in order, so the reply to a following request proves it.
    pub async fn ack_sync(
        &mut self,
        id: i64,
    ) -> Result<(), String> {
        self.ack(id).await?;
        self.send(&json!({"op": "topics"})).await?;
        loop {
            match self.next().await? {
                Some(v) if v["op"] == "topics" => return Ok(()),
                Some(v) if v["op"] == "error" => return Err(v["error"].as_str().unwrap_or("error").into()),
                Some(_) => {}
                None => return Err("connection closed".into()),
            }
        }
    }

    /// Queued messages, oldest first, from a `listen` connection. When none are
    /// queued and `wait` is non-zero, waits up to `wait` for the first one.
    /// Messages stay queued until acked.
    pub async fn inbox(
        &mut self,
        wait: Duration,
    ) -> Result<Vec<Value>, String> {
        let deadline = Instant::now() + wait;
        let (mut out, mut ready) = (Vec::new(), false);
        let mut next_ping = Instant::now() + self.ping_every;
        loop {
            let frame = if ready {
                match timeout_at(deadline.min(next_ping), self.next()).await {
                    Ok(f) => f?,
                    Err(_) if Instant::now() >= deadline => return Ok(out),
                    Err(_) => {
                        self.send(&json!({"op": "ping"})).await?;
                        next_ping = Instant::now() + self.ping_every;
                        continue;
                    }
                }
            } else {
                self.next().await?
            };
            let Some(v) = frame else { return Err("connection closed".into()) };
            match v["op"].as_str() {
                Some("msg") => {
                    out.push(v);
                    if ready {
                        return Ok(out);
                    }
                }
                Some("ready") if !out.is_empty() || wait.is_zero() => return Ok(out),
                Some("ready") => ready = true,
                Some("error") => return Err(v["error"].as_str().unwrap_or("error").into()),
                _ => {}
            }
        }
    }
}
