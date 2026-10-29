use std::path::Path;

use serde_json::{Value, json};
use ssh_key::{HashAlg, LineEnding, PrivateKey};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf},
    net::TcpStream,
};

use crate::server::{NAMESPACE, signed_payload};

pub struct Client {
    rd: Lines<BufReader<ReadHalf<TcpStream>>>,
    wr: WriteHalf<TcpStream>,
}

pub fn load_key(path: &Path) -> Result<PrivateKey, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let key = PrivateKey::from_openssh(text).map_err(|e| format!("{}: {e}", path.display()))?;
    if key.is_encrypted() {
        return Err(format!("{}: encrypted keys are not supported", path.display()));
    }
    Ok(key)
}

impl Client {
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
        let mut c = Client { rd: BufReader::new(rd).lines(), wr };
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
}
