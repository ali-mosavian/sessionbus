use std::{path::PathBuf, sync::Arc, time::Duration};

use serde_json::{Value, json};
use sessionbus::{
    client::Config,
    db::Db,
    mcp,
    server::{Hub, run},
};
use ssh_key::{Algorithm, LineEnding, PrivateKey, rand_core::OsRng};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
};

/// A relay plus a key file per session.
struct Rig {
    dir: PathBuf,
    addr: String,
}

impl Rig {
    async fn new(
        test: &str,
        names: &[&str],
    ) -> Rig {
        let dir = std::env::temp_dir().join(format!("sessionbus-mcp-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut auth = String::new();
        for n in names {
            let k = PrivateKey::random(&mut OsRng, Algorithm::Ed25519).unwrap();
            auth += &format!("{n} {}\n", k.public_key().to_openssh().unwrap());
            std::fs::write(dir.join(format!("{n}.key")), k.to_openssh(LineEnding::LF).unwrap()).unwrap();
        }
        std::fs::write(dir.join("authorized_keys"), auth).unwrap();
        let hub = Hub::new(Db::open(&dir.join("bus.db")).unwrap(), dir.join("authorized_keys"));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        tokio::spawn(run(l, hub));
        Rig { dir, addr }
    }

    fn mcp(
        &self,
        name: &str,
    ) -> Mcp {
        let cfg =
            Config { addr: self.addr.clone(), name: Some(name.into()), key_path: self.dir.join(format!("{name}.key")) };
        let (w, sr) = tokio::io::duplex(1 << 16);
        let (sw, cr) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(async move { mcp::serve(Arc::new(cfg), sr, sw).await.unwrap() });
        Mcp { w, r: BufReader::new(cr).lines(), server, next_id: 1 }
    }
}

struct Mcp {
    w: DuplexStream,
    r: Lines<BufReader<DuplexStream>>,
    server: JoinHandle<()>,
    next_id: u64,
}

impl Mcp {
    async fn post(
        &mut self,
        method: &str,
        params: Value,
    ) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.w.write_all(format!("{req}\n").as_bytes()).await.unwrap();
        id
    }

    async fn recv(&mut self) -> Value {
        let l = timeout(Duration::from_secs(5), self.r.next_line()).await.expect("timed out").unwrap().unwrap();
        serde_json::from_str(&l).unwrap()
    }

    async fn rpc(
        &mut self,
        method: &str,
        params: Value,
    ) -> Value {
        let id = self.post(method, params).await;
        let v = self.recv().await;
        assert_eq!(v["id"], id);
        v
    }

    /// (is_error, text)
    async fn tool(
        &mut self,
        name: &str,
        args: Value,
    ) -> (bool, String) {
        let v = self.rpc("tools/call", json!({"name": name, "arguments": args})).await;
        let r = &v["result"];
        (r["isError"].as_bool().unwrap(), r["content"][0]["text"].as_str().unwrap().to_string())
    }

    /// Closes stdin and waits for in-flight calls, acks included.
    async fn finish(self) {
        drop(self.w);
        self.server.await.unwrap();
    }
}

fn messages(text: &str) -> Vec<Value> {
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn initialize_and_tool_list() {
    let rig = Rig::new("init", &["a"]).await;
    let mut m = rig.mcp("a");
    let v = m.rpc("initialize", json!({"protocolVersion": "2025-03-26"})).await;
    assert_eq!(v["result"]["protocolVersion"], "2025-03-26");
    assert!(v["result"]["capabilities"]["tools"].is_object());
    let v = m.rpc("tools/list", json!({})).await;
    let mut names: Vec<_> =
        v["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap()).collect();
    names.sort();
    assert_eq!(names, ["list_sessions", "list_topics", "read_inbox", "send_message", "subscribe", "unsubscribe"]);
    assert_eq!(m.rpc("nope", json!({})).await["error"]["code"], -32601);
}

#[tokio::test]
async fn message_is_read_once() {
    let rig = Rig::new("once", &["a", "b"]).await;
    let mut a = rig.mcp("a");
    let mut b = rig.mcp("b");
    b.tool("list_sessions", json!({})).await; // registers b
    assert!(!a.tool("send_message", json!({"to": "b", "message": "hi"})).await.0);

    let (err, text) = b.tool("read_inbox", json!({})).await;
    let got = messages(&text);
    assert!(!err);
    assert_eq!((got.len(), got[0]["body"].as_str(), got[0]["from"].as_str()), (1, Some("hi"), Some("a")));
    b.finish().await;

    let mut b = rig.mcp("b");
    assert_eq!(b.tool("read_inbox", json!({})).await, (false, "no messages".into()));
}

/// A tool call is acked only after its result is written, so a lost reply
/// leaves the message queued instead of dropping it.
#[tokio::test]
async fn message_survives_when_the_reply_is_never_read() {
    let rig = Rig::new("lost", &["a", "b"]).await;
    let mut a = rig.mcp("a");
    rig.mcp("b").tool("list_sessions", json!({})).await;
    a.tool("send_message", json!({"to": "b", "message": "keep"})).await;

    // Current-thread runtime: the server cannot run before the reader is gone.
    let mut b = rig.mcp("b");
    b.post("tools/call", json!({"name": "read_inbox", "arguments": {}})).await;
    let Mcp { w, r, server, .. } = b;
    drop(r);
    drop(w);
    server.await.unwrap();

    let mut b = rig.mcp("b");
    assert_eq!(messages(&b.tool("read_inbox", json!({})).await.1)[0]["body"], "keep");
}

/// A blocked `read_inbox` froze every later request on the same server.
#[tokio::test]
async fn waiting_inbox_does_not_block_other_calls() {
    let rig = Rig::new("conc", &["a", "b"]).await;
    let mut a = rig.mcp("a");
    let mut b = rig.mcp("b");
    let waiting = b.post("tools/call", json!({"name": "read_inbox", "arguments": {"wait_seconds": 30}})).await;
    let quick = b.post("tools/call", json!({"name": "list_sessions", "arguments": {}})).await;
    assert_eq!(b.recv().await["id"], quick);

    a.tool("send_message", json!({"to": "b", "message": "wake"})).await;
    let v = b.recv().await;
    assert_eq!(v["id"], waiting);
    assert_eq!(messages(v["result"]["content"][0]["text"].as_str().unwrap())[0]["body"], "wake");
}

#[tokio::test]
async fn topic_round_trip_and_errors() {
    let rig = Rig::new("topic", &["a", "b"]).await;
    let mut a = rig.mcp("a");
    let mut b = rig.mcp("b");
    assert!(!b.tool("subscribe", json!({"topic": "ops"})).await.0);
    let (err, text) = a.tool("send_message", json!({"to": "#ops", "message": "deploy"})).await;
    assert!(!err && text.contains("\"delivered\":1"), "{text}");
    assert_eq!(messages(&b.tool("read_inbox", json!({})).await.1)[0]["topic"], "ops");

    assert_eq!(
        a.tool("send_message", json!({"to": "nobody", "message": "x"})).await,
        (true, "unknown recipient".into())
    );
    assert_eq!(a.tool("send_message", json!({"to": "b"})).await, (true, "missing string argument `message`".into()));
}

#[tokio::test]
async fn missing_name_is_a_tool_error_not_a_crash() {
    let rig = Rig::new("noname", &["a"]).await;
    let mut cfg = Config { addr: rig.addr.clone(), name: None, key_path: rig.dir.join("a.key") };
    cfg.name = None;
    let (w, sr) = tokio::io::duplex(1 << 16);
    let (sw, cr) = tokio::io::duplex(1 << 16);
    let server = tokio::spawn(async move { mcp::serve(Arc::new(cfg), sr, sw).await.unwrap() });
    let mut m = Mcp { w, r: BufReader::new(cr).lines(), server, next_id: 1 };
    assert_eq!(m.rpc("initialize", json!({})).await["result"]["serverInfo"]["name"], "sessionbus");
    assert_eq!(m.tool("list_sessions", json!({})).await, (true, "SESSIONBUS_NAME not set".into()));
}
