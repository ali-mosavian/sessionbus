use std::{net::TcpListener as StdListener, path::PathBuf, time::Duration};

use serde_json::{Value, json};
use sessionbus::agent::{self, Config, Running, uuid7};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
    time::{Instant, sleep},
};

fn dir() -> PathBuf {
    PathBuf::from(format!("/tmp/sb-{}", &uuid7()[24..]))
}

fn free_addr() -> String {
    StdListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string()
}

struct Host {
    run: Running,
    dir: PathBuf,
    listen: String,
}

async fn host_in(
    name: &str,
    dir: PathBuf,
    listen: String,
    trust: bool,
) -> Host {
    let mut cfg = Config::new(name, dir.clone());
    cfg.listen = listen.clone();
    cfg.advertise = Some(listen.clone());
    cfg.trust_clients = trust;
    cfg.retry = Duration::from_millis(100);
    Host { run: agent::start(cfg).await.unwrap(), dir, listen }
}

async fn host(name: &str) -> Host {
    host_in(name, dir(), free_addr(), true).await
}

impl Host {
    async fn call(
        &self,
        session: &str,
        mut req: Value,
    ) -> Value {
        req["session"] = json!(session);
        let s = UnixStream::connect(&self.run.agent.cfg.sock).await.unwrap();
        let (r, mut w) = s.into_split();
        w.write_all(format!("{req}\n").as_bytes()).await.unwrap();
        let mut l = String::new();
        BufReader::new(r).read_line(&mut l).await.unwrap();
        serde_json::from_str(&l).unwrap()
    }

    async fn ok(
        &self,
        session: &str,
        req: Value,
    ) -> Value {
        let v = self.call(session, req).await;
        assert_eq!(v["ok"], true, "{v}");
        v
    }

    async fn setup(
        &self,
        session: &str,
        alias: &str,
    ) {
        self.ok(session, json!({"op": "alias", "project": "p", "alias": alias})).await;
    }

    async fn inbox(
        &self,
        session: &str,
    ) -> Vec<Value> {
        self.ok(session, json!({"op": "peek"})).await["messages"].as_array().unwrap().clone()
    }

    async fn state(
        &self,
        session: &str,
        id: &Value,
    ) -> String {
        let v = self.ok(session, json!({"op": "status", "ids": [id]})).await;
        v["messages"][0]["state"].as_str().unwrap().into()
    }

    async fn stop(self) -> (PathBuf, String) {
        drop(self.run);
        sleep(Duration::from_millis(50)).await;
        (self.dir, self.listen)
    }
}

/// Makes `a` and `b` know each other: peers by address, then a directory exchange.
async fn link(
    a: &Host,
    b: &Host,
) {
    a.run.agent.set_peer(&b.run.agent.cfg.host, &b.listen);
    b.run.agent.set_peer(&a.run.agent.cfg.host, &a.listen);
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    b.ok("x", json!({"op": "addproject", "name": "p"})).await;
    wait_until(|| async { a.ok("x", json!({"op": "list", "project": "p"})).await["hosts"].as_array().unwrap().len() == 1 }).await;
    wait_until(|| async { b.ok("x", json!({"op": "list", "project": "p"})).await["hosts"].as_array().unwrap().len() == 1 }).await;
}

async fn wait_until<F: Future<Output = bool>>(mut f: impl FnMut() -> F) {
    let end = Instant::now() + Duration::from_secs(3);
    while !f().await {
        assert!(Instant::now() < end, "condition not reached in 3s");
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn local_send_read_and_receipt() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    a.setup("s1", "alice").await;
    a.setup("s2", "bob").await;
    let sent = a.ok("s1", json!({"op": "send", "project": "p", "to": "bob", "body": "hi"})).await;
    let id = sent["sent"][0]["id"].clone();
    let got = a.ok("s2", json!({"op": "wait", "timeout_ms": 1000})).await;
    assert_eq!(got["messages"][0]["body"], "hi");
    assert_eq!(got["messages"][0]["from"], "alice");
    assert_eq!(a.state("s1", &id).await, "delivered");
    a.ok("s2", json!({"op": "commit", "ids": [id.clone()]})).await;
    assert_eq!(a.state("s1", &id).await, "read");
    assert!(a.inbox("s2").await.is_empty());
}

/// A send to an unregistered project used to be accepted; it must name the registered ones.
#[tokio::test]
async fn unknown_project_is_refused_with_registered_list() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "llrm"})).await;
    let v = a.call("s1", json!({"op": "send", "project": "typo", "to": "bob", "body": "hi"})).await;
    assert_eq!(v["error"]["code"], "unknown_project");
    assert!(v["error"]["message"].as_str().unwrap().contains("llrm"));
}

/// The peer was down when the message was sent: the sender saw "queued", and the
/// message arrived, unduplicated, once the peer came back.
#[tokio::test]
async fn message_waits_in_outbox_until_peer_returns() {
    let (a, b) = (host("A").await, host("B").await);
    link(&a, &b).await;
    a.setup("sa", "alice").await;
    b.setup("sb", "bob").await;
    wait_until(|| async { a.ok("sa", json!({"op": "list", "project": "p"})).await["sessions"].as_array().unwrap().len() == 2 }).await;
    let (bdir, baddr) = b.stop().await;
    let sent = a.ok("sa", json!({"op": "send", "project": "p", "to": "bob", "body": "late"})).await;
    let id = sent["sent"][0]["id"].clone();
    assert_eq!(sent["sent"][0]["state"], "queued");
    sleep(Duration::from_millis(300)).await;
    assert_eq!(a.state("sa", &id).await, "queued");
    let b = host_in("B", bdir, baddr, true).await;
    wait_until(|| async { a.state("sa", &id).await == "delivered" }).await;
    let m = b.inbox("sb").await;
    assert_eq!((m.len(), m[0]["body"].as_str()), (1, Some("late")));
    assert_eq!(m[0]["from"], "alice");
    b.ok("sb", json!({"op": "commit", "ids": [id.clone()]})).await;
    wait_until(|| async { a.state("sa", &id).await == "read" }).await;
}

/// Pushing an envelope again (a lost ack makes the sender retry) must neither
/// duplicate it nor bring a message that was already read back as unread.
#[tokio::test]
async fn repeated_push_is_stored_once() {
    let b = host("B").await;
    b.ok("x", json!({"op": "addproject", "name": "p"})).await;
    b.setup("sb", "bob").await;
    let env = json!({"id": uuid7(), "project": "p", "from_session": "s", "from_alias": "al", "from_host": "A",
        "to_session": "sb", "topic": null, "body": "x", "reply_to": null, "status": null, "priority": null,
        "summary": null, "ttl_s": 60});
    let s = TcpStream::connect(&b.listen).await.unwrap();
    let (r, mut w) = s.into_split();
    let mut r = BufReader::new(r);
    let mut push = async || {
        w.write_all(format!("{}\n", json!({"t": "msg", "env": env})).as_bytes()).await.unwrap();
        let mut l = String::new();
        r.read_line(&mut l).await.unwrap();
        assert!(l.contains("\"ack\""), "{l}");
    };
    push().await;
    push().await;
    assert_eq!(b.inbox("sb").await.len(), 1);
    b.ok("sb", json!({"op": "commit", "ids": [env["id"].clone()]})).await;
    push().await;
    assert!(b.inbox("sb").await.is_empty());
}

/// Old news about work in progress is worthless: a message still queued when its
/// ttl runs out is dropped and reported `expired`.
#[tokio::test]
async fn queued_message_expires_when_peer_stays_away() {
    let (a, b) = (host("A").await, host("B").await);
    link(&a, &b).await;
    a.setup("sa", "alice").await;
    b.setup("sb", "bob").await;
    wait_until(|| async { a.ok("sa", json!({"op": "list", "project": "p"})).await["sessions"].as_array().unwrap().len() == 2 }).await;
    b.stop().await;
    let q = a.ok("sa", json!({"op": "send", "project": "p", "to": "bob", "body": "never", "ttl_s": 1})).await;
    let qid = q["sent"][0]["id"].clone();
    assert_eq!(a.state("sa", &qid).await, "queued");
    sleep(Duration::from_millis(2200)).await;
    wait_until(|| async { a.state("sa", &qid).await == "expired" }).await;
}

#[tokio::test]
async fn unread_message_leaves_the_inbox_when_its_ttl_runs_out() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    a.setup("s1", "alice").await;
    a.setup("s2", "bob").await;
    a.ok("s1", json!({"op": "send", "project": "p", "to": "bob", "body": "stale", "ttl_s": 1})).await;
    assert_eq!(a.inbox("s2").await.len(), 1);
    sleep(Duration::from_millis(2100)).await;
    assert!(a.inbox("s2").await.is_empty());
}

/// Two hosts both have a "bob": a bare name must be refused, not guessed.
#[tokio::test]
async fn ambiguous_alias_is_rejected_and_host_qualified_works() {
    let (a, b) = (host("A").await, host("B").await);
    link(&a, &b).await;
    a.setup("sa", "bob").await;
    b.setup("sb", "bob").await;
    a.setup("sc", "carol").await;
    wait_until(|| async { a.ok("sc", json!({"op": "list", "project": "p"})).await["sessions"].as_array().unwrap().len() == 3 }).await;
    let v = a.call("sc", json!({"op": "send", "project": "p", "to": "bob", "body": "hi"})).await;
    assert_eq!(v["error"]["code"], "ambiguous");
    assert!(v["error"]["message"].as_str().unwrap().contains("bob@B"));
    a.ok("sc", json!({"op": "send", "project": "p", "to": "bob@B", "body": "hi"})).await;
    wait_until(|| async { b.inbox("sb").await.len() == 1 }).await;
    assert!(a.inbox("sa").await.is_empty());
}

/// The alias is turned into a session id when sending: renaming afterwards cannot redirect it.
#[tokio::test]
async fn rename_after_send_does_not_redirect() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    a.setup("s1", "alice").await;
    a.setup("s2", "bob").await;
    a.setup("s3", "carol").await;
    a.ok("s1", json!({"op": "send", "project": "p", "to": "bob", "body": "for bob"})).await;
    a.setup("s2", "dave").await;
    a.setup("s3", "bob").await;
    assert_eq!(a.inbox("s2").await.len(), 1);
    assert!(a.inbox("s3").await.is_empty());
}

#[tokio::test]
async fn topic_reaches_subscribers_on_every_host() {
    let (a, b) = (host("A").await, host("B").await);
    link(&a, &b).await;
    a.setup("sa", "alice").await;
    b.setup("sb", "bob").await;
    a.setup("sc", "carol").await;
    a.ok("sc", json!({"op": "sub", "project": "p", "topic": "news"})).await;
    b.ok("sb", json!({"op": "sub", "project": "p", "topic": "news"})).await;
    wait_until(|| async { a.ok("sa", json!({"op": "topics", "project": "p"})).await["topics"].as_array().unwrap().len() == 1
        && a.ok("sa", json!({"op": "list", "project": "p"})).await["sessions"].as_array().unwrap().len() == 3 }).await;
    wait_until(|| async {
        let v = a.ok("sa", json!({"op": "send", "project": "p", "to": "#news", "body": "all"})).await;
        v["sent"].as_array().unwrap().len() == 2
    })
    .await;
    wait_until(|| async { b.inbox("sb").await.iter().any(|m| m["topic"] == "news") }).await;
    assert!(a.inbox("sc").await.iter().any(|m| m["topic"] == "news"));
}

/// `/clear` gives the same Claude process a new session id: aliases and unread mail follow.
#[tokio::test]
async fn replaced_session_inherits_aliases_and_unread() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    a.setup("s1", "alice").await;
    a.ok("old", json!({"op": "register", "session": "old", "anchor": 4242})).await;
    a.setup("old", "bob").await;
    a.ok("s1", json!({"op": "send", "project": "p", "to": "bob", "body": "kept"})).await;
    let v = a.ok("new", json!({"op": "register", "session": "new", "anchor": 4242})).await;
    assert_eq!(v["replaced"], "old");
    assert_eq!(a.inbox("new").await[0]["body"], "kept");
    let w = a.ok("new", json!({"op": "whoami"})).await;
    assert_eq!(w["aliases"][0]["alias"], "bob");
}

/// Without a trusted `session` field the caller is found from its process tree:
/// a hook registers under the Claude process, and the MCP below it resolves to the same session.
#[tokio::test]
async fn caller_is_identified_by_process_ancestry() {
    let mut cfg = Config::new("A", dir());
    cfg.listen = free_addr();
    let a = agent::start(cfg).await.unwrap();
    let ask = |req: Value| {
        let sock = a.agent.cfg.sock.clone();
        async move {
            let s = UnixStream::connect(sock).await.unwrap();
            let (r, mut w) = s.into_split();
            w.write_all(format!("{req}\n").as_bytes()).await.unwrap();
            let mut l = String::new();
            BufReader::new(r).read_line(&mut l).await.unwrap();
            serde_json::from_str::<Value>(&l).unwrap()
        }
    };
    let r = ask(json!({"op": "register", "session": "S-from-hook", "anchor": 1})).await;
    assert_eq!(r["ok"], true);
    // the anchor claim above is ignored: it is bound to this process's parent instead
    let w = ask(json!({"op": "whoami", "session": "forged"})).await;
    assert_eq!(w["session"], "S-from-hook");
}

/// Every Stop hook starts a waiter; an older one still running would wake Claude
/// with the same messages twice. A newer exclusive wait must retire it.
#[tokio::test]
async fn newer_exclusive_wait_supersedes_older() {
    let a = host("A").await;
    a.ok("x", json!({"op": "addproject", "name": "p"})).await;
    a.setup("s1", "alice").await;
    let old = a.ok("s1", json!({"op": "wait", "exclusive": true, "timeout_ms": 5000}));
    let new = async {
        sleep(Duration::from_millis(100)).await;
        a.ok("s1", json!({"op": "wait", "exclusive": true, "timeout_ms": 300})).await
    };
    let (old, new) = tokio::join!(old, new);
    assert_eq!(old["superseded"], true);
    assert_eq!(new["superseded"], Value::Null);
}

const TAILNET: &str = r#"{"BackendState":"Running",
 "Self":{"HostName":"tinytim","DNSName":"tinytim.tailf2b15.ts.net.","TailscaleIPs":["100.70.132.20","fd7a::1"],"UserID":1,"Online":true},
 "Peer":{"a":{"HostName":"MacBook Air (3)","DNSName":"macbook-air-3.tailf2b15.ts.net.","TailscaleIPs":["100.72.106.35"],"UserID":1,"Online":false},
         "b":{"HostName":"stranger","DNSName":"stranger.other.ts.net.","TailscaleIPs":["100.1.2.3"],"UserID":2,"Online":true}}}"#;

/// Hostnames like "MacBook Air (3)" are not usable as addresses, and a device of another
/// user on a shared tailnet must not be let in.
#[test]
fn tailnet_peers_are_the_users_own_devices_by_dns_label() {
    use sessionbus::agent::tailnet;
    let st = tailnet::parse(TAILNET).unwrap();
    assert_eq!(st.me.host, "tinytim");
    assert_eq!(st.peers.iter().map(|p| p.host.as_str()).collect::<Vec<_>>(), ["macbook-air-3"]);
    let (auth, _) = tailnet::auth(&st);
    assert!(auth("100.72.106.35".parse().unwrap()));
    assert!(!auth("100.1.2.3".parse().unwrap()));
    assert!(tailnet::parse(&TAILNET.replace("Running", "Stopped")).is_err());
}
