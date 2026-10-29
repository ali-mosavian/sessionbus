use serde_json::{Value, json};
use sessionbus::{
    client::Client,
    db::Db,
    server::{Hub, run},
};
use ssh_key::{Algorithm, PrivateKey, rand_core::OsRng};
use std::{collections::HashMap, path::PathBuf, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

struct Rig {
    dir: PathBuf,
    keys: HashMap<String, PrivateKey>,
}

impl Rig {
    fn new(test: &str, names: &[&str]) -> Rig {
        let dir = std::env::temp_dir().join(format!("sessionbus-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let keys: HashMap<_, _> = names
            .iter()
            .map(|n| (n.to_string(), PrivateKey::random(&mut OsRng, Algorithm::Ed25519).unwrap()))
            .collect();
        let auth: String = keys
            .iter()
            .map(|(n, k)| format!("{n} {}\n", k.public_key().to_openssh().unwrap()))
            .collect();
        std::fs::write(dir.join("authorized_keys"), auth).unwrap();
        Rig { dir, keys }
    }

    /// Starts a relay on a fresh port over the same db and key files.
    async fn start(&self) -> (String, JoinHandle<()>) {
        let db = Db::open(&self.dir.join("bus.db")).unwrap();
        let hub = Hub::new(db, self.dir.join("authorized_keys"));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        (addr, tokio::spawn(run(l, hub)))
    }

    async fn conn(&self, addr: &str, name: &str, listen: bool) -> Client {
        let mut c = Client::connect(addr, name, &self.keys[name], listen).await.unwrap();
        if listen {
            assert_eq!(c.next().await.unwrap().unwrap()["op"], "ready");
        }
        c
    }
}

async fn msg(c: &mut Client) -> Value {
    let v = timeout(Duration::from_secs(5), c.next()).await.expect("timed out").unwrap().unwrap();
    assert_eq!(v["op"], "msg", "{v}");
    v
}

async fn send(c: &mut Client, to: &str, body: &str) -> Value {
    c.call(json!({"op": "send", "to": to, "body": body})).await.unwrap()
}

#[tokio::test]
async fn direct_messages_arrive_in_order() {
    let rig = Rig::new("order", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    let mut b = rig.conn(&addr, "b", true).await;
    let mut a = rig.conn(&addr, "a", false).await;
    for i in 0..3 {
        send(&mut a, "b", &format!("m{i}")).await;
    }
    for i in 0..3 {
        let m = msg(&mut b).await;
        assert_eq!((m["body"].as_str().unwrap(), m["from"].as_str().unwrap()), (&*format!("m{i}"), "a"));
    }
}

/// Drain-on-read lost messages whenever the connection dropped after the relay wrote them.
#[tokio::test]
async fn unacked_message_is_redelivered_after_reconnect() {
    let rig = Rig::new("redeliver", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    let mut a = rig.conn(&addr, "a", false).await;
    let b0 = rig.conn(&addr, "b", false).await;
    drop(b0);
    send(&mut a, "b", "hello").await;

    drop(rig.conn(&addr, "b", false).await);
    let mut b1 = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let first = msg(&mut b1).await;
    drop(b1); // never acked

    let mut b2 = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let again = msg(&mut b2).await;
    assert_eq!(first["id"], again["id"]);
    assert_eq!(again["body"], "hello");
}

#[tokio::test]
async fn acked_message_is_not_redelivered() {
    let rig = Rig::new("acked", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "once").await;

    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let m = msg(&mut b).await;
    b.ack(m["id"].as_i64().unwrap()).await.unwrap();
    // The ack is fire-and-forget; a round trip on another connection orders it before the reconnect.
    let mut b_ctl = rig.conn(&addr, "b", false).await;
    b_ctl.call(json!({"op": "list"})).await.unwrap();
    drop(b);

    let mut b2 = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(b2.next().await.unwrap().unwrap()["op"], "ready");
}

#[tokio::test]
async fn topic_reaches_subscribers_only_and_not_the_sender() {
    let rig = Rig::new("topic", &["a", "b", "c", "d"]);
    let (addr, _s) = rig.start().await;
    let mut b = rig.conn(&addr, "b", true).await;
    let mut c = rig.conn(&addr, "c", true).await;
    let mut d = rig.conn(&addr, "d", true).await;
    let mut a = rig.conn(&addr, "a", true).await;
    for n in ["a", "b", "c"] {
        let mut ctl = rig.conn(&addr, n, false).await;
        ctl.call(json!({"op": "sub", "topic": "ops"})).await.unwrap();
    }
    let mut a_ctl = rig.conn(&addr, "a", false).await;
    let r = send(&mut a_ctl, "#ops", "deploy").await;
    assert_eq!(r["delivered"], 2);
    for r in [&mut b, &mut c] {
        let m = msg(r).await;
        assert_eq!((m["body"].as_str().unwrap(), m["topic"].as_str().unwrap()), ("deploy", "ops"));
    }
    for quiet in [&mut a, &mut d] {
        assert!(timeout(Duration::from_millis(300), quiet.next()).await.is_err());
    }
}

#[tokio::test]
async fn unknown_recipient_is_an_error() {
    let rig = Rig::new("unknown", &["a"]);
    let (addr, _s) = rig.start().await;
    let mut a = rig.conn(&addr, "a", false).await;
    let e = a.call(json!({"op": "send", "to": "nobody", "body": "x"})).await.unwrap_err();
    assert_eq!(e, "unknown recipient");
}

#[tokio::test]
async fn wrong_key_or_unlisted_name_is_rejected() {
    let rig = Rig::new("auth", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    assert!(Client::connect(&addr, "a", &rig.keys["b"], false).await.is_err());
    assert!(Client::connect(&addr, "mallory", &rig.keys["a"], false).await.is_err());
}

#[tokio::test]
async fn queued_messages_survive_a_relay_restart() {
    let rig = Rig::new("restart", &["a", "b"]);
    let (addr, s) = rig.start().await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "persist me").await;
    s.abort();

    let (addr2, _s2) = rig.start().await;
    let mut b = Client::connect(&addr2, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(msg(&mut b).await["body"], "persist me");
}
