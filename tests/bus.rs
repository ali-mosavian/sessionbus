use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use serde_json::{Value, json};
use sessionbus::{
    client::Client,
    db::{self, Db},
    server::{Hub, run, serve_conn},
};
use ssh_key::{Algorithm, PrivateKey, rand_core::OsRng};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};

struct Rig {
    dir: PathBuf,
    keys: HashMap<String, PrivateKey>,
}

impl Rig {
    fn new(
        test: &str,
        names: &[&str],
    ) -> Rig {
        let dir = std::env::temp_dir().join(format!("sessionbus-{}-{test}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let keys: HashMap<_, _> = names
            .iter()
            .map(|n| (n.to_string(), PrivateKey::random(&mut OsRng, Algorithm::Ed25519).unwrap()))
            .collect();
        let auth: String =
            keys.iter().map(|(n, k)| format!("{n} {}\n", k.public_key().to_openssh().unwrap())).collect();
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

    /// Like `start`, but hands back the db and hub for white-box checks. With
    /// `timer` false nothing applies acks behind the test's back.
    async fn start_hub(
        &self,
        timer: bool,
    ) -> (String, Db, Arc<Hub>) {
        let db = Db::open(&self.dir.join("bus.db")).unwrap();
        let hub = Hub::new(db.clone(), self.dir.join("authorized_keys"));
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let h = hub.clone();
        if timer {
            tokio::spawn(run(l, h));
        } else {
            tokio::spawn(async move {
                loop {
                    let (s, _) = l.accept().await.unwrap();
                    tokio::spawn(serve_conn(h.clone(), s));
                }
            });
        }
        (addr, db, hub)
    }

    async fn conn(
        &self,
        addr: &str,
        name: &str,
        listen: bool,
    ) -> Client {
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

async fn send(
    c: &mut Client,
    to: &str,
    body: &str,
) -> Value {
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

/// Drain-on-read lost messages whenever the connection dropped after the relay
/// wrote them.
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
    // The ack is fire-and-forget; a round trip on another connection orders it
    // before the reconnect.
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

async fn queued(
    db: &Db,
    to: &str,
) -> usize {
    let to = to.to_string();
    db.run(move |c| db::pending(c, &to, 0, 1000)).await.unwrap().len()
}

/// Every ack was its own synchronous=FULL commit, so N deliveries cost N
/// fsyncs: on NVMe 763 deliveries/s and send p99 of 1.9s.
#[tokio::test]
async fn deliveries_do_not_cost_a_commit_each() {
    const N: usize = 200;
    let rig = Rig::new("ackcost", &["a", "b"]);
    let (addr, db, hub) = rig.start_hub(false).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    for i in 0..N {
        send(&mut a, "b", &format!("m{i}")).await;
    }

    let before = db.commits();
    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let mut last = 0;
    for _ in 0..N {
        last = msg(&mut b).await["id"].as_i64().unwrap();
        b.ack(last).await.unwrap();
    }
    b.ack_sync(last).await.unwrap();
    hub.apply_acks().await;
    let spent = db.commits() - before;
    assert!(spent <= 5, "{N} acked deliveries cost {spent} commits");
    assert_eq!(queued(&db, "b").await, 0);
    drop(b);

    let mut b2 = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(b2.next().await.unwrap().unwrap()["op"], "ready", "acked messages were redelivered");
}

/// Acks are held in memory, so nothing but a timer applies them when no send
/// follows; without it acked messages would sit in the db until a reconnect.
#[tokio::test]
async fn acks_are_applied_without_any_further_send() {
    let rig = Rig::new("acktimer", &["a", "b"]);
    let (addr, db, _hub) = rig.start_hub(true).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "x").await;

    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let id = msg(&mut b).await["id"].as_i64().unwrap();
    b.ack_sync(id).await.unwrap();
    for _ in 0..60 {
        if queued(&db, "b").await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("acked message still queued after 3s");
}

/// A failed send rolls back its own work, but must not take the acks that were
/// pending with it.
#[tokio::test]
async fn failed_send_keeps_pending_acks() {
    let rig = Rig::new("ackfail", &["a", "b"]);
    let (addr, db, hub) = rig.start_hub(false).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "x").await;
    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let id = msg(&mut b).await["id"].as_i64().unwrap();
    b.ack_sync(id).await.unwrap();

    let e = a.call(json!({"op": "send", "to": "nobody", "body": "x"})).await.unwrap_err();
    assert_eq!(e, "unknown recipient");
    hub.apply_acks().await;
    assert_eq!(queued(&db, "b").await, 0, "ack was lost with the failed send");
}

/// Pending acks ride along with the next send's commit instead of costing one.
#[tokio::test]
async fn successful_send_applies_pending_acks() {
    let rig = Rig::new("acksend", &["a", "b", "c"]);
    let (addr, db, _hub) = rig.start_hub(false).await;
    drop(rig.conn(&addr, "b", false).await);
    drop(rig.conn(&addr, "c", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "x").await;
    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    let id = msg(&mut b).await["id"].as_i64().unwrap();
    b.ack_sync(id).await.unwrap();

    let before = db.commits();
    send(&mut a, "c", "y").await;
    assert_eq!(queued(&db, "b").await, 0, "send did not apply the pending ack");
    assert_eq!(db.commits() - before, 1);
}
