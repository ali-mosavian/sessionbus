use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use serde_json::{Value, json};
use sessionbus::{
    client::Client,
    db::{self, Db, Meta},
    server::{Hub, Settings, run, serve_conn},
};
use ssh_key::{Algorithm, PrivateKey, rand_core::OsRng};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

/// Effectively no send budget.
const UNLIMITED: (f64, f64) = (1e9, 1e9);

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
        (burst, per_sec): (f64, f64),
    ) -> (String, Db, Arc<Hub>) {
        self.start_hub_with(timer, Settings { limit: (burst, per_sec), ..Settings::default() }).await
    }

    async fn start_hub_with(
        &self,
        timer: bool,
        settings: Settings,
    ) -> (String, Db, Arc<Hub>) {
        let db = Db::open(&self.dir.join("bus.db")).unwrap();
        let hub = Hub::with_settings(db.clone(), self.dir.join("authorized_keys"), settings);
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
    let (addr, db, hub) = rig.start_hub(false, UNLIMITED).await;
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
    let (addr, db, _hub) = rig.start_hub(true, UNLIMITED).await;
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
    let (addr, db, hub) = rig.start_hub(false, UNLIMITED).await;
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
    let (addr, db, _hub) = rig.start_hub(false, UNLIMITED).await;
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

async fn state(
    c: &mut Client,
    id: i64,
) -> String {
    let r = c.call(json!({"op": "status", "ids": [id]})).await.unwrap();
    r["messages"][0]["state"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn sender_can_follow_a_message_from_queued_to_read() {
    let rig = Rig::new("status", &["a", "b"]);
    let (addr, _db, hub) = rig.start_hub(false, UNLIMITED).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    let r = send(&mut a, "b", "x").await;
    let id = r["ids"][0].as_i64().unwrap();
    assert_eq!((r["recipients"][0]["name"].as_str(), r["recipients"][0]["online"].as_bool()), (Some("b"), Some(false)));
    assert_eq!(state(&mut a, id).await, "queued");

    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(msg(&mut b).await["id"], id);
    assert_eq!(state(&mut a, id).await, "delivered");
    assert_eq!(send(&mut a, "b", "y").await["recipients"][0]["online"], true);

    b.ack_sync(id).await.unwrap();
    assert_eq!(state(&mut a, id).await, "read", "acked but not yet written");
    hub.apply_acks().await;
    assert_eq!(state(&mut a, id).await, "read", "after the write");

    let mut other = rig.conn(&addr, "b", false).await;
    assert_eq!(state(&mut other, id).await, "unknown", "only the sender may ask");
    assert_eq!(state(&mut a, 9999).await, "unknown");
}

#[tokio::test]
async fn labels_reach_the_recipient_and_default_to_normal() {
    let rig = Rig::new("labels", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    let mut b = rig.conn(&addr, "b", true).await;
    let mut a = rig.conn(&addr, "a", false).await;
    a.call(json!({"op": "send", "to": "b", "body": "stop", "reply_to": 7, "status": "blocked",
                  "priority": "urgent", "summary": "halt now"}))
        .await
        .unwrap();
    send(&mut a, "b", "plain").await;
    let m = msg(&mut b).await;
    assert_eq!(
        (&m["reply_to"], &m["status"], &m["priority"], &m["summary"]),
        (&json!(7), &json!("blocked"), &json!("urgent"), &json!("halt now"))
    );
    let m = msg(&mut b).await;
    assert_eq!(
        (&m["reply_to"], &m["status"], &m["priority"], &m["summary"]),
        (&json!(null), &json!(null), &json!("normal"), &json!(null))
    );
}

#[tokio::test]
async fn malformed_labels_are_refused_and_nothing_is_queued() {
    let rig = Rig::new("badlabels", &["a", "b"]);
    let (addr, db, _hub) = rig.start_hub(false, UNLIMITED).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    for bad in [
        json!({"status": "approved"}),
        json!({"priority": "critical"}),
        json!({"summary": "two\nlines"}),
        json!({"reply_to": 0}),
    ] {
        let mut req = json!({"op": "send", "to": "b", "body": "x"});
        req.as_object_mut().unwrap().extend(bad.as_object().unwrap().clone());
        assert!(a.call(req.clone()).await.is_err(), "{req} was accepted");
    }
    assert_eq!(queued(&db, "b").await, 0);
}

/// Over budget must say so; a silent drop would leave the sender believing the
/// message went out.
#[tokio::test]
async fn sends_over_budget_are_refused_loudly_and_accepted_ones_arrive() {
    let rig = Rig::new("rate", &["a", "b"]);
    let (addr, db, _hub) = rig.start_hub(false, (3.0, 0.001)).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    for i in 0..3 {
        send(&mut a, "b", &format!("m{i}")).await;
    }
    let e = a.call(json!({"op": "send", "to": "b", "body": "m3"})).await.unwrap_err();
    assert!(e.starts_with("rate limited") && e.contains("retry after"), "{e}");
    assert_eq!(queued(&db, "b").await, 3, "an accepted message was dropped or a refused one queued");
}

/// A looping sender must not be able to flood by marking everything urgent.
#[tokio::test]
async fn urgent_has_its_own_small_budget() {
    let rig = Rig::new("urgent", &["a", "b"]);
    let (addr, _db, _hub) = rig.start_hub(false, (1.0, 0.001)).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    send(&mut a, "b", "uses the normal budget").await;
    assert!(a.call(json!({"op": "send", "to": "b", "body": "n"})).await.is_err());

    let urgent = || json!({"op": "send", "to": "b", "body": "!", "priority": "urgent"});
    let mut accepted = 0;
    let mut refused = None;
    for _ in 0..50 {
        match a.call(urgent()).await {
            Ok(_) => accepted += 1,
            Err(e) => {
                refused = Some(e);
                break;
            }
        }
    }
    assert!((10..=11).contains(&accepted), "urgent budget is not its own burst of 10: {accepted} accepted");
    assert!(refused.unwrap().contains("urgent sends"));
}

/// A key listed under several names let its holder sign in as any of them.
#[tokio::test]
async fn a_key_shared_by_two_names_is_refused_for_both() {
    let rig = Rig::new("sharedkey", &["a", "b"]);
    let (addr, _s) = rig.start().await;
    let auth = rig.dir.join("authorized_keys");
    let original = std::fs::read_to_string(&auth).unwrap();
    let a_key = original.lines().find(|l| l.starts_with("a ")).unwrap().split_once(' ').unwrap().1.to_string();
    std::fs::write(&auth, format!("{original}evil {a_key}\n")).unwrap();

    assert!(Client::connect(&addr, "evil", &rig.keys["a"], false).await.is_err());
    assert!(
        Client::connect(&addr, "a", &rig.keys["a"], false).await.is_err(),
        "the original owner must not stay valid"
    );
    assert!(Client::connect(&addr, "b", &rig.keys["b"], false).await.is_ok(), "unrelated names are unaffected");

    std::fs::write(&auth, original).unwrap();
    assert!(Client::connect(&addr, "a", &rig.keys["a"], false).await.is_ok());
}

/// Databases from before delivery tracking lack the new columns.
#[tokio::test]
async fn an_old_database_is_upgraded_in_place() {
    let dir = std::env::temp_dir().join(format!("sessionbus-{}-migrate", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("old.db");
    {
        let c = rusqlite::Connection::open(&path).unwrap();
        c.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE agents(name TEXT PRIMARY KEY, last_seen INTEGER NOT NULL);
             CREATE TABLE subs(topic TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY(topic, name));
             CREATE TABLE messages(id INTEGER PRIMARY KEY AUTOINCREMENT, from_name TEXT NOT NULL,
                 to_name TEXT NOT NULL, topic TEXT, body TEXT NOT NULL, at INTEGER NOT NULL);
             CREATE INDEX messages_to ON messages(to_name, id);
             INSERT INTO messages(from_name, to_name, body, at) VALUES('a', 'b', 'old', 1);",
        )
        .unwrap();
    }
    let db = Db::open(&path).unwrap();
    let got = db.read(|c| db::pending(c, "b", 0, 10)).await.unwrap();
    assert_eq!((got.len(), got[0].body.as_str(), got[0].priority.as_str()), (1, "old", "normal"));
    db.run(|c| db::ack(c, "b", 1)).await.unwrap();
    assert_eq!(queued(&db, "b").await, 0);
}

/// Read messages are kept (empty) for senders to check on, then dropped.
#[tokio::test]
async fn read_messages_are_kept_empty_then_purged() {
    let dir = std::env::temp_dir().join(format!("sessionbus-{}-purge", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Db::open(&dir.join("bus.db")).unwrap();
    db.run(|c| db::touch(c, "b")).await.unwrap();
    let sent = db.run(|c| db::enqueue(c, "a", "b", "secret", &Meta::default(), &[])).await.unwrap();
    let id = sent[0].1;
    db.run(move |c| db::ack(c, "b", id)).await.unwrap();

    let known = || db.read(move |c| db::statuses(c, "a", &[id]));
    assert_eq!(known().await.unwrap().len(), 1, "a read message vanished before the purge");
    let body: String =
        db.read(move |c| Ok(c.query_row("SELECT body FROM messages WHERE id=?1", [id], |r| r.get(0))?)).await.unwrap();
    assert_eq!(body, "", "a read message kept its content");
    assert_eq!(db.run(|c| db::purge_read(c, 0)).await.unwrap(), 0, "purged something read after the cutoff");
    assert_eq!(db.run(|c| db::purge_read(c, db::now() + 1)).await.unwrap(), 1);
    assert!(known().await.unwrap().is_empty());
}

/// Clients matched error text ("unknown recipient"); frames now carry a code
/// to match instead.
#[tokio::test]
async fn error_frames_carry_a_machine_readable_code() {
    let rig = Rig::new("codes", &["a"]);
    let (addr, _db, _hub) = rig.start_hub(false, (1.0, 0.001)).await;
    let mut a = rig.conn(&addr, "a", false).await;
    let mut reply = async |req: Value| {
        a.send(&req).await.unwrap();
        a.next().await.unwrap().unwrap()
    };
    let nobody = reply(json!({"op": "send", "to": "nobody", "body": "x"})).await;
    assert_eq!((&nobody["op"], &nobody["code"]), (&json!("error"), &json!("unknown_recipient")));
    let limited = reply(json!({"op": "send", "to": "nobody", "body": "x"})).await;
    assert_eq!(limited["code"], "rate_limited");
    assert!(limited["retry_after_ms"].as_u64().unwrap() > 0);
    assert_eq!(reply(json!({"op": "send", "to": "a", "body": "x", "status": "approved"})).await["code"], "bad_request");
    assert_eq!(reply(json!({"op": "sub", "topic": "two words"})).await["code"], "bad_request");
    assert_eq!(reply(json!({"op": "nonsense"})).await["code"], "bad_request");

    let mut raw = BufReader::new(TcpStream::connect(&addr).await.unwrap());
    let mut line = String::new();
    raw.read_line(&mut line).await.unwrap(); // the challenge
    raw.get_mut().write_all(b"{\"op\":\"hello\",\"name\":\"a\",\"sig\":\"junk\"}\n").await.unwrap();
    line.clear();
    raw.read_line(&mut line).await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["code"], "auth");
}

/// `delivered` outlived the listener it was pushed to: a sender kept reading
/// "delivered" for a message nobody could receive any more.
#[tokio::test]
async fn delivered_reverts_to_queued_when_the_listener_drops() {
    let rig = Rig::new("undeliver", &["a", "b"]);
    let (addr, _db, _hub) = rig.start_hub(false, UNLIMITED).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    let id = send(&mut a, "b", "x").await["ids"][0].as_i64().unwrap();

    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(msg(&mut b).await["id"], id);
    assert_eq!(state(&mut a, id).await, "delivered");
    drop(b);
    for _ in 0..40 {
        if state(&mut a, id).await == "queued" {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("still {} after the listener dropped", state(&mut a, id).await);
}

fn quick_idle() -> Settings {
    Settings { limit: UNLIMITED, idle: Duration::from_millis(300) }
}

/// A listener whose peer vanished (cable pulled, machine asleep) held its
/// connection, and so its name, until TCP gave up, which can take hours.
#[tokio::test]
async fn a_silent_listener_is_dropped() {
    let rig = Rig::new("silent", &["b"]);
    let (addr, _db, _hub) = rig.start_hub_with(false, quick_idle()).await;
    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(b.next().await.unwrap().unwrap()["op"], "ready");
    let closed = timeout(Duration::from_secs(3), b.next()).await.expect("still open after 3s");
    assert!(closed.unwrap().is_none(), "expected the relay to close the connection");
}

#[tokio::test]
async fn a_pinging_listener_is_kept() {
    let rig = Rig::new("pinging", &["a", "b"]);
    let (addr, _db, _hub) = rig.start_hub_with(false, quick_idle()).await;
    drop(rig.conn(&addr, "b", false).await);
    let mut a = rig.conn(&addr, "a", false).await;
    let mut b = Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap();
    assert_eq!(b.next().await.unwrap().unwrap()["op"], "ready");
    for _ in 0..20 {
        b.send(&json!({"op": "ping"})).await.unwrap();
        assert_eq!(b.next().await.unwrap().expect("dropped while pinging")["op"], "pong");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    send(&mut a, "b", "still there").await;
    assert_eq!(msg(&mut b).await["body"], "still there");
}

/// `sessionbus inbox --wait` sat silent past the relay's idle limit and was
/// dropped. The client must ping on its own while it waits.
#[tokio::test]
async fn a_waiting_inbox_pings_by_itself() {
    let rig = Rig::new("waiting", &["b"]);
    let (addr, _db, _hub) = rig.start_hub_with(false, quick_idle()).await;
    let mut b =
        Client::connect(&addr, "b", &rig.keys["b"], true).await.unwrap().with_ping_every(Duration::from_millis(80));
    let got = b.inbox(Duration::from_millis(1200)).await;
    assert_eq!(got.expect("connection was dropped while waiting").len(), 0);
}
