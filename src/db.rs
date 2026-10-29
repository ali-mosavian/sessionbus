//! SQLite (WAL, synchronous=FULL) is the source of truth; a message is deleted
//! only on ack. Writes share one connection; reads use a second one, so they
//! never wait on a commit's fsync.
use std::{
    fmt,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;

#[derive(Debug)]
pub struct BusErr(pub String);

impl BusErr {
    pub fn new(s: &str) -> Self {
        BusErr(s.into())
    }
}

impl fmt::Display for BusErr {
    fn fmt(
        &self,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<E: std::error::Error> From<E> for BusErr {
    fn from(e: E) -> Self {
        BusErr(e.to_string())
    }
}

pub type Res<T> = Result<T, BusErr>;

pub fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs() as i64)
}

#[derive(Serialize, Debug, Clone)]
pub struct Msg {
    pub id: i64,
    pub from: String,
    pub to: String,
    pub topic: Option<String>,
    pub body: String,
    pub at: i64,
}

#[derive(Clone)]
pub struct Db {
    w: Arc<Mutex<Connection>>,
    r: Arc<Mutex<Connection>>,
    commits: Arc<AtomicU64>,
}

impl Db {
    pub fn open(path: &Path) -> Res<Db> {
        let mut c = Connection::open(path)?;
        c.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
             CREATE TABLE IF NOT EXISTS agents(name TEXT PRIMARY KEY, last_seen INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS subs(topic TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY(topic, name));
             CREATE TABLE IF NOT EXISTS messages(
                 id INTEGER PRIMARY KEY AUTOINCREMENT, from_name TEXT NOT NULL, to_name TEXT NOT NULL,
                 topic TEXT, body TEXT NOT NULL, at INTEGER NOT NULL);
             CREATE INDEX IF NOT EXISTS messages_to ON messages(to_name, id);",
        )?;
        let commits = Arc::new(AtomicU64::new(0));
        let n = commits.clone();
        c.commit_hook(Some(move || {
            n.fetch_add(1, Ordering::Relaxed);
            false
        }))?;
        let r = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        r.execute_batch("PRAGMA busy_timeout=5000;")?;
        Ok(Db { w: Arc::new(Mutex::new(c)), r: Arc::new(Mutex::new(r)), commits })
    }

    /// Commits so far; under synchronous=FULL each one is an fsync.
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }

    /// Runs a write on the blocking pool so fsyncs never stall the async
    /// reactor.
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Res<T> + Send + 'static,
    ) -> Res<T> {
        let db = self.w.clone();
        tokio::task::spawn_blocking(move || f(&mut db.lock().unwrap_or_else(|p| p.into_inner()))).await?
    }

    /// Runs a read on the read-only connection: it does not wait for writers.
    pub async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> Res<T> + Send + 'static,
    ) -> Res<T> {
        let db = self.r.clone();
        tokio::task::spawn_blocking(move || f(&db.lock().unwrap_or_else(|p| p.into_inner()))).await?
    }
}

pub fn touch(
    c: &Connection,
    name: &str,
) -> Res<()> {
    c.execute(
        "INSERT INTO agents(name, last_seen) VALUES(?1, ?2) ON CONFLICT(name) DO UPDATE SET last_seen=?2",
        params![name, now()],
    )?;
    Ok(())
}

pub fn agents(c: &Connection) -> Res<Vec<(String, i64)>> {
    let mut s = c.prepare("SELECT name, last_seen FROM agents ORDER BY name")?;
    Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?)
}

pub fn subscribe(
    c: &Connection,
    topic: &str,
    name: &str,
) -> Res<()> {
    c.execute("INSERT OR IGNORE INTO subs(topic, name) VALUES(?1, ?2)", params![topic, name])?;
    Ok(())
}

pub fn unsubscribe(
    c: &Connection,
    topic: &str,
    name: &str,
) -> Res<()> {
    c.execute("DELETE FROM subs WHERE topic=?1 AND name=?2", params![topic, name])?;
    Ok(())
}

pub fn topics(c: &Connection) -> Res<Vec<(String, i64)>> {
    let mut s = c.prepare("SELECT topic, COUNT(*) FROM subs GROUP BY topic ORDER BY topic")?;
    Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?)
}

/// Applies `acks` (name, highest acked id) and then queues `body` for
/// `target`, an agent name or `#topic` for every subscriber except the sender,
/// in one commit. The outer error means nothing was committed; an inner error
/// is a refused send that still committed the acks.
pub fn enqueue(
    c: &mut Connection,
    from: &str,
    target: &str,
    body: &str,
    acks: &[(String, i64)],
) -> Res<Res<Vec<String>>> {
    let mut tx = c.transaction()?;
    for (name, upto) in acks {
        ack(&tx, name, *upto)?;
    }
    let sent = {
        let sp = tx.savepoint()?;
        match queue(&sp, from, target, body) {
            Ok(recips) => {
                sp.commit()?;
                Ok(recips)
            }
            // dropping `sp` rolls back the inserts, not the acks
            Err(e) => Err(e),
        }
    };
    tx.commit()?;
    Ok(sent)
}

fn queue(
    c: &Connection,
    from: &str,
    target: &str,
    body: &str,
) -> Res<Vec<String>> {
    let (topic, recips): (Option<&str>, Vec<String>) = match target.strip_prefix('#') {
        Some(t) => {
            let mut s = c.prepare("SELECT name FROM subs WHERE topic=?1 AND name<>?2 ORDER BY name")?;
            let names = s.query_map(params![t, from], |r| r.get(0))?.collect::<Result<_, _>>()?;
            (Some(t), names)
        }
        None => {
            let known: bool =
                c.query_row("SELECT EXISTS(SELECT 1 FROM agents WHERE name=?1)", [target], |r| r.get(0))?;
            if !known {
                return Err(BusErr::new("unknown recipient"));
            }
            (None, vec![target.to_string()])
        }
    };
    for r in &recips {
        c.execute(
            "INSERT INTO messages(from_name, to_name, topic, body, at) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![from, r, topic, body, now()],
        )?;
    }
    Ok(recips)
}

pub fn pending(
    c: &Connection,
    name: &str,
    after: i64,
    limit: i64,
) -> Res<Vec<Msg>> {
    let mut s = c.prepare(
        "SELECT id, from_name, to_name, topic, body, at FROM messages
         WHERE to_name=?1 AND id>?2 ORDER BY id LIMIT ?3",
    )?;
    let rows = s.query_map(params![name, after, limit], |r| {
        Ok(Msg { id: r.get(0)?, from: r.get(1)?, to: r.get(2)?, topic: r.get(3)?, body: r.get(4)?, at: r.get(5)? })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

pub fn ack(
    c: &Connection,
    name: &str,
    upto: i64,
) -> Res<()> {
    c.execute("DELETE FROM messages WHERE to_name=?1 AND id<=?2", params![name, upto])?;
    Ok(())
}

/// All of `acks` in one commit.
pub fn apply_acks(
    c: &mut Connection,
    acks: &[(String, i64)],
) -> Res<()> {
    let tx = c.transaction()?;
    for (name, upto) in acks {
        ack(&tx, name, *upto)?;
    }
    tx.commit()?;
    Ok(())
}
