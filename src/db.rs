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
use serde::{Deserialize, Serialize};

use crate::lock;

/// An error with a machine-readable `code` (bad_request, auth,
/// unknown_recipient, rate_limited, internal) and a message for people.
#[derive(Debug)]
pub struct BusErr {
    pub code: &'static str,
    pub msg: String,
    /// For rate_limited: when a retry can succeed.
    pub retry_ms: Option<u64>,
}

impl BusErr {
    pub fn new(
        code: &'static str,
        msg: impl Into<String>,
    ) -> Self {
        BusErr { code, msg: msg.into(), retry_ms: None }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        BusErr::new("bad_request", msg)
    }
}

impl fmt::Display for BusErr {
    fn fmt(
        &self,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl<E: std::error::Error> From<E> for BusErr {
    fn from(e: E) -> Self {
        BusErr::new("internal", e.to_string())
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
    pub reply_to: Option<i64>,
    pub status: Option<String>,
    pub priority: String,
    pub summary: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Done,
    Blocked,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Done => "done",
            Status::Blocked => "blocked",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    Normal,
    Urgent,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Priority::Normal => "normal",
            Priority::Urgent => "urgent",
        }
    }
}

const MAX_SUMMARY: usize = 200;

/// Optional fields of a send. They are labels set by the sender and carry no
/// authority.
#[derive(Clone, Default, Debug, Deserialize)]
pub struct Meta {
    pub reply_to: Option<i64>,
    pub status: Option<Status>,
    pub priority: Option<Priority>,
    pub summary: Option<String>,
}

impl Meta {
    /// Checks what the types cannot, and drops a redundant `normal` priority.
    pub fn validate(&mut self) -> Res<()> {
        if self.reply_to.is_some_and(|r| r < 1) {
            return Err(BusErr::bad_request("reply_to must be a message id"));
        }
        if self.summary.as_deref().is_some_and(|s| s.len() > MAX_SUMMARY || s.contains(['\n', '\r'])) {
            return Err(BusErr::bad_request("summary must be one line of at most 200 bytes"));
        }
        self.priority = self.priority.filter(|p| *p == Priority::Urgent);
        Ok(())
    }
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
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; PRAGMA busy_timeout=5000;
             CREATE TABLE IF NOT EXISTS agents(name TEXT PRIMARY KEY, last_seen INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS subs(topic TEXT NOT NULL, name TEXT NOT NULL, PRIMARY KEY(topic, name));
             CREATE TABLE IF NOT EXISTS messages(
                 id INTEGER PRIMARY KEY AUTOINCREMENT, from_name TEXT NOT NULL, to_name TEXT NOT NULL,
                 topic TEXT, body TEXT NOT NULL, at INTEGER NOT NULL,
                 reply_to INTEGER, status TEXT, priority TEXT, summary TEXT, read_at INTEGER);",
        )?;
        migrate(&c)?;
        c.execute_batch(
            "CREATE INDEX IF NOT EXISTS messages_unread ON messages(to_name, id) WHERE read_at IS NULL;
             DROP INDEX IF EXISTS messages_to;",
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
        tokio::task::spawn_blocking(move || f(&mut lock(&db))).await?
    }

    /// Runs a read on the read-only connection: it does not wait for writers.
    pub async fn read<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Connection) -> Res<T> + Send + 'static,
    ) -> Res<T> {
        let db = self.r.clone();
        tokio::task::spawn_blocking(move || f(&lock(&db))).await?
    }
}

/// Adds the columns databases from before delivery tracking lack.
fn migrate(c: &Connection) -> Res<()> {
    let have: Vec<String> = c
        .prepare("SELECT name FROM pragma_table_info('messages')")?
        .query_map([], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    for (col, ty) in
        [("reply_to", "INTEGER"), ("status", "TEXT"), ("priority", "TEXT"), ("summary", "TEXT"), ("read_at", "INTEGER")]
    {
        if !have.iter().any(|h| h == col) {
            c.execute_batch(&format!("ALTER TABLE messages ADD COLUMN {col} {ty}"))?;
        }
    }
    Ok(())
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
/// in one commit. Returns each recipient with the id of its copy. A refused
/// send rolls the acks back too; the caller still holds them.
pub fn enqueue(
    c: &mut Connection,
    from: &str,
    target: &str,
    body: &str,
    meta: &Meta,
    acks: &[(String, i64)],
) -> Res<Vec<(String, i64)>> {
    let tx = c.transaction()?;
    ack_all(&tx, acks)?;
    let sent = queue(&tx, from, target, body, meta)?;
    tx.commit()?;
    Ok(sent)
}

fn queue(
    c: &Connection,
    from: &str,
    target: &str,
    body: &str,
    meta: &Meta,
) -> Res<Vec<(String, i64)>> {
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
                return Err(BusErr::new("unknown_recipient", "unknown recipient"));
            }
            (None, vec![target.to_string()])
        }
    };
    let mut sent = Vec::with_capacity(recips.len());
    for r in recips {
        c.execute(
            "INSERT INTO messages(from_name, to_name, topic, body, at, reply_to, status, priority, summary)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                from,
                r,
                topic,
                body,
                now(),
                meta.reply_to,
                meta.status.map(Status::as_str),
                meta.priority.map(Priority::as_str),
                meta.summary
            ],
        )?;
        sent.push((r, c.last_insert_rowid()));
    }
    Ok(sent)
}

pub fn pending(
    c: &Connection,
    name: &str,
    after: i64,
    limit: i64,
) -> Res<Vec<Msg>> {
    let mut s = c.prepare(
        "SELECT id, from_name, to_name, topic, body, at, reply_to, status, COALESCE(priority, 'normal'), summary
         FROM messages WHERE to_name=?1 AND id>?2 AND read_at IS NULL ORDER BY id LIMIT ?3",
    )?;
    let rows = s.query_map(params![name, after, limit], |r| {
        Ok(Msg {
            id: r.get(0)?,
            from: r.get(1)?,
            to: r.get(2)?,
            topic: r.get(3)?,
            body: r.get(4)?,
            at: r.get(5)?,
            reply_to: r.get(6)?,
            status: r.get(7)?,
            priority: r.get(8)?,
            summary: r.get(9)?,
        })
    })?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// Marks everything up to `upto` read. The row stays, without its content, so
/// the sender can still ask what became of it.
pub fn ack(
    c: &Connection,
    name: &str,
    upto: i64,
) -> Res<()> {
    c.execute(
        "UPDATE messages SET read_at=?3, body='', summary=NULL WHERE to_name=?1 AND id<=?2 AND read_at IS NULL",
        params![name, upto, now()],
    )?;
    Ok(())
}

/// For each of `from`'s messages among `ids`: (id, recipient, read_at).
pub fn statuses(
    c: &Connection,
    from: &str,
    ids: &[i64],
) -> Res<Vec<(i64, String, Option<i64>)>> {
    let mut s = c.prepare(
        "SELECT id, to_name, read_at FROM messages WHERE from_name=?1 AND id IN (SELECT value FROM json_each(?2))",
    )?;
    let rows = s.query_map(params![from, serde_json::to_string(ids)?], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

/// Drops messages read before `read_before` and unread ones queued before
/// `unread_before`; returns how many.
pub fn purge(
    c: &Connection,
    read_before: i64,
    unread_before: i64,
) -> Res<usize> {
    Ok(c.execute(
        "DELETE FROM messages WHERE (read_at IS NOT NULL AND read_at < ?1) OR (read_at IS NULL AND at < ?2)",
        [read_before, unread_before],
    )?)
}

/// Folds the write-ahead log back in and truncates it, so deleted content
/// does not linger there. Best effort: a reader holding the log defers it.
pub fn compact(c: &Connection) {
    let _ = c.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
}

/// All of `acks` in one commit.
pub fn apply_acks(
    c: &mut Connection,
    acks: &[(String, i64)],
) -> Res<()> {
    let tx = c.transaction()?;
    ack_all(&tx, acks)?;
    tx.commit()?;
    Ok(())
}

fn ack_all(
    c: &Connection,
    acks: &[(String, i64)],
) -> Res<()> {
    acks.iter().try_for_each(|(name, upto)| ack(c, name, *upto))
}
