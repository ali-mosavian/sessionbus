//! The agent's durable state: sessions and their aliases, the outbox of
//! envelopes waiting for a peer, the inbox it owns, and other hosts' records.
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::db::{BusErr, Res, now};

/// One message on the wire and in an inbox. `ttl_s` is what is left of its
/// life when it is pushed: the receiver counts from its own clock.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope {
    pub id: String,
    pub project: String,
    pub from_session: String,
    pub from_alias: String,
    pub from_host: String,
    pub to_session: String,
    pub topic: Option<String>,
    pub body: String,
    pub reply_to: Option<String>,
    pub status: Option<String>,
    pub priority: Option<String>,
    pub summary: Option<String>,
    pub ttl_s: i64,
}

#[derive(Clone, Debug)]
pub struct OutItem {
    pub id: String,
    pub kind: String,
    pub payload: String,
    pub expires: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Sent {
    pub id: String,
    pub project: String,
    pub to_session: String,
    pub to_alias: String,
    pub to_host: String,
    pub state: String,
}

pub fn open(path: &Path) -> Res<Connection> {
    let c = Connection::open(path)?;
    c.execute_batch(
        "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON; PRAGMA busy_timeout=5000;
         CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY, cwd TEXT, started INTEGER NOT NULL, last_seen INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS pids(pid INTEGER PRIMARY KEY, started TEXT NOT NULL, session TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS aliases(project TEXT NOT NULL, alias TEXT NOT NULL, session TEXT NOT NULL,
             PRIMARY KEY(project, alias), UNIQUE(project, session));
         CREATE TABLE IF NOT EXISTS projects(name TEXT PRIMARY KEY);
         CREATE TABLE IF NOT EXISTS subs(project TEXT NOT NULL, topic TEXT NOT NULL, session TEXT NOT NULL,
             PRIMARY KEY(project, topic, session));
         CREATE TABLE IF NOT EXISTS outbox(id TEXT NOT NULL, kind TEXT NOT NULL, to_host TEXT NOT NULL,
             payload TEXT NOT NULL, expires INTEGER NOT NULL, PRIMARY KEY(id, kind, to_host));
         CREATE TABLE IF NOT EXISTS sent(id TEXT PRIMARY KEY, project TEXT NOT NULL, from_session TEXT NOT NULL,
             to_session TEXT NOT NULL, to_alias TEXT NOT NULL, to_host TEXT NOT NULL, created INTEGER NOT NULL,
             state TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS inbox(id TEXT PRIMARY KEY, project TEXT NOT NULL, from_session TEXT NOT NULL,
             from_alias TEXT NOT NULL, from_host TEXT NOT NULL, to_session TEXT NOT NULL, topic TEXT, body TEXT NOT NULL,
             reply_to TEXT, status TEXT, priority TEXT, summary TEXT, received INTEGER NOT NULL, expires INTEGER NOT NULL,
             read_at INTEGER);
         CREATE INDEX IF NOT EXISTS inbox_unread ON inbox(to_session, received) WHERE read_at IS NULL;
         CREATE TABLE IF NOT EXISTS directory(host TEXT PRIMARY KEY, version INTEGER NOT NULL, record TEXT NOT NULL,
             received INTEGER NOT NULL);",
    )?;
    Ok(c)
}

// --- sessions ---------------------------------------------------------------

/// Binds `anchor` (the pid of the Claude process a hook runs under) to `session`.
/// An anchor that belonged to another session means that session was replaced
/// (`/clear`, resume): its aliases, subscriptions and unread messages move to
/// the new one, and its id is returned.
pub fn register_session(
    c: &mut Connection,
    session: &str,
    cwd: &str,
    anchor: (i64, &str),
) -> Res<Option<String>> {
    let tx = c.transaction()?;
    let t = now();
    tx.execute(
        "INSERT INTO sessions(id, cwd, started, last_seen) VALUES(?1, ?2, ?3, ?3)
         ON CONFLICT(id) DO UPDATE SET cwd=?2, last_seen=?3",
        params![session, cwd, t],
    )?;
    // the same pid with another start time is a different process that reused the number
    let old: Option<String> = tx
        .query_row("SELECT session FROM pids WHERE pid=?1 AND started=?2", params![anchor.0, anchor.1], |r| r.get(0))
        .optional()?
        .filter(|p: &String| p != session);
    tx.execute(
        "INSERT INTO pids(pid, started, session) VALUES(?1, ?2, ?3) ON CONFLICT(pid) DO UPDATE SET started=?2, session=?3",
        params![anchor.0, anchor.1, session],
    )?;
    if let Some(old) = &old {
        for sql in [
            "UPDATE OR IGNORE aliases SET session=?2 WHERE session=?1",
            "UPDATE OR IGNORE subs SET session=?2 WHERE session=?1",
            "UPDATE inbox SET to_session=?2 WHERE to_session=?1 AND read_at IS NULL",
        ] {
            tx.execute(sql, params![old, session])?;
        }
    }
    tx.commit()?;
    Ok(old)
}

/// The session whose recorded processes include the nearest of `chain` (a
/// caller's ancestry, nearest first).
pub fn session_for_chain(
    c: &Connection,
    chain: &[(i64, String)],
) -> Res<Option<String>> {
    for (pid, started) in chain {
        if let Some(s) = c
            .query_row("SELECT session FROM pids WHERE pid=?1 AND started=?2", params![pid, started], |r| r.get(0))
            .optional()?
        {
            return Ok(Some(s));
        }
    }
    Ok(None)
}

pub fn touch_session(
    c: &Connection,
    id: &str,
) -> Res<()> {
    c.execute(
        "INSERT INTO sessions(id, cwd, started, last_seen) VALUES(?1, '', ?2, ?2)
         ON CONFLICT(id) DO UPDATE SET last_seen=?2",
        params![id, now()],
    )?;
    Ok(())
}

pub fn bind_pid(
    c: &Connection,
    pid: i64,
    started: &str,
    session: &str,
) -> Res<()> {
    c.execute(
        "INSERT INTO pids(pid, started, session) VALUES(?1, ?2, ?3) ON CONFLICT(pid) DO UPDATE SET started=?2, session=?3",
        params![pid, started, session],
    )?;
    Ok(())
}

pub fn sessions(c: &Connection) -> Res<Vec<(String, i64)>> {
    let mut s = c.prepare("SELECT id, last_seen FROM sessions ORDER BY id")?;
    Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?)
}

// --- aliases, projects, subscriptions -------------------------------------

pub fn set_alias(
    c: &mut Connection,
    project: &str,
    session: &str,
    alias: &str,
    dead: &dyn Fn(&[(i64, String)]) -> bool,
) -> Res<()> {
    let tx = c.transaction()?;
    let owner: Option<String> = tx
        .query_row("SELECT session FROM aliases WHERE project=?1 AND alias=?2", params![project, alias], |r| r.get(0))
        .optional()?;
    if let Some(o) = owner.filter(|o| o != session) {
        // a restarted Claude is a new session: its predecessor's name is free once that process is gone
        let procs: Vec<(i64, String)> = tx
            .prepare("SELECT pid, started FROM pids WHERE session=?1")?
            .query_map([&o], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        if procs.is_empty() || !dead(&procs) {
            return Err(BusErr::new("name_in_use", format!("alias \"{alias}\" is taken by another session in {project}")));
        }
        tx.execute("DELETE FROM aliases WHERE project=?1 AND alias=?2", params![project, alias])?;
    }
    tx.execute("DELETE FROM aliases WHERE project=?1 AND session=?2", params![project, session])?;
    tx.execute("INSERT INTO aliases(project, alias, session) VALUES(?1, ?2, ?3)", params![project, alias, session])?;
    tx.commit()?;
    Ok(())
}

pub fn alias_of(
    c: &Connection,
    project: &str,
    session: &str,
) -> Res<Option<String>> {
    Ok(c.query_row("SELECT alias FROM aliases WHERE project=?1 AND session=?2", params![project, session], |r| r.get(0)).optional()?)
}

pub fn local_aliases(c: &Connection) -> Res<Vec<(String, String, String)>> {
    let mut s = c.prepare("SELECT project, alias, session FROM aliases ORDER BY project, alias")?;
    Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?)
}

pub fn projects(c: &Connection) -> Res<Vec<String>> {
    let mut s = c.prepare("SELECT name FROM projects ORDER BY name")?;
    Ok(s.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?)
}

pub fn add_project(
    c: &Connection,
    name: &str,
) -> Res<bool> {
    Ok(c.execute("INSERT OR IGNORE INTO projects(name) VALUES(?1)", [name])? > 0)
}

pub fn subscribe(
    c: &Connection,
    project: &str,
    topic: &str,
    session: &str,
    join: bool,
) -> Res<()> {
    if join {
        c.execute("INSERT OR IGNORE INTO subs(project, topic, session) VALUES(?1, ?2, ?3)", params![project, topic, session])?;
    } else {
        c.execute("DELETE FROM subs WHERE project=?1 AND topic=?2 AND session=?3", params![project, topic, session])?;
    }
    Ok(())
}

pub fn local_subs(c: &Connection) -> Res<Vec<(String, String, String)>> {
    let mut s = c.prepare("SELECT project, topic, session FROM subs ORDER BY project, topic, session")?;
    Ok(s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?.collect::<Result<_, _>>()?)
}

// --- outbox and sent --------------------------------------------------------

pub fn queue_sent(
    c: &mut Connection,
    sent: &Sent,
    from_session: &str,
    wire: Option<(&str, &str, i64)>,
) -> Res<()> {
    let tx = c.transaction()?;
    tx.execute(
        "INSERT OR IGNORE INTO sent(id, project, from_session, to_session, to_alias, to_host, created, state)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![sent.id, sent.project, from_session, sent.to_session, sent.to_alias, sent.to_host, now(), sent.state],
    )?;
    if let Some((kind, payload, expires)) = wire {
        tx.execute(
            "INSERT OR IGNORE INTO outbox(id, kind, to_host, payload, expires) VALUES(?1, ?2, ?3, ?4, ?5)",
            params![sent.id, kind, sent.to_host, payload, expires],
        )?;
    }
    tx.commit()?;
    Ok(())
}

pub fn queue_wire(
    c: &Connection,
    id: &str,
    kind: &str,
    host: &str,
    payload: &str,
    expires: i64,
) -> Res<()> {
    c.execute(
        "INSERT OR REPLACE INTO outbox(id, kind, to_host, payload, expires) VALUES(?1, ?2, ?3, ?4, ?5)",
        params![id, kind, host, payload, expires],
    )?;
    Ok(())
}

pub fn due_outbox(
    c: &Connection,
    host: &str,
    limit: i64,
) -> Res<Vec<OutItem>> {
    let mut s = c.prepare(
        "SELECT id, kind, payload, expires FROM outbox WHERE to_host=?1 AND expires>?2 ORDER BY rowid LIMIT ?3",
    )?;
    Ok(s.query_map(params![host, now(), limit], |r| {
        Ok(OutItem { id: r.get(0)?, kind: r.get(1)?, payload: r.get(2)?, expires: r.get(3)? })
    })?
    .collect::<Result<_, _>>()?)
}

pub fn hosts_with_outbox(c: &Connection) -> Res<Vec<String>> {
    let mut s = c.prepare("SELECT DISTINCT to_host FROM outbox WHERE expires>?1")?;
    Ok(s.query_map([now()], |r| r.get(0))?.collect::<Result<_, _>>()?)
}

pub fn queued_by_host(c: &Connection) -> Res<Vec<(String, i64)>> {
    let mut s = c.prepare("SELECT to_host, COUNT(*) FROM outbox WHERE kind='msg' AND expires>?1 GROUP BY to_host")?;
    Ok(s.query_map([now()], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?)
}

/// Removes an acked item, unless it was replaced while in flight (a newer
/// directory record must stay queued).
pub fn outbox_done(
    c: &Connection,
    host: &str,
    id: &str,
    kind: &str,
    payload: &str,
) -> Res<()> {
    c.execute("DELETE FROM outbox WHERE id=?1 AND kind=?2 AND to_host=?3 AND payload=?4", params![id, kind, host, payload])?;
    if kind == "msg" {
        c.execute("UPDATE sent SET state='delivered' WHERE id=?1 AND state='queued'", [id])?;
    }
    Ok(())
}

/// Moves what has outlived its ttl: waiting envelopes are dropped (body
/// included) and their `sent` rows say `expired`; unread inbox messages lose
/// their content.
pub fn expire(c: &Connection) -> Res<usize> {
    let t = now();
    let ids: Vec<String> = c
        .prepare("SELECT id FROM outbox WHERE kind='msg' AND expires<=?1")?
        .query_map([t], |r| r.get(0))?
        .collect::<Result<_, _>>()?;
    for id in &ids {
        c.execute("UPDATE sent SET state='expired' WHERE id=?1 AND state='queued'", [id])?;
    }
    let mut n = c.execute("DELETE FROM outbox WHERE expires<=?1", [t])?;
    n += c.execute("DELETE FROM inbox WHERE read_at IS NULL AND expires<=?1", [t])?;
    Ok(n)
}

/// Drops read messages and old status rows after `keep` seconds, and sessions
/// unseen for `idle` seconds with their names, subscriptions and process records.
pub fn purge(
    c: &Connection,
    keep: i64,
    idle: i64,
) -> Res<()> {
    let before = now() - keep;
    c.execute("DELETE FROM inbox WHERE read_at IS NOT NULL AND read_at<=?1", [before])?;
    c.execute("DELETE FROM sent WHERE created<=?1", [before])?;
    let gone = now() - idle;
    for t in ["aliases", "subs", "pids"] {
        c.execute(&format!("DELETE FROM {t} WHERE session IN (SELECT id FROM sessions WHERE last_seen<=?1)"), [gone])?;
    }
    c.execute("DELETE FROM sessions WHERE last_seen<=?1 AND id NOT IN (SELECT to_session FROM inbox WHERE read_at IS NULL)", [gone])?;
    Ok(())
}

/// `read` always wins (a receipt can outlive an `expired` mark when the ack was lost);
/// anything else only moves a message that is still queued.
pub fn set_sent_state(
    c: &Connection,
    id: &str,
    state: &str,
) -> Res<()> {
    if state == "read" {
        c.execute("UPDATE sent SET state='read' WHERE id=?1 AND state!='read'", [id])?;
    } else {
        c.execute("UPDATE sent SET state=?2 WHERE id=?1 AND state='queued'", params![id, state])?;
    }
    Ok(())
}

/// The receiver refused the message for good.
pub fn outbox_reject(
    c: &Connection,
    host: &str,
    id: &str,
) -> Res<()> {
    c.execute("DELETE FROM outbox WHERE id=?1 AND kind='msg' AND to_host=?2", params![id, host])?;
    set_sent_state(c, id, "rejected")
}

pub fn sent_status(
    c: &Connection,
    from_session: &str,
    ids: &[String],
) -> Res<Vec<Sent>> {
    let mut out = Vec::new();
    for id in ids {
        let row = c
            .query_row(
                "SELECT id, project, to_session, to_alias, to_host, state FROM sent WHERE id=?1 AND from_session=?2",
                params![id, from_session],
                |r| Ok(Sent { id: r.get(0)?, project: r.get(1)?, to_session: r.get(2)?, to_alias: r.get(3)?, to_host: r.get(4)?, state: r.get(5)? }),
            )
            .optional()?;
        out.extend(row);
    }
    Ok(out)
}

// --- inbox --------------------------------------------------------------------

/// Stores an envelope the sender pushed. Pushing the same id again changes nothing.
pub fn deliver(
    c: &Connection,
    e: &Envelope,
) -> Res<bool> {
    let t = now();
    Ok(c.execute(
        "INSERT OR IGNORE INTO inbox(id, project, from_session, from_alias, from_host, to_session, topic, body, reply_to,
             status, priority, summary, received, expires) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            e.id, e.project, e.from_session, e.from_alias, e.from_host, e.to_session, e.topic, e.body, e.reply_to,
            e.status, e.priority, e.summary, t, t + e.ttl_s
        ],
    )? > 0)
}

#[derive(Clone, Debug, Serialize)]
pub struct InMsg {
    pub id: String,
    pub project: String,
    pub from: String,
    pub from_session: String,
    pub from_host: String,
    pub topic: Option<String>,
    pub body: String,
    pub reply_to: Option<String>,
    pub status: Option<String>,
    pub priority: String,
    pub summary: Option<String>,
    pub received: i64,
}

pub fn pending(
    c: &Connection,
    session: &str,
    project: Option<&str>,
) -> Res<Vec<InMsg>> {
    let mut s = c.prepare(
        "SELECT id, project, from_alias, from_session, from_host, topic, body, reply_to, status, COALESCE(priority,'normal'),
                summary, received FROM inbox
         WHERE to_session=?1 AND read_at IS NULL AND expires>?2 AND (?3 IS NULL OR project=?3)
         ORDER BY (priority='urgent') DESC, received, rowid",
    )?;
    Ok(s.query_map(params![session, now(), project], |r| {
        Ok(InMsg {
            id: r.get(0)?, project: r.get(1)?, from: r.get(2)?, from_session: r.get(3)?, from_host: r.get(4)?,
            topic: r.get(5)?, body: r.get(6)?, reply_to: r.get(7)?, status: r.get(8)?, priority: r.get(9)?,
            summary: r.get(10)?, received: r.get(11)?,
        })
    })?
    .collect::<Result<_, _>>()?)
}

/// Marks messages read and returns `(id, sender's host, sender's session)` for the receipts.
pub fn mark_read(
    c: &Connection,
    session: &str,
    ids: &[String],
) -> Res<Vec<(String, String)>> {
    let mut out = Vec::new();
    for id in ids {
        let from: Option<String> = c
            .query_row("SELECT from_host FROM inbox WHERE id=?1 AND to_session=?2 AND read_at IS NULL", params![id, session], |r| r.get(0))
            .optional()?;
        if let Some(h) = from {
            // the body is not needed once read; the row stays for status and dedupe
            c.execute("UPDATE inbox SET read_at=?3, body='' WHERE id=?1 AND to_session=?2", params![id, session, now()])?;
            out.push((id.clone(), h));
        }
    }
    Ok(out)
}

pub fn unread_counts(c: &Connection) -> Res<Vec<(String, i64)>> {
    let mut s = c.prepare("SELECT to_session, COUNT(*) FROM inbox WHERE read_at IS NULL AND expires>?1 GROUP BY to_session")?;
    Ok(s.query_map([now()], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?)
}

// --- directory of other hosts -----------------------------------------------

/// Keeps a host's record if it is newer than what we hold.
pub fn put_dir(
    c: &Connection,
    host: &str,
    version: i64,
    record: &Value,
) -> Res<bool> {
    let have: Option<i64> =
        c.query_row("SELECT version FROM directory WHERE host=?1", [host], |r| r.get(0)).optional()?;
    if have.is_some_and(|v| v >= version) {
        return Ok(false);
    }
    c.execute(
        "INSERT OR REPLACE INTO directory(host, version, record, received) VALUES(?1, ?2, ?3, ?4)",
        params![host, version, record.to_string(), now()],
    )?;
    Ok(true)
}

pub fn directory(c: &Connection) -> Res<Vec<(String, Value)>> {
    let mut s = c.prepare("SELECT host, record FROM directory ORDER BY host")?;
    let rows: Vec<(String, String)> = s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<Result<_, _>>()?;
    Ok(rows.into_iter().filter_map(|(h, r)| Some((h, serde_json::from_str(&r).ok()?))).collect())
}
