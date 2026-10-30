//! The unix-socket API sessions use: newline JSON, `{"op": ..}` in,
//! `{"ok": true, ..}` or `{"error": {code, message}}` out. The caller's session is
//! found from the connecting process's ancestry, never from what it claims.
use std::sync::Arc;

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    time::{Duration, Instant, timeout},
};

use super::{Agent, ancestry, anchor, processes, store, uuid7};
use crate::db::{BusErr, Res, now};

pub async fn accept(
    agent: Arc<Agent>,
    l: UnixListener,
) {
    loop {
        let Ok((s, _)) = l.accept().await else { continue };
        tokio::spawn(serve(agent.clone(), s));
    }
}

async fn serve(
    agent: Arc<Agent>,
    s: UnixStream,
) {
    let pid = s.peer_cred().ok().and_then(|c| c.pid());
    let (r, mut w) = s.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let res = match serde_json::from_str::<Value>(&line) {
            Ok(req) => handle(&agent, pid.map(i64::from), &req).await,
            Err(e) => Err(BusErr::bad_request(e.to_string())),
        };
        let out = match res {
            Ok(mut v) => {
                v["ok"] = json!(true);
                v
            }
            Err(e) => json!({"error": {"code": e.code, "message": e.msg}}),
        };
        if w.write_all(format!("{out}\n").as_bytes()).await.is_err() {
            return;
        }
    }
}

fn str_<'a>(
    req: &'a Value,
    k: &str,
) -> Res<&'a str> {
    req[k].as_str().filter(|s| !s.is_empty()).ok_or_else(|| BusErr::bad_request(format!("{k} is required")))
}

/// The session behind this connection: the one recorded for the nearest
/// ancestor of the caller (`chain`, from `ancestry`), or a new session tied to the caller's parent.
fn caller(
    agent: &Agent,
    chain: &[(i64, String, String)],
    req: &Value,
) -> Res<String> {
    let db = agent.db();
    if agent.cfg.trust_clients {
        if let Some(s) = req["session"].as_str() {
            store::touch_session(&db, s)?;
            return Ok(s.into());
        }
    }
    if chain.is_empty() {
        return Err(BusErr::new("unidentified", "cannot tell which process is calling"));
    }
    let keys: Vec<(i64, String)> = chain.iter().map(|c| (c.0, c.1.clone())).collect();
    if let Some(s) = store::session_for_chain(&db, &keys)? {
        store::touch_session(&db, &s)?;
        return Ok(s);
    }
    let s = format!("auto-{}", uuid7());
    store::touch_session(&db, &s)?;
    let anchor = anchor(chain).unwrap_or(&chain[0]);
    store::bind_pid(&db, anchor.0, &anchor.1, &s)?;
    Ok(s)
}

fn short(s: &str) -> String {
    s.chars().take(8).collect()
}

struct Who {
    session: String,
    alias: String,
    host: String,
}

/// Every session known on any host, as `(host, session, aliases-by-project)`.
fn universe(agent: &Agent) -> Res<(Vec<(String, String, String, String)>, Vec<(String, String, String, String)>, Vec<String>)> {
    // (host, project, alias, session), (host, project, topic, session), projects
    let db = agent.db();
    let me = agent.cfg.host.clone();
    let mut al: Vec<_> = store::local_aliases(&db)?.into_iter().map(|(p, a, s)| (me.clone(), p, a, s)).collect();
    let mut subs: Vec<_> = store::local_subs(&db)?.into_iter().map(|(p, t, s)| (me.clone(), p, t, s)).collect();
    let mut projects = store::projects(&db)?;
    for (host, rec) in store::directory(&db)? {
        let rows = |k: &str| rec[k].as_array().cloned().unwrap_or_default();
        let t = |r: &Value, i: usize| r[i].as_str().unwrap_or_default().to_string();
        al.extend(rows("aliases").iter().map(|r| (host.clone(), t(r, 0), t(r, 1), t(r, 2))));
        subs.extend(rows("subs").iter().map(|r| (host.clone(), t(r, 0), t(r, 1), t(r, 2))));
        projects.extend(rows("projects").iter().filter_map(|p| p.as_str().map(String::from)));
    }
    projects.sort();
    projects.dedup();
    Ok((al, subs, projects))
}

/// Names sessions recently gave up, as `(host, project, alias, session)`.
fn former(agent: &Agent) -> Res<Vec<(String, String, String, String)>> {
    let db = agent.db();
    let me = agent.cfg.host.clone();
    let mut out: Vec<_> = store::local_former(&db)?.into_iter().map(|(p, a, s)| (me.clone(), p, a, s)).collect();
    for (host, rec) in store::directory(&db)? {
        let t = |r: &Value, i: usize| r[i].as_str().unwrap_or_default().to_string();
        out.extend(rec["former"].as_array().into_iter().flatten().map(|r| (host.clone(), t(r, 0), t(r, 1), t(r, 2))));
    }
    Ok(out)
}

fn known_project(
    agent: &Agent,
    project: &str,
) -> Res<()> {
    let (_, _, projects) = universe(agent)?;
    if projects.iter().any(|p| p == project) {
        return Ok(());
    }
    Err(BusErr::new(
        "unknown_project",
        format!("project \"{project}\" is not registered; registered: [{}]. Ask your user before registering one", projects.join(", ")),
    ))
}

fn sender(
    agent: &Agent,
    project: &str,
    session: &str,
) -> Res<Who> {
    let alias = store::alias_of(&agent.db(), project, session)?.unwrap_or_else(|| short(session));
    Ok(Who { session: session.into(), alias, host: agent.cfg.host.clone() })
}

/// Picks the recipient sessions `to` names inside `project`.
fn resolve(
    agent: &Agent,
    project: &str,
    to: &str,
) -> Res<Vec<(String, String, String)>> {
    // (host, session, alias)
    let (al, subs, _) = universe(agent)?;
    let alias_of = |s: &str| al.iter().find(|a| a.1 == project && a.3 == s).map_or_else(|| short(s), |a| a.2.clone());
    if let Some(topic) = to.strip_prefix('#') {
        return Ok(subs
            .iter()
            .filter(|s| s.1 == project && s.2 == topic)
            .map(|s| (s.0.clone(), s.3.clone(), alias_of(&s.3)))
            .collect());
    }
    let (name, host) = match to.split_once('@') {
        Some((n, h)) => (n, Some(h)),
        None => (to, None),
    };
    let on_host = |h: &str| host.is_none_or(|x| x == h);
    let mut hits: Vec<(String, String, String)> = al
        .iter()
        .filter(|a| a.1 == project && a.2 == name && on_host(&a.0))
        .map(|a| (a.0.clone(), a.3.clone(), a.2.clone()))
        .collect();
    if hits.is_empty() {
        hits = former(agent)?
            .into_iter()
            .filter(|f| f.1 == project && f.2 == name && on_host(&f.0))
            .map(|f| (f.0, f.3.clone(), alias_of(&f.3)))
            .collect();
    }
    if hits.is_empty() {
        // a session id, for sessions that never took an alias
        let db = agent.db();
        let me = &agent.cfg.host;
        let mut ids: Vec<(String, String)> =
            store::sessions(&db)?.into_iter().map(|(s, _)| (me.clone(), s)).collect();
        for (h, rec) in store::directory(&db)? {
            ids.extend(rec["sessions"].as_array().into_iter().flatten().filter_map(|s| Some((h.clone(), s[0].as_str()?.to_string()))));
        }
        hits = ids.into_iter().filter(|(h, s)| s == name && on_host(h)).map(|(h, s)| { let a = alias_of(&s); (h, s, a) }).collect();
    }
    match hits.len() {
        1 => Ok(hits),
        0 => {
            let names: Vec<String> = al.iter().filter(|a| a.1 == project).map(|a| format!("{}@{}", a.2, a.0)).collect();
            Err(BusErr::new("unknown_recipient", format!(
                    "no session \"{to}\" in {project}; known: [{}]. A session is listed once it has called any sessionbus tool with `as`; renamed sessions answer to their old name for an hour only",
                    names.join(", ")
                )))
        }
        _ => {
            let names: Vec<String> = hits.iter().map(|h| format!("{}@{}", h.2, h.0)).collect();
            Err(BusErr::new("ambiguous", format!("\"{to}\" matches {}; use one of [{}]", hits.len(), names.join(", "))))
        }
    }
}

fn send(
    agent: &Agent,
    session: &str,
    req: &Value,
) -> Res<Value> {
    let project = str_(req, "project")?;
    known_project(agent, project)?;
    let to = str_(req, "to")?;
    let body = str_(req, "body")?;
    let ttl = req["ttl_s"].as_i64().unwrap_or(agent.cfg.default_ttl).clamp(1, agent.cfg.max_ttl);
    let who = sender(agent, project, session)?;
    let targets = resolve(agent, project, to)?;
    let opt = |k: &str| req[k].as_str().map(String::from);
    let mut sent = vec![];
    for (host, sess, alias) in targets {
        let env = store::Envelope {
            id: uuid7(),
            project: project.into(),
            from_session: who.session.clone(),
            from_alias: who.alias.clone(),
            from_host: who.host.clone(),
            to_session: sess.clone(),
            topic: to.strip_prefix('#').map(String::from),
            body: body.into(),
            reply_to: opt("reply_to"),
            status: opt("status"),
            priority: opt("priority"),
            summary: opt("summary"),
            ttl_s: ttl,
        };
        let local = host == agent.cfg.host;
        let mut row = store::Sent {
            id: env.id.clone(),
            project: project.into(),
            to_session: sess,
            to_alias: alias.clone(),
            to_host: host.clone(),
            state: if local { "delivered" } else { "queued" }.into(),
        };
        if local {
            store::queue_sent(&mut agent.db(), &row, session, None)?;
            store::deliver(&agent.db(), &env)?;
            agent.wake_inbox();
        } else {
            let payload = serde_json::to_string(&env)?;
            store::queue_sent(&mut agent.db(), &row, session, Some(("msg", &payload, now() + ttl)))?;
            agent.kick.notify_one();
        }
        row.to_alias = alias;
        sent.push(json!(row));
    }
    Ok(json!({"sent": sent}))
}

fn list(
    agent: &Agent,
    req: &Value,
) -> Res<Value> {
    let project = str_(req, "project")?;
    known_project(agent, project)?;
    let (al, _, _) = universe(agent)?;
    let db = agent.db();
    let unread: std::collections::HashMap<_, _> = store::unread_counts(&db)?.into_iter().collect();
    let mut seen: std::collections::HashMap<String, i64> = store::sessions(&db)?.into_iter().collect();
    for (_, rec) in store::directory(&db)? {
        for r in rec["sessions"].as_array().into_iter().flatten() {
            if let (Some(id), Some(t)) = (r[0].as_str(), r[1].as_i64()) {
                seen.insert(id.into(), t);
            }
        }
    }
    let sessions: Vec<Value> = al
        .iter()
        .filter(|a| a.1 == project)
        .map(|a| {
            let local = a.0 == agent.cfg.host;
            json!({"alias": a.2, "session": a.3, "host": a.0,
                   "unread": if local { unread.get(&a.3).copied() } else { None },
                   "last_seen": seen.get(&a.3).copied()})
        })
        .collect();
    let hosts: Vec<Value> = store::directory(&db)?.into_iter().map(|(h, r)| json!({"host": h, "version": r["version"]})).collect();
    Ok(json!({"sessions": sessions, "hosts": hosts, "host": agent.cfg.host}))
}

fn commit(
    agent: &Agent,
    session: &str,
    ids: &[String],
) -> Res<Value> {
    let read = store::mark_read(&agent.db(), session, ids)?;
    for (id, from_host) in &read {
        if *from_host == agent.cfg.host {
            store::set_sent_state(&agent.db(), id, "read")?;
        } else {
            let payload = json!({"id": id, "state": "read"}).to_string();
            store::queue_wire(&agent.db(), id, "receipt", from_host, &payload, now() + agent.cfg.max_ttl)?;
        }
    }
    agent.kick.notify_one();
    Ok(json!({"read": read.len()}))
}

async fn handle(
    agent: &Agent,
    pid: Option<i64>,
    req: &Value,
) -> Res<Value> {
    let op = str_(req, "op")?;
    let claimed = agent.cfg.trust_clients && req["session"].is_string();
    // `ps` takes a while: run it before taking the database lock and off the async threads
    let chain = match pid {
        Some(p) if !claimed || op == "register" => tokio::task::spawn_blocking(move || ancestry(p)).await.unwrap_or_default(),
        _ => vec![],
    };
    if op == "register" {
        // identity comes from the hook's process tree, not from the request, unless tests allow it
        let session = str_(req, "session")?;
        let anchor = match (agent.cfg.trust_clients, req["anchor"].as_i64()) {
            (true, Some(a)) => (a, req["anchor_started"].as_str().unwrap_or("").to_string()),
            _ => {
                let a = anchor(&chain);
                let a = a.ok_or_else(|| BusErr::new("unidentified", "cannot tell which process is calling"))?;
                (a.0, a.1.clone())
            }
        };
        let old = store::register_session(&mut agent.db(), session, req["cwd"].as_str().unwrap_or(""), (anchor.0, &anchor.1))?;
        agent.publish()?;
        return Ok(json!({"session": session, "replaced": old}));
    }
    let session = caller(agent, &chain, req)?;
    match op {
        "whoami" => {
            let db = agent.db();
            let aliases: Vec<_> = store::local_aliases(&db)?
                .into_iter()
                .filter(|a| a.2 == session)
                .map(|a| json!({"project": a.0, "alias": a.1}))
                .collect();
            Ok(json!({"session": session, "host": agent.cfg.host, "aliases": aliases}))
        }
        "alias" => {
            let (project, alias) = (str_(req, "project")?, str_(req, "alias")?);
            known_project(agent, project)?;
            if alias.contains(['@', '#', '/']) || alias.contains(char::is_whitespace) || alias.len() > 64 {
                return Err(BusErr::bad_request("alias may not contain whitespace, @, # or /"));
            }
            let procs = tokio::task::spawn_blocking(processes).await.unwrap_or_default();
            let dead = |ps: &[(i64, String)]| !procs.is_empty() && ps.iter().all(|(p, st)| procs.get(p).is_none_or(|x| &x.1 != st));
            store::set_alias(&mut agent.db(), project, &session, alias, &dead)?;
            agent.publish()?;
            Ok(json!({"session": session, "alias": alias}))
        }
        "peers" => {
            let db = agent.db();
            let known: std::collections::HashSet<_> = store::directory(&db)?.into_iter().map(|d| d.0).collect();
            let queued: std::collections::HashMap<_, _> = store::queued_by_host(&db)?.into_iter().collect();
            let mut hosts = agent.peer_hosts();
            hosts.sort();
            let peers: Vec<Value> = hosts
                .iter()
                .map(|h| json!({"host": h, "addr": agent.peer_addr(h), "in_directory": known.contains(h), "queued": queued.get(h).copied().unwrap_or(0)}))
                .collect();
            Ok(json!({"host": agent.cfg.host, "peers": peers}))
        }
        "projects" => Ok(json!({"projects": universe(agent)?.2})),
        "addproject" => {
            store::add_project(&agent.db(), str_(req, "name")?)?;
            agent.publish()?;
            Ok(json!({"projects": universe(agent)?.2}))
        }
        "list" => list(agent, req),
        "send" => send(agent, &session, req),
        "sub" | "unsub" => {
            let (p, t) = (str_(req, "project")?, str_(req, "topic")?);
            known_project(agent, p)?;
            store::subscribe(&agent.db(), p, t, &session, op == "sub")?;
            agent.publish()?;
            Ok(json!({}))
        }
        "topics" => {
            let p = str_(req, "project")?;
            let (_, subs, _) = universe(agent)?;
            let mut t: Vec<_> = subs.into_iter().filter(|s| s.1 == p).map(|s| s.2).collect();
            t.sort();
            t.dedup();
            Ok(json!({"topics": t}))
        }
        "status" => {
            let ids: Vec<String> = serde_json::from_value(req["ids"].clone()).unwrap_or_default();
            Ok(json!({"messages": store::sent_status(&agent.db(), &session, &ids)?}))
        }
        "peek" | "wait" => {
            let project = req["project"].as_str();
            let mut rx = agent.inbox.subscribe();
            let mine = (op == "wait" && req["exclusive"] == true).then(|| agent.new_waiter(&session));
            let deadline = Instant::now() + Duration::from_millis(req["timeout_ms"].as_u64().unwrap_or(0).min(600_000));
            loop {
                if mine.is_some_and(|n| n != agent.current_waiter(&session)) {
                    return Ok(json!({"messages": [], "superseded": true}));
                }
                let m = store::pending(&agent.db(), &session, project)?;
                if !m.is_empty() || op == "peek" || Instant::now() >= deadline {
                    return Ok(json!({"messages": m}));
                }
                let _ = timeout(deadline - Instant::now(), rx.changed()).await;
            }
        }
        "commit" => {
            let ids: Vec<String> = serde_json::from_value(req["ids"].clone()).unwrap_or_default();
            commit(agent, &session, &ids)
        }
        _ => Err(BusErr::bad_request(format!("unknown op {op:?}"))),
    }
}
