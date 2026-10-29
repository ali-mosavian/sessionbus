//! One function per request. `dispatch` is the only place that knows them all.
use std::collections::HashMap;

use serde_json::{Value, json};

use super::{
    Hub,
    proto::{Req, SendReq},
};
use crate::db::{self, BusErr, Res};

const MAX_BODY: usize = 1 << 20;
const MAX_STATUS_IDS: usize = 200;

/// `None` means no reply.
pub async fn dispatch(
    hub: &Hub,
    me: &str,
    req: Req,
) -> Res<Option<Value>> {
    let reply = match req {
        Req::Hello { .. } => Err(BusErr::bad_request("already authenticated")),
        Req::Projects => projects(hub).await,
        Req::AddProject { project } => add_project(hub, &project).await,
        Req::Ping => Ok(json!({"op": "pong"})),
        Req::List => gated(hub, me, list(hub, me)).await,
        Req::Topics => gated(hub, me, topics(hub, me)).await,
        Req::Sub { topic } => gated(hub, me, sub(hub, me, &topic, true)).await,
        Req::Unsub { topic } => gated(hub, me, sub(hub, me, &topic, false)).await,
        Req::Send(r) => gated(hub, me, send(hub, me, r)).await,
        Req::Status { ids } => gated(hub, me, status(hub, me, ids)).await,
        Req::Ack { id } => {
            hub.ledger.ack(me, id);
            return Ok(None);
        }
    };
    reply.map(Some)
}

/// Runs `op` only if `me`'s project is registered. Plain names (no project) are
/// exempt.
async fn gated(
    hub: &Hub,
    me: &str,
    op: impl Future<Output = Res<Value>>,
) -> Res<Value> {
    require_project(hub, me).await?;
    op.await
}

/// Why a known key holder cannot sign in as `name`: its project is not
/// registered (a typo, usually), or the key's entries do not cover it.
pub async fn refusal(
    hub: &Hub,
    name: &str,
) -> BusErr {
    match require_project(hub, name).await {
        Err(e) => e,
        Ok(()) => BusErr::new(
            "auth",
            format!("this key may not sign in as \"{name}\" (its keys-file entries do not cover that name)"),
        ),
    }
}

pub async fn require_project(
    hub: &Hub,
    me: &str,
) -> Res<()> {
    let ns = db::namespace(me).to_string();
    if ns.is_empty() {
        return Ok(());
    }
    let known = hub.db.read(|c| db::projects(c)).await?;
    if known.iter().any(|(n, _)| *n == ns) {
        return Ok(());
    }
    let list = if known.is_empty() {
        "none".to_string()
    } else {
        known.into_iter().map(|(n, _)| n).collect::<Vec<_>>().join(", ")
    };
    Err(BusErr::new(
        "unknown_project",
        format!(
            "project \"{ns}\" is not registered. Registered projects: {list}. A project is registered once, on purpose; do not register one to work around this error."
        ),
    ))
}

async fn projects(hub: &Hub) -> Res<Value> {
    let rows = hub.db.read(|c| db::projects(c)).await?;
    let projects: Vec<Value> = rows.into_iter().map(|(n, a)| json!({"name": n, "agents": a})).collect();
    Ok(json!({"op": "projects", "projects": projects}))
}

async fn add_project(
    hub: &Hub,
    project: &str,
) -> Res<Value> {
    if project.is_empty() || project.len() > 64 || project.contains(|c: char| c.is_whitespace() || c == '/' || c == '#')
    {
        return Err(BusErr::bad_request("a project name is 1..64 characters without spaces, '/' or '#'"));
    }
    let p = project.to_string();
    let created = hub.db.run(move |c| db::add_project(c, &p)).await?;
    Ok(json!({"op": "project", "name": project, "created": created}))
}

async fn list(
    hub: &Hub,
    me: &str,
) -> Res<Value> {
    hub.apply_acks().await; // a just-acked message must not count as unread
    let ns = db::namespace(me).to_string();
    let rows = hub.db.read(move |c| db::agents(c, &ns)).await?;
    let agents: Vec<Value> = rows
        .into_iter()
        .map(|(n, t, u)| json!({"name": n, "online": hub.listeners.online(&n), "last_seen": t, "unread": u}))
        .collect();
    Ok(json!({"op": "agents", "agents": agents}))
}

async fn topics(
    hub: &Hub,
    me: &str,
) -> Res<Value> {
    let ns = db::namespace(me).to_string();
    let rows = hub.db.read(move |c| db::topics(c, &ns)).await?;
    let topics: Vec<Value> = rows.into_iter().map(|(t, n)| json!({"topic": t, "subscribers": n})).collect();
    Ok(json!({"op": "topics", "topics": topics}))
}

async fn sub(
    hub: &Hub,
    me: &str,
    topic: &str,
    join: bool,
) -> Res<Value> {
    let topic = topic.strip_prefix('#').unwrap_or(topic).to_string();
    if topic.is_empty() || topic.len() > 64 || topic.contains(char::is_whitespace) {
        return Err(BusErr::bad_request("bad topic"));
    }
    let me = me.to_string();
    let change = if join { db::subscribe } else { db::unsubscribe };
    hub.db.run(move |c| change(c, &topic, &me)).await?;
    Ok(json!({"op": "ok"}))
}

async fn send(
    hub: &Hub,
    me: &str,
    SendReq { to, body, mut meta }: SendReq,
) -> Res<Value> {
    if to.is_empty() || body.is_empty() || body.len() > MAX_BODY {
        return Err(BusErr::bad_request("need to and a body of 1..1MiB"));
    }
    meta.validate()?;
    let urgent = meta.priority.is_some();
    hub.limits
        .take(me, urgent)
        .map_err(
            |ms| BusErr {
                code: "rate_limited",
                msg: format!("rate limited ({}); retry after {ms} ms", if urgent { "urgent sends" } else { "sends" }),
                retry_ms: Some(ms),
            },
        )?;

    // Pending acks ride along: they cost no commit of their own.
    let acks = hub.ledger.snapshot();
    let (from, a) = (me.to_string(), acks.clone());
    let recipients = hub.db.run(move |c| db::enqueue(c, &from, &to, &body, &meta, &a)).await?;
    hub.ledger.settle(&acks);

    recipients.iter().for_each(|(name, _)| hub.listeners.notify(name));
    let ids: Vec<i64> = recipients.iter().map(|(_, id)| *id).collect();
    let who: Vec<Value> = recipients
        .iter()
        .map(|(name, id)| json!({"name": name, "id": id, "online": hub.listeners.online(name)}))
        .collect();
    Ok(json!({"op": "sent", "delivered": recipients.len(), "ids": ids, "recipients": who}))
}

async fn status(
    hub: &Hub,
    me: &str,
    ids: Vec<i64>,
) -> Res<Value> {
    if ids.len() > MAX_STATUS_IDS {
        return Err(BusErr::bad_request("at most 200 ids"));
    }
    let (from, asked) = (me.to_string(), ids.clone());
    let rows = hub.db.read(move |c| db::statuses(c, &from, &asked)).await?;
    let known: Vec<(&str, i64, bool)> = rows.iter().map(|(id, to, read)| (to.as_str(), *id, read.is_some())).collect();
    let by_id: HashMap<i64, (&str, &str)> =
        known.iter().zip(hub.ledger.states(&known)).map(|((to, id, _), s)| (*id, (*to, s.as_str()))).collect();
    let messages: Vec<Value> = ids
        .iter()
        .map(|id| match by_id.get(id) {
            Some((to, state)) => json!({"id": id, "to": to, "state": state}),
            None => json!({"id": id, "state": "unknown"}),
        })
        .collect();
    Ok(json!({"op": "status", "messages": messages}))
}
