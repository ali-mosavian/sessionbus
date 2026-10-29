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
        Req::List => list(hub).await,
        Req::Topics => topics(hub).await,
        Req::Ping => Ok(json!({"op": "pong"})),
        Req::Sub { topic } => sub(hub, me, &topic, true).await,
        Req::Unsub { topic } => sub(hub, me, &topic, false).await,
        Req::Send(r) => send(hub, me, r).await,
        Req::Status { ids } => status(hub, me, ids).await,
        Req::Ack { id } => {
            hub.ledger.ack(me, id);
            return Ok(None);
        }
    };
    reply.map(Some)
}

async fn list(hub: &Hub) -> Res<Value> {
    let rows = hub.db.read(|c| db::agents(c)).await?;
    let agents: Vec<Value> =
        rows.into_iter().map(|(n, t)| json!({"name": n, "online": hub.listeners.online(&n), "last_seen": t})).collect();
    Ok(json!({"op": "agents", "agents": agents}))
}

async fn topics(hub: &Hub) -> Res<Value> {
    let rows = hub.db.read(|c| db::topics(c)).await?;
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
