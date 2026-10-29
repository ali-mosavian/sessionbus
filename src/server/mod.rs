//! The relay: accepts connections, authenticates them, serves their requests.
mod auth;
mod ops;
mod proto;
mod state;

use std::io;
use std::{
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};

use serde_json::json;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, BufReader, WriteHalf},
    net::TcpListener,
    sync::Notify,
    time::{MissedTickBehavior, interval, timeout},
};

pub use self::auth::{NAMESPACE, signed_payload};
use self::{
    proto::{Req, err_frame, msg_frame, put},
    state::{DEFAULT_LIMIT, Ledger, Listeners, RateLimit},
};
use crate::db::{self, BusErr, Db, Msg};

const BATCH: i64 = 500;
/// How long an ack may wait in memory when no send is committing.
const ACK_INTERVAL: Duration = Duration::from_millis(100);
/// Read messages are kept (without content) so senders can check on them.
const READ_TTL_SECS: i64 = 7 * 24 * 3600;
const PURGE_EVERY: u32 = 6000; // ticks: ten minutes

pub struct Hub {
    db: Db,
    keys: PathBuf,
    listeners: Listeners,
    ledger: Ledger,
    limits: RateLimit,
}

impl Hub {
    pub fn new(
        db: Db,
        authorized_keys: PathBuf,
    ) -> Arc<Hub> {
        let (burst, per_sec) = DEFAULT_LIMIT;
        Hub::with_limit(db, authorized_keys, burst, per_sec)
    }

    /// `burst` sends at once per sender, refilled at `per_sec`.
    pub fn with_limit(
        db: Db,
        authorized_keys: PathBuf,
        burst: f64,
        per_sec: f64,
    ) -> Arc<Hub> {
        Arc::new(Hub {
            db,
            keys: authorized_keys,
            listeners: Listeners::default(),
            ledger: Ledger::default(),
            limits: RateLimit::new((burst, per_sec)),
        })
    }

    /// Commits the acks received so far.
    pub async fn apply_acks(&self) {
        let acks = self.ledger.snapshot();
        if acks.is_empty() {
            return;
        }
        let a = acks.clone();
        if self.db.run(move |c| db::apply_acks(c, &a)).await.is_ok() {
            self.ledger.settle(&acks);
        }
    }
}

/// Applies pending acks every `ACK_INTERVAL`, until the hub is dropped.
async fn flush_acks(hub: Weak<Hub>) {
    let mut tick = interval(ACK_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut ticks = 0u32;
    loop {
        tick.tick().await;
        let Some(h) = hub.upgrade() else { return };
        h.apply_acks().await;
        ticks += 1;
        if ticks % PURGE_EVERY == 0 {
            let _ = h.db.run(|c| db::purge_read(c, db::now() - READ_TTL_SECS)).await;
        }
    }
}

pub async fn run(
    listener: TcpListener,
    hub: Arc<Hub>,
) {
    tokio::spawn(flush_acks(Arc::downgrade(&hub)));
    loop {
        match listener.accept().await {
            Ok((sock, _)) => {
                tokio::spawn(serve_conn(hub.clone(), sock));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

pub async fn serve_conn<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    hub: Arc<Hub>,
    s: S,
) {
    let _ = handle(&hub, s).await;
}

async fn handle<S: AsyncRead + AsyncWrite + Unpin>(
    hub: &Arc<Hub>,
    s: S,
) -> io::Result<()> {
    let (rd, mut wr) = tokio::io::split(s);
    let mut br = BufReader::new(rd);
    let nonce = auth::nonce()?;
    put(&mut wr, &json!({"op": "challenge", "nonce": nonce})).await?;

    // Unauthenticated peers get a small, time-boxed first line.
    let mut first = String::new();
    let n = timeout(Duration::from_secs(10), (&mut br).take(8192).read_line(&mut first))
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
    if n == 0 {
        return Ok(());
    }
    let (name, listen) = match serde_json::from_str::<Req>(&first) {
        Ok(Req::Hello { name, sig, listen })
            if auth::valid_name(&name) && auth::verify(&hub.keys, &name, &nonce, &sig) =>
        {
            (name, listen)
        }
        _ => return put(&mut wr, &err_frame(&BusErr::new("auth", "auth failed"))).await,
    };

    let wake = Arc::new(Notify::new());
    let registration = listen.then(|| hub.listeners.register(&name, wake.clone()));
    let res = session(hub, &name, listen, br, &mut wr, &wake).await;
    drop(registration);
    let n = name.clone();
    let _ = hub.db.run(move |c| db::touch(c, &n)).await;
    res
}

async fn session<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    hub: &Hub,
    name: &str,
    listen: bool,
    br: BufReader<R>,
    wr: &mut WriteHalf<W>,
    wake: &Notify,
) -> io::Result<()> {
    let n = name.to_string();
    if let Err(e) = hub.db.run(move |c| db::touch(c, &n)).await {
        return put(wr, &err_frame(&e)).await;
    }
    put(wr, &json!({"op": "ok"})).await?;

    let mut lines = br.lines();
    let mut cursor = 0;
    if listen {
        // a reconnect must not see what it already acked
        hub.apply_acks().await;
        cursor = flush(hub, name, cursor, wr).await?;
        put(wr, &json!({"op": "ready"})).await?;
    }
    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { return Ok(()) };
                let reply = match serde_json::from_str::<Req>(&line) {
                    Ok(req) => ops::dispatch(hub, name, req).await,
                    Err(e) => Err(BusErr::bad_request(e.to_string())),
                };
                match reply {
                    Ok(Some(v)) => put(wr, &v).await?,
                    Ok(None) => {}
                    Err(e) => put(wr, &err_frame(&e)).await?,
                }
            }
            _ = wake.notified(), if listen => cursor = flush(hub, name, cursor, wr).await?,
        }
    }
}

/// Sends every queued message after `cursor`; messages stay queued until the
/// client acks.
async fn flush<W: AsyncWrite + Unpin>(
    hub: &Hub,
    name: &str,
    mut cursor: i64,
    wr: &mut W,
) -> io::Result<i64> {
    loop {
        let n = name.to_string();
        let msgs: Vec<Msg> = match hub.db.read(move |c| db::pending(c, &n, cursor, BATCH)).await {
            Ok(m) => m,
            Err(e) => {
                put(wr, &err_frame(&e)).await?;
                return Ok(cursor);
            }
        };
        for m in &msgs {
            put(wr, &msg_frame(m)).await?;
            cursor = m.id;
        }
        hub.ledger.pushed(name, msgs.iter().map(|m| m.id));
        if (msgs.len() as i64) < BATCH {
            return Ok(cursor);
        }
    }
}
