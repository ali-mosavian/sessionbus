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
    time::{Instant, MissedTickBehavior, interval, timeout, timeout_at},
};

pub use self::auth::{NAMESPACE, check_keys_perms, signed_payload};
use self::{
    proto::{Req, err_frame, msg_frame, put},
    state::{DEFAULT_LIMIT, Ledger, Listeners, RateLimit},
};
use crate::db::{self, BusErr, Db, Msg};

const BATCH: i64 = 500;
const DAY: u64 = 24 * 3600;
const DEFAULT_IDLE: Duration = Duration::from_secs(45);
/// For connections that may stay silent (not listening).
const NEVER: Duration = Duration::from_secs(365 * 24 * 3600);
/// How long an ack may wait in memory when no send is committing.
const ACK_INTERVAL: Duration = Duration::from_millis(100);

pub struct Hub {
    db: Db,
    keys: PathBuf,
    listeners: Listeners,
    ledger: Ledger,
    limits: RateLimit,
    settings: Settings,
}

/// The relay's tunables.
#[derive(Clone, Debug)]
pub struct Settings {
    /// `(burst, per second)` of normal sends per sender.
    pub limit: (f64, f64),
    /// A listening connection that sends nothing for this long is dropped, so
    /// a vanished peer does not hold its name.
    pub idle: Duration,
    /// Read messages are kept (without content) for this long, so senders can
    /// still ask what became of them.
    pub read_ttl: Duration,
    /// Unread messages, content and all, are dropped after this long.
    pub unread_ttl: Duration,
    /// How often expired messages are purged.
    pub purge_every: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limit: DEFAULT_LIMIT,
            idle: DEFAULT_IDLE,
            read_ttl: Duration::from_secs(7 * DAY),
            unread_ttl: Duration::from_secs(30 * DAY),
            purge_every: Duration::from_secs(600),
        }
    }
}

impl Hub {
    pub fn new(
        db: Db,
        authorized_keys: PathBuf,
    ) -> Arc<Hub> {
        Hub::with_settings(db, authorized_keys, Settings::default())
    }

    pub fn with_settings(
        db: Db,
        authorized_keys: PathBuf,
        settings: Settings,
    ) -> Arc<Hub> {
        Arc::new(Hub {
            db,
            keys: authorized_keys,
            listeners: Listeners::default(),
            ledger: Ledger::default(),
            limits: RateLimit::new(settings.limit),
            settings,
        })
    }

    /// Drops messages past their ttl, content included.
    async fn purge(&self) {
        let now = db::now();
        let (read_before, unread_before) =
            (now - self.settings.read_ttl.as_secs() as i64, now - self.settings.unread_ttl.as_secs() as i64);
        let _ = self
            .db
            .run(move |c| {
                db::purge(c, read_before, unread_before)?;
                db::compact(c);
                Ok(())
            })
            .await;
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

/// Applies pending acks every `ACK_INTERVAL` and purges expired messages every
/// `purge_every`, until the hub is dropped.
async fn flush_acks(hub: Weak<Hub>) {
    let mut tick = interval(ACK_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_purge = Instant::now();
    loop {
        tick.tick().await;
        let Some(h) = hub.upgrade() else { return };
        h.apply_acks().await;
        if last_purge.elapsed() >= h.settings.purge_every {
            last_purge = Instant::now();
            h.purge().await;
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
    let registration = if listen {
        match hub.listeners.register(&name, wake.clone()) {
            Ok(r) => Some(r),
            Err(_) => {
                let taken = BusErr::new("name_in_use", "a listener for this name is already connected");
                return put(&mut wr, &err_frame(&taken)).await;
            }
        }
    } else {
        None
    };
    let res = session(hub, &name, listen, br, &mut wr, &wake).await;
    drop(registration);
    if listen && !hub.listeners.online(&name) {
        hub.ledger.forget_pushed(&name);
    }
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
    // A silent listener is dropped; any line, a ping included, resets the
    // clock.
    let idle = if listen { hub.settings.idle } else { NEVER };
    let mut deadline = Instant::now() + idle;
    loop {
        tokio::select! {
            line = timeout_at(deadline, lines.next_line()) => {
                let Ok(line) = line else { return Ok(()) };
                deadline = Instant::now() + idle;
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
