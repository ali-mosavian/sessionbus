//! Per-host agent. It owns the inbox of the sessions on its host, keeps a
//! durable outbox for messages to other hosts and pushes them until acked.
//! Sessions talk to it over a unix socket; agents talk to each other over TCP
//! (reached through Tailscale), so no host is a hub.
mod api;
mod peer;
pub mod store;

use std::{
    collections::HashMap,
    io::Read,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::Connection;
use serde_json::{Value, json};
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{Notify, watch},
    task::JoinHandle,
};

use crate::{
    db::{BusErr, Res, now},
    lock,
};

/// Decides whether a peer connection from this address may talk to the agent.
pub type PeerAuth = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

pub struct Config {
    pub host: String,
    pub dir: PathBuf,
    pub sock: PathBuf,
    pub listen: String,
    /// Address other agents can reach this one at, told to them in the directory.
    pub advertise: Option<String>,
    pub peers: Vec<(String, String)>,
    pub auth: PeerAuth,
    /// Tests only: lets a local client name its session instead of being identified by process.
    pub trust_clients: bool,
    pub default_ttl: i64,
    pub max_ttl: i64,
    pub retry: Duration,
}

impl Config {
    pub fn new(
        host: &str,
        dir: PathBuf,
    ) -> Self {
        Config {
            host: host.into(),
            sock: dir.join("agent.sock"),
            dir,
            listen: "127.0.0.1:8788".into(),
            advertise: None,
            peers: vec![],
            auth: Arc::new(|_| true),
            trust_clients: false,
            default_ttl: 3600,
            max_ttl: 86400,
            retry: Duration::from_secs(3),
        }
    }
}

pub struct Agent {
    pub cfg: Config,
    db: Mutex<Connection>,
    peers: Mutex<HashMap<String, String>>,
    kick: Notify,
    inbox: watch::Sender<u64>,
    version: AtomicI64,
}

/// A running agent; dropping it stops its tasks.
pub struct Running {
    pub agent: Arc<Agent>,
    pub addr: SocketAddr,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.tasks.iter().for_each(|t| t.abort());
    }
}

pub async fn start(cfg: Config) -> Res<Running> {
    std::fs::create_dir_all(&cfg.dir)?;
    let db = store::open(&cfg.dir.join("agent.db"))?;
    let tcp = TcpListener::bind(&cfg.listen).await?;
    let addr = tcp.local_addr()?;
    let _ = std::fs::remove_file(&cfg.sock);
    let unix = UnixListener::bind(&cfg.sock)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&cfg.sock, std::fs::Permissions::from_mode(0o600))?;
    }
    let peers = cfg.peers.iter().cloned().collect();
    let agent = Arc::new(Agent {
        cfg,
        db: Mutex::new(db),
        peers: Mutex::new(peers),
        kick: Notify::new(),
        inbox: watch::channel(0).0,
        version: AtomicI64::new(0),
    });
    let tasks = vec![
        tokio::spawn(peer::accept(agent.clone(), tcp)),
        tokio::spawn(peer::pump(agent.clone())),
        tokio::spawn(api::accept(agent.clone(), unix)),
    ];
    Ok(Running { agent, addr, tasks })
}

impl Agent {
    pub(crate) fn db(&self) -> std::sync::MutexGuard<'_, Connection> {
        lock(&self.db)
    }

    pub fn set_peer(
        &self,
        host: &str,
        addr: &str,
    ) {
        lock(&self.peers).insert(host.into(), addr.into());
        self.kick.notify_one();
    }

    pub(crate) fn peer_addr(
        &self,
        host: &str,
    ) -> Option<String> {
        lock(&self.peers).get(host).cloned()
    }

    pub(crate) fn peer_hosts(&self) -> Vec<String> {
        lock(&self.peers).keys().cloned().collect()
    }

    pub(crate) fn wake_inbox(&self) {
        self.inbox.send_modify(|n| *n += 1);
    }

    /// What this host tells others: its sessions, aliases, subscriptions and projects.
    pub(crate) fn record(&self) -> Res<Value> {
        let db = self.db();
        let ms = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64);
        let version = self.version.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |p| Some((p + 1).max(ms))).unwrap_or(ms);
        let version = (version + 1).max(ms);
        Ok(json!({
            "host": self.cfg.host,
            "version": version,
            "addr": self.cfg.advertise,
            "sessions": store::sessions(&db)?,
            "aliases": store::local_aliases(&db)?,
            "subs": store::local_subs(&db)?,
            "projects": store::projects(&db)?,
        }))
    }

    /// Queues the current record for every known peer; call after local state changes.
    pub(crate) fn publish(&self) -> Res<()> {
        let rec = self.record()?.to_string();
        let db = self.db();
        for h in self.peer_hosts() {
            store::queue_wire(&db, "dir", "dir", &h, &rec, now() + self.cfg.max_ttl)?;
        }
        drop(db);
        self.kick.notify_one();
        Ok(())
    }

    /// Records from other hosts, plus where to reach them when they said so.
    pub(crate) fn learn(
        &self,
        rec: &Value,
    ) -> Res<()> {
        let (Some(host), Some(version)) = (rec["host"].as_str(), rec["version"].as_i64()) else {
            return Err(BusErr::bad_request("bad directory record"));
        };
        if host == self.cfg.host {
            return Ok(());
        }
        store::put_dir(&self.db(), host, version, rec)?;
        if let Some(a) = rec["addr"].as_str() {
            lock(&self.peers).entry(host.into()).or_insert_with(|| a.into());
        }
        Ok(())
    }
}

pub fn uuid7() -> String {
    let ms = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64);
    let mut r = [0u8; 10];
    let _ = std::fs::File::open("/dev/urandom").and_then(|mut f| f.read_exact(&mut r));
    let mut b = [0u8; 16];
    b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    b[6..].copy_from_slice(&r[..10]);
    b[6] = 0x70 | (b[6] & 0x0f);
    b[8] = 0x80 | (b[8] & 0x3f);
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

/// `pid` followed by its ancestors, nearest first, with each process's command name.
pub fn ancestry(pid: i64) -> Vec<(i64, String)> {
    let out = std::process::Command::new("ps").args(["-axo", "pid=,ppid=,comm="]).output();
    let Ok(out) = out else { return vec![] };
    let mut procs: HashMap<i64, (i64, String)> = HashMap::new();
    for l in String::from_utf8_lossy(&out.stdout).lines() {
        let mut it = l.split_whitespace();
        if let (Some(p), Some(pp)) = (it.next().and_then(|x| x.parse().ok()), it.next().and_then(|x| x.parse().ok())) {
            procs.insert(p, (pp, it.collect::<Vec<_>>().join(" ")));
        }
    }
    let mut chain = vec![];
    let mut cur = pid;
    while let Some((pp, comm)) = procs.get(&cur) {
        chain.push((cur, comm.clone()));
        if *pp <= 1 || chain.len() > 64 || chain.iter().any(|(p, _)| p == pp) {
            break;
        }
        cur = *pp;
    }
    chain
}
