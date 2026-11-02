//! Peer discovery and authentication through Tailscale: every device the same
//! user owns is a possible peer, and only those addresses may connect. The
//! tailnet already encrypts and authenticates the transport (WireGuard).
use std::{
    collections::HashSet,
    net::IpAddr,
    sync::{Arc, RwLock},
    time::Duration,
};

use serde_json::Value;

use super::{Agent, PeerAuth};
use crate::db::{BusErr, Res};

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    /// First label of the MagicDNS name: plain, unique in the tailnet.
    pub host: String,
    pub ips: Vec<IpAddr>,
    pub online: bool,
}

#[derive(Debug)]
pub struct Status {
    pub me: Node,
    /// Other devices of the same user.
    pub peers: Vec<Node>,
}

fn node(v: &Value) -> Option<Node> {
    let label = |s: &str| s.trim_end_matches('.').split('.').next().map(str::to_lowercase);
    let host = v["DNSName"].as_str().and_then(label).or_else(|| v["HostName"].as_str().and_then(label))?;
    let ips = v["TailscaleIPs"].as_array()?.iter().filter_map(|i| i.as_str()?.parse().ok()).collect();
    Some(Node { host, ips, online: v["Online"].as_bool().unwrap_or(false) })
}

pub fn parse(json: &str) -> Res<Status> {
    let v: Value = serde_json::from_str(json).map_err(|_| {
        let head: String = json.trim().chars().take(120).collect();
        BusErr::new("tailscale", format!("`tailscale status --json` did not print JSON: {head}"))
    })?;
    if v["BackendState"] != "Running" {
        return Err(BusErr::new("tailscale", format!("tailscale is {}, not Running", v["BackendState"])));
    }
    let me = node(&v["Self"]).ok_or_else(|| BusErr::new("tailscale", "tailscale status has no usable Self"))?;
    let owner = &v["Self"]["UserID"];
    let peers = v["Peer"]
        .as_object()
        .into_iter()
        .flat_map(|m| m.values())
        .filter(|p| &p["UserID"] == owner)
        .filter_map(node)
        .collect();
    Ok(Status { me, peers })
}

/// How long a `tailscale` subprocess may take: the macOS app binary can hang, and the agent must still start.
const CLI_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs `cli args` as the Tailscale CLI, killing it after `limit`. The macOS app's binary is a GUI
/// unless told to act as the CLI (launchd starts us without a terminal), and then it may never exit.
fn run(
    cli: &str,
    args: &[&str],
    limit: Duration,
) -> Res<std::process::Output> {
    use std::process::{Command, Stdio};
    let mut child = Command::new(cli)
        .env("TAILSCALE_BE_CLI", "1")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let (out, err) = (child.stdout.take(), child.stderr.take());
    fn read<R: std::io::Read + Send + 'static>(r: Option<R>) -> std::thread::JoinHandle<Vec<u8>> {
        std::thread::spawn(move || {
            let mut v = Vec::new();
            if let Some(mut r) = r {
                let _ = r.read_to_end(&mut v);
            }
            v
        })
    }
    let (out, err) = (read(out), read(err));
    let start = std::time::Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if start.elapsed() > limit {
            let _ = child.kill();
            let _ = child.wait();
            return Err(BusErr::new("tailscale", format!("`{cli} {}` did not finish in {}s", args.join(" "), limit.as_secs())));
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    Ok(std::process::Output { status, stdout: out.join().unwrap_or_default(), stderr: err.join().unwrap_or_default() })
}

pub fn find_cli() -> Option<String> {
    find_in(&["tailscale", "/Applications/Tailscale.app/Contents/MacOS/Tailscale", "/usr/bin/tailscale", "/usr/local/bin/tailscale"], CLI_TIMEOUT)
}

/// The first of `candidates` that answers `version` within `limit`.
pub fn find_in(
    candidates: &[&str],
    limit: Duration,
) -> Option<String> {
    candidates.iter().find(|c| run(c, &["version"], limit).is_ok_and(|o| o.status.success())).map(|c| c.to_string())
}

pub fn status(cli: &str) -> Res<Status> {
    let out = run(cli, &["status", "--json"], CLI_TIMEOUT)?;
    if !out.status.success() {
        return Err(BusErr::new("tailscale", String::from_utf8_lossy(&out.stderr).trim().to_string()));
    }
    parse(&String::from_utf8_lossy(&out.stdout))
}

/// The tailnet address agents listen on, and the filter that admits only the user's own devices.
pub fn auth(st: &Status) -> (PeerAuth, Arc<RwLock<HashSet<IpAddr>>>) {
    let allowed: Arc<RwLock<HashSet<IpAddr>>> = Arc::new(RwLock::new(st.peers.iter().flat_map(|p| p.ips.clone()).collect()));
    let a = allowed.clone();
    (Arc::new(move |ip| ip.is_loopback() || a.read().is_ok_and(|s| s.contains(&ip))), allowed)
}

/// Keeps the allowed set and the agent's peer addresses in step with the tailnet.
pub fn watch(
    agent: Arc<Agent>,
    cli: String,
    allowed: Arc<RwLock<HashSet<IpAddr>>>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(20)).await;
            let cli = cli.clone();
            let Ok(Ok(st)) = tokio::task::spawn_blocking(move || status(&cli)).await else { continue };
            let mut set = HashSet::new();
            for p in &st.peers {
                set.extend(p.ips.iter().copied());
                if let Some(ip) = p.ips.iter().find(|i| i.is_ipv4()) {
                    agent.set_peer(&p.host, &format!("{ip}:{port}"));
                }
            }
            if let Ok(mut a) = allowed.write() {
                *a = set;
            }
        }
    })
}
