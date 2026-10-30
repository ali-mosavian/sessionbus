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
    let v: Value = serde_json::from_str(json)?;
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

pub fn find_cli() -> Option<String> {
    ["tailscale", "/Applications/Tailscale.app/Contents/MacOS/Tailscale", "/usr/bin/tailscale", "/usr/local/bin/tailscale"]
        .into_iter()
        .find(|c| std::process::Command::new(c).arg("version").output().is_ok_and(|o| o.status.success()))
        .map(String::from)
}

pub fn status(cli: &str) -> Res<Status> {
    let out = std::process::Command::new(cli).args(["status", "--json"]).output()?;
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
