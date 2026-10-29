//! In-process load test: `cargo run --release --example load -- [listeners]
//! [publishers] [msgs-per-publisher]`. Publishers send to `#load`; every
//! listener acks every message.
use std::{
    env,
    time::{Duration, Instant},
};

use serde_json::json;
use sessionbus::{
    client::Client,
    db::Db,
    server::{Hub, Settings, run},
};
use ssh_key::{Algorithm, PrivateKey, rand_core::OsRng};
use tokio::net::TcpListener;

fn pct(
    v: &[Duration],
    p: f64,
) -> Duration {
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
}

#[tokio::main]
async fn main() {
    let arg = |i: usize, d: usize| env::args().nth(i).map_or(d, |a| a.parse().unwrap());
    let (nl, np, m) = (arg(1, 20), arg(2, 4), arg(3, 100));
    let dir = env::temp_dir().join(format!("sessionbus-load-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let names: Vec<String> = (0..nl).map(|i| format!("l{i}")).chain((0..np).map(|i| format!("p{i}"))).collect();
    let keys: Vec<PrivateKey> =
        names.iter().map(|_| PrivateKey::random(&mut OsRng, Algorithm::Ed25519).unwrap()).collect();
    let auth: String =
        names.iter().zip(&keys).map(|(n, k)| format!("{n} {}\n", k.public_key().to_openssh().unwrap())).collect();
    std::fs::write(dir.join("authorized_keys"), auth).unwrap();

    let hub = Hub::with_settings(
        Db::open(&dir.join("bus.db")).unwrap(),
        dir.join("authorized_keys"),
        Settings { limit: (1e9, 1e9), ..Settings::default() },
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(run(l, hub));

    let total = np * m;
    let mut listeners = Vec::new();
    for i in 0..nl {
        let mut ctl = Client::connect(&addr, &names[i], &keys[i], false).await.unwrap();
        ctl.call(json!({"op": "sub", "topic": "load"})).await.unwrap();
        let mut c = Client::connect(&addr, &names[i], &keys[i], true).await.unwrap();
        listeners.push(tokio::spawn(async move {
            let mut seen = 0;
            while seen < total {
                let f = c.next().await.unwrap().expect("closed");
                if f["op"] == "msg" {
                    seen += 1;
                    c.ack(f["id"].as_i64().unwrap()).await.unwrap();
                }
            }
        }));
    }

    let t0 = Instant::now();
    let mut pubs = Vec::new();
    for i in 0..np {
        let mut c = Client::connect(&addr, &names[nl + i], &keys[nl + i], false).await.unwrap();
        pubs.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(m);
            for j in 0..m {
                let t = Instant::now();
                c.call(json!({"op": "send", "to": "#load", "body": format!("m{j}")})).await.unwrap();
                lat.push(t.elapsed());
            }
            lat
        }));
    }
    let mut lat: Vec<Duration> = Vec::new();
    for p in pubs {
        lat.extend(p.await.unwrap());
    }
    let sent = t0.elapsed();
    for l in listeners {
        l.await.unwrap();
    }
    let all = t0.elapsed().as_secs_f64();
    lat.sort();
    println!(
        "{nl} listeners x {np} pubs x {m} msgs: {:.0} sends/s, {:.0} deliveries/s, send p50 {:?} p99 {:?}, sends done {:.2?}, all delivered {:.2}s",
        total as f64 / sent.as_secs_f64(),
        (total * nl) as f64 / all,
        pct(&lat, 0.5),
        pct(&lat, 0.99),
        sent,
        all,
    );
    let _ = std::fs::remove_dir_all(&dir);
}
