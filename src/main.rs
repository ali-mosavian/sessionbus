//! Env: SESSIONBUS_NAME, SESSIONBUS_ADDR (default 127.0.0.1:8787),
//! SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
use std::{env, process::exit, time::Duration};

use serde_json::{Value, json};
use sessionbus::{
    client::Config,
    db::Db,
    server::{Hub, Settings, run},
};
use tokio::net::TcpListener;

const USAGE: &str = "usage: sessionbus serve [--addr A] [--db F] [--keys F] [--burst N] [--rate PER_SEC] [--unread-ttl-days D]
       sessionbus list | topics | sub <topic> | unsub <topic> | send <to|#topic> <message> [--reply-to ID] [--status done|blocked] [--priority urgent] [--summary TEXT] | status <id>... | inbox [--wait SECS]";

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("sessionbus: {msg}");
    exit(1)
}

fn opt(
    args: &mut Vec<String>,
    flag: &str,
) -> Option<String> {
    let i = args.iter().position(|a| a == flag)?;
    if i + 1 >= args.len() {
        die(format!("{flag} needs a value"));
    }
    args.remove(i);
    Some(args.remove(i))
}

fn home_dir() -> std::path::PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| die("HOME is not set"));
    std::path::PathBuf::from(home).join(".sessionbus")
}

fn hostname() -> String {
    let out = std::process::Command::new("hostname").output().ok();
    let h = out.map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
    h.split('.').next().unwrap_or("").to_lowercase()
}

/// `sessionbus agent [--dir D] [--host H] [--listen A] [--advertise A] [--peer NAME=ADDR]...`
async fn run_agent(mut args: Vec<String>) {
    use sessionbus::agent::{Config, start};
    let dir = opt(&mut args, "--dir").map_or_else(home_dir, Into::into);
    let host = opt(&mut args, "--host").unwrap_or_else(hostname);
    if host.is_empty() {
        die("cannot tell this host's name; pass --host");
    }
    let mut cfg = Config::new(&host, dir);
    if let Some(l) = opt(&mut args, "--listen") {
        cfg.listen = l;
    }
    cfg.advertise = opt(&mut args, "--advertise");
    // for tests: local clients may name their own session
    if let Some(i) = args.iter().position(|a| a == "--trust-clients") {
        args.remove(i);
        cfg.trust_clients = true;
    }
    while let Some(p) = opt(&mut args, "--peer") {
        let (n, a) = p.split_once('=').unwrap_or_else(|| die("--peer wants NAME=ADDR"));
        cfg.peers.push((n.into(), a.into()));
    }
    if !args.is_empty() {
        die(format!("unexpected {args:?}"));
    }
    let run = start(cfg).await.unwrap_or_else(|e| die(e));
    eprintln!("sessionbus agent {host}: peers on {}, sessions on {}", run.addr, run.agent.cfg.sock.display());
    std::future::pending::<()>().await;
}

/// `sessionbus hook` registers the session named on stdin (Claude Code's hook
/// payload); `sessionbus call JSON` sends one request to the local agent.
async fn local_call(
    cmd: &str,
    mut args: Vec<String>,
) {
    use tokio::{io::{AsyncBufReadExt, AsyncWriteExt, BufReader}, net::UnixStream};
    let sock = env::var("SESSIONBUS_SOCK").map_or_else(|_| home_dir().join("agent.sock"), Into::into);
    let req = if cmd == "hook" {
        let mut input = String::new();
        let _ = std::io::Read::read_to_string(&mut std::io::stdin(), &mut input);
        let p: Value = serde_json::from_str(&input).unwrap_or_else(|_| die("hook input is not JSON"));
        let session = p["session_id"].as_str().unwrap_or_else(|| die("hook input has no session_id"));
        json!({"op": "register", "session": session, "cwd": p["cwd"]})
    } else {
        serde_json::from_str(&args.pop().unwrap_or_else(|| die("call wants a JSON request")))
            .unwrap_or_else(|e| die(e))
    };
    let s = UnixStream::connect(&sock).await.unwrap_or_else(|e| die(format!("agent not running at {}: {e}", sock.display())));
    let (r, mut w) = s.into_split();
    w.write_all(format!("{req}\n").as_bytes()).await.unwrap_or_else(|e| die(e));
    let mut line = String::new();
    BufReader::new(r).read_line(&mut line).await.unwrap_or_else(|e| die(e));
    print!("{line}");
    if line.contains("\"error\"") {
        exit(1);
    }
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("{USAGE}");
        exit(2);
    }
    let cmd = args.remove(0);
    if cmd == "agent" {
        return run_agent(args).await;
    }
    if cmd == "hook" || cmd == "call" {
        return local_call(&cmd, args).await;
    }
    if cmd == "serve" {
        let addr = opt(&mut args, "--addr").unwrap_or("127.0.0.1:8787".into());
        let db = opt(&mut args, "--db").unwrap_or("sessionbus.db".into());
        let keys = opt(&mut args, "--keys").unwrap_or("authorized_keys".into());
        sessionbus::server::check_keys_perms(keys.as_ref()).unwrap_or_else(|e| die(e));
        let db = Db::open(db.as_ref()).unwrap_or_else(|e| die(e));
        let l = TcpListener::bind(&addr).await.unwrap_or_else(|e| die(format!("{addr}: {e}")));
        let num =
            |v: Option<String>, default: f64| v.map_or(default, |v| v.parse().unwrap_or_else(|_| die("bad number")));
        let (burst, rate) = (num(opt(&mut args, "--burst"), 100.0), num(opt(&mut args, "--rate"), 20.0));
        let days = num(opt(&mut args, "--unread-ttl-days"), 30.0);
        let unread_ttl = Duration::from_secs_f64(days * 86400.0);
        run(
            l,
            Hub::with_settings(db, keys.into(), Settings { limit: (burst, rate), unread_ttl, ..Settings::default() }),
        )
        .await;
        return;
    }
    if let Err(e) = client_cmd(&cmd, args).await {
        die(e);
    }
}

async fn client_cmd(
    cmd: &str,
    mut args: Vec<String>,
) -> Result<(), String> {
    let cfg = Config::from_env();
    if cmd == "inbox" {
        let wait: u64 = opt(&mut args, "--wait").map_or(Ok(0), |w| w.parse()).map_err(|_| "bad --wait")?;
        let mut c = cfg.connect(true).await?;
        let msgs = c.inbox(Duration::from_secs(wait)).await?;
        for m in &msgs {
            println!("{m}");
        }
        if let Some(last) = msgs.last() {
            c.ack_sync(last["id"].as_i64().unwrap_or(0)).await?;
        }
        return Ok(());
    }

    let mut extra = serde_json::Map::new();
    if cmd == "send" {
        for (flag, key) in
            [("--reply-to", "reply_to"), ("--status", "status"), ("--priority", "priority"), ("--summary", "summary")]
        {
            if let Some(v) = opt(&mut args, flag) {
                let v =
                    if key == "reply_to" { json!(v.parse::<i64>().map_err(|_| "bad --reply-to")?) } else { json!(v) };
                extra.insert(key.into(), v);
            }
        }
    }
    let mut req: Value = match (cmd, args.as_slice()) {
        ("status", ids) if !ids.is_empty() => {
            json!({"op": "status", "ids": ids.iter().map(|i| i.parse::<i64>()).collect::<Result<Vec<_>, _>>().map_err(|_| "ids must be numbers")?})
        }
        ("list", []) => json!({"op": "list"}),
        ("topics", []) => json!({"op": "topics"}),
        ("sub", [t]) => json!({"op": "sub", "topic": t}),
        ("unsub", [t]) => json!({"op": "unsub", "topic": t}),
        ("send", [to, msg @ ..]) if !msg.is_empty() => json!({"op": "send", "to": to, "body": msg.join(" ")}),
        _ => return Err(USAGE.into()),
    };
    req.as_object_mut().expect("object").extend(extra);
    println!("{}", cfg.connect(false).await?.call(req).await?);
    Ok(())
}
