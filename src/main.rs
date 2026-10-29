//! Env: SESSIONBUS_NAME, SESSIONBUS_ADDR (default 127.0.0.1:8787), SESSIONBUS_KEY (default ~/.ssh/id_ed25519).
use serde_json::{Value, json};
use sessionbus::{
    client::{Client, load_key},
    db::Db,
    server::{Hub, run},
};
use std::{env, path::PathBuf, process::exit, time::Duration};
use tokio::{net::TcpListener, time::Instant};

const USAGE: &str = "usage: sessionbus serve [--addr A] [--db F] [--keys F]
       sessionbus list | topics | sub <topic> | unsub <topic> | send <to|#topic> <message> | inbox [--wait SECS]";

fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("sessionbus: {msg}");
    exit(1)
}

fn opt(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let i = args.iter().position(|a| a == flag)?;
    if i + 1 >= args.len() {
        die(format!("{flag} needs a value"));
    }
    args.remove(i);
    Some(args.remove(i))
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("{USAGE}");
        exit(2);
    }
    let cmd = args.remove(0);
    if cmd == "serve" {
        let addr = opt(&mut args, "--addr").unwrap_or("127.0.0.1:8787".into());
        let db = opt(&mut args, "--db").unwrap_or("sessionbus.db".into());
        let keys = opt(&mut args, "--keys").unwrap_or("authorized_keys".into());
        let db = Db::open(db.as_ref()).unwrap_or_else(|e| die(e));
        let l = TcpListener::bind(&addr).await.unwrap_or_else(|e| die(format!("{addr}: {e}")));
        run(l, Hub::new(db, keys.into())).await;
        return;
    }
    if let Err(e) = client_cmd(&cmd, args).await {
        die(e);
    }
}

async fn client_cmd(cmd: &str, mut args: Vec<String>) -> Result<(), String> {
    let name = env::var("SESSIONBUS_NAME").map_err(|_| "SESSIONBUS_NAME not set")?;
    let addr = env::var("SESSIONBUS_ADDR").unwrap_or("127.0.0.1:8787".into());
    let key_path = env::var("SESSIONBUS_KEY").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(env::var("HOME").unwrap_or_default()).join(".ssh/id_ed25519")
    });
    let key = load_key(&key_path)?;

    if cmd == "inbox" {
        let wait: u64 = opt(&mut args, "--wait").map_or(Ok(0), |w| w.parse()).map_err(|_| "bad --wait")?;
        let mut c = Client::connect(&addr, &name, &key, true).await?;
        let mut deadline = None;
        loop {
            let frame = match deadline {
                None => c.next().await?,
                Some(d) => match tokio::time::timeout_at(d, c.next()).await {
                    Ok(f) => f?,
                    Err(_) => return Ok(()),
                },
            };
            let Some(v) = frame else { return Ok(()) };
            match v["op"].as_str() {
                Some("msg") => {
                    println!("{v}");
                    c.ack(v["id"].as_i64().unwrap_or(0)).await?;
                }
                Some("ready") => {
                    if wait == 0 {
                        return Ok(());
                    }
                    deadline = Some(Instant::now() + Duration::from_secs(wait));
                }
                Some("error") => return Err(v["error"].as_str().unwrap_or("error").into()),
                _ => {}
            }
        }
    }

    let req: Value = match (cmd, args.as_slice()) {
        ("list", []) => json!({"op": "list"}),
        ("topics", []) => json!({"op": "topics"}),
        ("sub", [t]) => json!({"op": "sub", "topic": t}),
        ("unsub", [t]) => json!({"op": "unsub", "topic": t}),
        ("send", [to, msg @ ..]) if !msg.is_empty() => json!({"op": "send", "to": to, "body": msg.join(" ")}),
        _ => return Err(USAGE.into()),
    };
    let mut c = Client::connect(&addr, &name, &key, false).await?;
    println!("{}", c.call(req).await?);
    Ok(())
}
