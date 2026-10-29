//! Env: SESSIONBUS_NAME, SESSIONBUS_ADDR (default 127.0.0.1:8787),
//! SESSIONBUS_KEY (default ~/.ssh/id_ed25519). `mcp` serves MCP on stdio.
use std::{env, process::exit, sync::Arc, time::Duration};

use serde_json::{Value, json};
use sessionbus::{
    client::Config,
    db::Db,
    mcp,
    server::{Hub, run},
};
use tokio::net::TcpListener;

const USAGE: &str = "usage: sessionbus serve [--addr A] [--db F] [--keys F]
       sessionbus list | topics | sub <topic> | unsub <topic> | send <to|#topic> <message> | inbox [--wait SECS] | mcp";

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

async fn client_cmd(
    cmd: &str,
    mut args: Vec<String>,
) -> Result<(), String> {
    let cfg = Config::from_env();
    if cmd == "mcp" {
        return mcp::serve(Arc::new(cfg), tokio::io::stdin(), tokio::io::stdout()).await.map_err(|e| e.to_string());
    }
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

    let req: Value = match (cmd, args.as_slice()) {
        ("list", []) => json!({"op": "list"}),
        ("topics", []) => json!({"op": "topics"}),
        ("sub", [t]) => json!({"op": "sub", "topic": t}),
        ("unsub", [t]) => json!({"op": "unsub", "topic": t}),
        ("send", [to, msg @ ..]) if !msg.is_empty() => json!({"op": "send", "to": to, "body": msg.join(" ")}),
        _ => return Err(USAGE.into()),
    };
    println!("{}", cfg.connect(false).await?.call(req).await?);
    Ok(())
}
