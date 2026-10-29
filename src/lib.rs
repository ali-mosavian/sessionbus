//! sessionbus: durable relay for sessions on different machines to message each other.
//!
//! Wire: newline-delimited JSON over TCP. Relay binds loopback; reach it through an SSH tunnel.
//! Auth: relay sends a nonce, client signs it with its ssh key (see `authorized_keys`).
pub mod client;
pub mod db;
pub mod server;
