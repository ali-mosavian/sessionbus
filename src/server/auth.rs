//! Who a connection is: a name bound to the ssh key listed for it.
use std::{
    io::{self, Read},
    path::Path,
};

use ssh_key::{PublicKey, SshSig};

pub const NAMESPACE: &str = "sessionbus";

/// What a client signs to prove it holds the key registered for `name`.
pub fn signed_payload(
    nonce: &str,
    name: &str,
) -> Vec<u8> {
    format!("sessionbus-v1:{nonce}:{name}").into_bytes()
}

pub fn nonce() -> io::Result<String> {
    let mut b = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut b)?;
    Ok(b.iter().map(|x| format!("{x:02x}")).collect())
}

pub fn valid_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 64
        && !n.starts_with(['#', '/'])
        && !n.ends_with('/')
        && !n.contains(char::is_whitespace)
}

/// Re-reads `keys` on every call so keys can be added without a restart. A
/// key listed under several names is refused for all of them: its holder
/// could sign in as any.
pub fn verify(
    keys: &Path,
    name: &str,
    nonce: &str,
    sig_pem: &str,
) -> bool {
    let Ok(sig) = SshSig::from_pem(sig_pem) else { return false };
    let Ok(text) = std::fs::read_to_string(keys) else { return false };
    let msg = signed_payload(nonce, name);
    let listed: Vec<(&str, PublicKey)> = text
        .lines()
        .filter_map(|l| l.trim().split_once(char::is_whitespace))
        .filter_map(|(n, k)| Some((n, PublicKey::from_openssh(k.trim()).ok()?)))
        .collect();
    listed
        .iter()
        .filter(|(n, _)| *n == name)
        .filter(|(_, k)| listed.iter().all(|(other, k2)| *other == name || k2.key_data() != k.key_data()))
        .any(|(_, k)| k.verify(NAMESPACE, &msg, &sig).is_ok())
}

#[cfg(test)]
mod tests {
    use super::valid_name;

    #[test]
    fn a_namespace_needs_a_name_on_both_sides_of_the_slash() {
        assert!(valid_name("proj/w1") && valid_name("w1"));
        assert!(!valid_name("/w1") && !valid_name("proj/"), "an empty namespace or name was accepted");
    }
}
