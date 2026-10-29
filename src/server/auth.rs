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

/// Whether an `authorized_keys` entry covers `name`: an exact name, or `ns/*`
/// for every name in that namespace, so one key per machine serves all of its
/// sessions.
fn covers(
    entry: &str,
    name: &str,
) -> bool {
    match entry.strip_suffix('*') {
        Some(prefix) if prefix.ends_with('/') => name.len() > prefix.len() && name.starts_with(prefix),
        _ => entry == name,
    }
}

/// Re-reads `keys` on every call so keys can be added without a restart. A
/// key listed under several entries is refused for all of them: its holder
/// could sign in as any. List a key once, as `ns/*` if it should serve a
/// namespace.
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
        .filter(|(entry, _)| covers(entry, name))
        .filter(|(entry, k)| listed.iter().all(|(other, k2)| other == entry || k2.key_data() != k.key_data()))
        .any(|(_, k)| k.verify(NAMESPACE, &msg, &sig).is_ok())
}

/// Whether some listed key signed in as `name`, even if no entry lets it use
/// that name: the signer is a known key holder, so it may be told why it was
/// refused.
pub fn signed_by_listed_key(
    keys: &Path,
    name: &str,
    nonce: &str,
    sig_pem: &str,
) -> bool {
    let Ok(sig) = SshSig::from_pem(sig_pem) else { return false };
    let Ok(text) = std::fs::read_to_string(keys) else { return false };
    let msg = signed_payload(nonce, name);
    text.lines()
        .filter_map(|l| l.trim().split_once(char::is_whitespace))
        .filter_map(|(_, k)| PublicKey::from_openssh(k.trim()).ok())
        .any(|k| k.verify(NAMESPACE, &msg, &sig).is_ok())
}

/// A keys file others can write is account takeover: refuse to serve with one.
#[cfg(unix)]
pub fn check_keys_perms(keys: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(keys).map_err(|e| format!("{}: {e}", keys.display()))?.permissions().mode();
    if mode & 0o022 != 0 {
        return Err(format!("{}: writable by group or others (mode {:o}); chmod go-w", keys.display(), mode & 0o777));
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn check_keys_perms(_: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_keys_perms, valid_name};

    #[cfg(unix)]
    #[test]
    fn a_keys_file_others_can_write_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let f = std::env::temp_dir().join(format!("sessionbus-perms-{}", std::process::id()));
        std::fs::write(&f, "").unwrap();
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(check_keys_perms(&f).is_ok());
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o664)).unwrap();
        assert!(check_keys_perms(&f).is_err(), "a group-writable keys file was accepted");
        std::fs::remove_file(&f).unwrap();
    }

    #[test]
    fn only_the_first_segment_is_the_namespace() {
        assert_eq!(crate::db::namespace("a/b/c"), "a");
        assert_eq!(crate::db::namespace("w1"), "");
    }

    #[test]
    fn a_namespace_needs_a_name_on_both_sides_of_the_slash() {
        assert!(valid_name("proj/w1") && valid_name("w1"));
        assert!(!valid_name("/w1") && !valid_name("proj/"), "an empty namespace or name was accepted");
    }
}
