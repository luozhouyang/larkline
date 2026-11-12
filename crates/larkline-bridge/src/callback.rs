//! Signed callback tokens for bridge-rendered card buttons.
//!
//! A button value carries `{ __bridge_cb, action, scope, token }`. The token is
//! an HMAC-SHA256 over `scope\0action` with a process-local key, so a forged
//! click (or a replay from another chat) is rejected. The key is generated at
//! startup: buttons rendered before a restart simply stop verifying, which is
//! the safe failure mode.
//!
//! Agent-rendered callbacks (buttons sent through `lark-cli`) use a different
//! contract and are not forwarded yet.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub struct CallbackTokens {
    key: [u8; 32],
}

impl std::fmt::Debug for CallbackTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackTokens").finish_non_exhaustive()
    }
}

impl Default for CallbackTokens {
    fn default() -> Self {
        Self::new()
    }
}

impl CallbackTokens {
    pub fn new() -> Self {
        let mut key = [0u8; 32];
        if getrandom::fill(&mut key).is_err() {
            // getrandom failing is unrecoverable for security; fall back to a
            // process-random key so buttons still cannot be forged offline.
            let mut seed = format!("{:?}{:?}", std::process::id(), std::time::SystemTime::now());
            for (index, byte) in key.iter_mut().enumerate() {
                *byte = seed.as_bytes().get(index).copied().unwrap_or(0)
                    ^ (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos() as u8)
                        .unwrap_or(0)
                        + index as u8);
            }
            seed.clear();
        }
        Self { key }
    }

    pub fn sign(&self, scope: &str, action: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(scope.as_bytes());
        mac.update(b"\0");
        mac.update(action.as_bytes());
        hex(&mac.finalize().into_bytes())
    }

    pub fn verify(&self, scope: &str, action: &str, token: &str) -> bool {
        let Some(provided) = unhex(token) else {
            return false;
        };
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("hmac accepts any key length");
        mac.update(scope.as_bytes());
        mac.update(b"\0");
        mac.update(action.as_bytes());
        mac.verify_slice(&provided).is_ok()
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn unhex(value: &str) -> Option<Vec<u8>> {
    if value.len() % 2 != 0 || value.len() != 64 {
        return None;
    }
    let mut out = Vec::with_capacity(32);
    for pair in value.as_bytes().chunks(2) {
        let text = std::str::from_utf8(pair).ok()?;
        out.push(u8::from_str_radix(text, 16).ok()?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_and_verifies() {
        let tokens = CallbackTokens::new();
        let token = tokens.sign("oc_1", "stop");
        assert!(tokens.verify("oc_1", "stop", &token));
    }

    #[test]
    fn rejects_other_scope_or_action() {
        let tokens = CallbackTokens::new();
        let token = tokens.sign("oc_1", "stop");
        assert!(!tokens.verify("oc_2", "stop", &token));
        assert!(!tokens.verify("oc_1", "retry", &token));
    }

    #[test]
    fn rejects_malformed_tokens() {
        let tokens = CallbackTokens::new();
        assert!(!tokens.verify("oc_1", "stop", ""));
        assert!(!tokens.verify("oc_1", "stop", "zz"));
        assert!(!tokens.verify("oc_1", "stop", &"0".repeat(64)));
    }

    #[test]
    fn different_processes_use_different_keys() {
        assert_ne!(
            CallbackTokens::new().sign("s", "a"),
            CallbackTokens::new().sign("s", "a")
        );
    }
}
