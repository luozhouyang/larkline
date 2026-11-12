//! Encrypted local keystore for App Secrets.
//!
//! Byte-for-byte compatible with `src/config/keystore.ts` so an upgraded
//! install can read secrets written by the Node bridge:
//!
//! * `secrets.enc`  — `{ "version": 1, "entries": { id: { iv, data, tag } } }`
//! * `.keystore.salt` — 32 random bytes, generated once
//! * key = PBKDF2-HMAC-SHA256(`hostname|username`, salt, 100_000, 32)
//! * cipher = AES-256-GCM, 12-byte IV, 16-byte tag, base64 (standard, padded)
//!
//! This is defence in depth against accidental disclosure (backups, log
//! dumps, a stray git commit), not protection from a same-user process. That
//! threat needs an OS keychain and is out of scope for the bridge.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};

use crate::atomic::write_atomic;
use crate::error::{CoreError, Result};

const KEY_LEN: usize = 32;
const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;
const PBKDF2_ITER: u32 = 100_000;
const FILE_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Envelope {
    /// base64 of the 12-byte IV.
    iv: String,
    /// base64 of the ciphertext (without the tag).
    data: String,
    /// base64 of the 16-byte GCM tag.
    tag: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct StoreFile {
    version: u32,
    entries: BTreeMap<String, Envelope>,
}

impl Default for StoreFile {
    fn default() -> Self {
        Self {
            version: FILE_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

/// Keystore handle for one profile.
#[derive(Debug, Clone)]
pub struct Keystore {
    secrets_file: PathBuf,
    salt_file: PathBuf,
    /// `(hostname, username)` seed. Overridable for tests and for reading a
    /// backup produced on another machine.
    identity: Option<(String, String)>,
}

impl Keystore {
    pub fn new(secrets_file: impl Into<PathBuf>, salt_file: impl Into<PathBuf>) -> Self {
        Self {
            secrets_file: secrets_file.into(),
            salt_file: salt_file.into(),
            identity: None,
        }
    }

    /// Use an explicit identity instead of the current machine's.
    pub fn with_identity(
        mut self,
        hostname: impl Into<String>,
        username: impl Into<String>,
    ) -> Self {
        self.identity = Some((hostname.into(), username.into()));
        self
    }

    pub fn secrets_file(&self) -> &Path {
        &self.secrets_file
    }

    pub fn exists(&self) -> bool {
        self.secrets_file.exists()
    }

    /// Look up a secret. Returns `Ok(None)` when the id is absent.
    pub fn get(&self, id: &str) -> Result<Option<String>> {
        let store = self.read_store()?;
        match store.entries.get(id) {
            None => Ok(None),
            Some(envelope) => {
                let key = self.derive_key()?;
                decrypt(&key, envelope).map(Some)
            }
        }
    }

    /// Store or overwrite `id`.
    pub fn set(&self, id: &str, plaintext: &str) -> Result<()> {
        let key = self.derive_key()?;
        let envelope = encrypt(&key, plaintext)?;
        let mut store = self.read_store()?;
        store.entries.insert(id.to_string(), envelope);
        self.write_store(&store)
    }

    /// Delete `id`; returns whether anything was removed.
    pub fn remove(&self, id: &str) -> Result<bool> {
        let mut store = self.read_store()?;
        let removed = store.entries.remove(id).is_some();
        if removed {
            self.write_store(&store)?;
        }
        Ok(removed)
    }

    /// List ids only — never the secret values.
    pub fn list_ids(&self) -> Result<Vec<String>> {
        Ok(self.read_store()?.entries.into_keys().collect())
    }

    fn read_store(&self) -> Result<StoreFile> {
        match fs::read(&self.secrets_file) {
            Ok(bytes) => {
                if bytes.is_empty() {
                    return Ok(StoreFile::default());
                }
                let store: StoreFile = serde_json::from_slice(&bytes)
                    .map_err(|e| CoreError::json(&self.secrets_file, e))?;
                if store.version != FILE_VERSION {
                    return Err(CoreError::Keystore(format!(
                        "unsupported keystore version {} in {}",
                        store.version,
                        self.secrets_file.display()
                    )));
                }
                Ok(store)
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(StoreFile::default()),
            Err(err) => Err(CoreError::io(&self.secrets_file, err)),
        }
    }

    fn write_store(&self, store: &StoreFile) -> Result<()> {
        let mut bytes =
            serde_json::to_vec_pretty(store).map_err(|e| CoreError::json(&self.secrets_file, e))?;
        bytes.push(b'\n');
        write_atomic(&self.secrets_file, &bytes, 0o600)
    }

    /// Load (or create) the salt, then derive the AES key.
    fn derive_key(&self) -> Result<[u8; KEY_LEN]> {
        let salt = self.load_or_create_salt()?;
        let (hostname, username) = match &self.identity {
            Some((host, user)) => (host.clone(), user.clone()),
            None => (
                hostname::get()
                    .map_err(|e| CoreError::Keystore(format!("cannot read hostname: {e}")))?
                    .to_string_lossy()
                    .into_owned(),
                whoami::username(),
            ),
        };
        let seed = format!("{hostname}|{username}");
        let mut key = [0u8; KEY_LEN];
        pbkdf2::pbkdf2_hmac::<sha2::Sha256>(seed.as_bytes(), &salt, PBKDF2_ITER, &mut key);
        Ok(key)
    }

    fn load_or_create_salt(&self) -> Result<Vec<u8>> {
        match fs::read(&self.salt_file) {
            Ok(salt) if salt.len() == KEY_LEN => Ok(salt),
            Ok(salt) => Err(CoreError::Keystore(format!(
                "invalid salt length {} in {}",
                salt.len(),
                self.salt_file.display()
            ))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut salt = vec![0u8; KEY_LEN];
                getrandom::fill(&mut salt)
                    .map_err(|e| CoreError::Keystore(format!("no randomness: {e}")))?;
                write_atomic(&self.salt_file, &salt, 0o600)?;
                Ok(salt)
            }
            Err(err) => Err(CoreError::io(&self.salt_file, err)),
        }
    }
}

fn encrypt(key: &[u8; KEY_LEN], plaintext: &str) -> Result<Envelope> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CoreError::Keystore(format!("invalid key: {e}")))?;
    let mut iv = [0u8; IV_LEN];
    getrandom::fill(&mut iv).map_err(|e| CoreError::Keystore(format!("no randomness: {e}")))?;
    let nonce = Nonce::from_slice(&iv);
    let mut sealed = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|_| CoreError::Keystore("encryption failed".into()))?;
    // aes-gcm appends the tag to the ciphertext; the Node envelope stores them
    // in separate base64 fields.
    let tag = sealed.split_off(sealed.len() - TAG_LEN);
    Ok(Envelope {
        iv: BASE64.encode(iv),
        data: BASE64.encode(sealed),
        tag: BASE64.encode(tag),
    })
}

fn decrypt(key: &[u8; KEY_LEN], envelope: &Envelope) -> Result<String> {
    let iv = BASE64
        .decode(&envelope.iv)
        .map_err(|e| CoreError::Keystore(format!("invalid iv: {e}")))?;
    let data = BASE64
        .decode(&envelope.data)
        .map_err(|e| CoreError::Keystore(format!("invalid data: {e}")))?;
    let tag = BASE64
        .decode(&envelope.tag)
        .map_err(|e| CoreError::Keystore(format!("invalid tag: {e}")))?;
    if iv.len() != IV_LEN || tag.len() != TAG_LEN {
        return Err(CoreError::Keystore("invalid envelope lengths".into()));
    }

    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CoreError::Keystore(format!("invalid key: {e}")))?;
    let mut sealed = data;
    sealed.extend_from_slice(&tag);
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&iv), sealed.as_slice())
        .map_err(|_| {
            CoreError::Keystore(
                "decryption failed (wrong machine/user, or the file is corrupt)".into(),
            )
        })?;
    String::from_utf8(plaintext)
        .map_err(|e| CoreError::Keystore(format!("secret is not valid utf-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keystore(dir: &tempfile::TempDir) -> Keystore {
        Keystore::new(dir.path().join("secrets.enc"), dir.path().join(".salt"))
            .with_identity("test-host", "test-user")
    }

    #[test]
    fn roundtrip_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = keystore(&dir);
        assert_eq!(store.get("missing").unwrap(), None);
        store.set("app-cli_1", "s3cret").unwrap();
        assert_eq!(store.get("app-cli_1").unwrap().as_deref(), Some("s3cret"));
        assert_eq!(store.list_ids().unwrap(), vec!["app-cli_1"]);
        assert!(store.remove("app-cli_1").unwrap());
        assert!(!store.remove("app-cli_1").unwrap());
    }

    #[test]
    fn ciphertext_differs_per_write() {
        let dir = tempfile::tempdir().unwrap();
        let store = keystore(&dir);
        store.set("k", "same").unwrap();
        let first = fs::read(store.secrets_file()).unwrap();
        store.set("k", "same").unwrap();
        let second = fs::read(store.secrets_file()).unwrap();
        assert_ne!(first, second, "IV must be fresh for every write");
    }

    #[test]
    fn wrong_identity_cannot_decrypt() {
        let dir = tempfile::tempdir().unwrap();
        keystore(&dir).set("k", "v").unwrap();
        let other = Keystore::new(dir.path().join("secrets.enc"), dir.path().join(".salt"))
            .with_identity("someone-else", "test-user");
        assert!(other.get("k").is_err());
    }

    #[test]
    fn salt_is_created_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = keystore(&dir);
        store.set("k", "v").unwrap();
        let salt = fs::read(dir.path().join(".salt")).unwrap();
        assert_eq!(salt.len(), KEY_LEN);
        store.set("k2", "v2").unwrap();
        assert_eq!(fs::read(dir.path().join(".salt")).unwrap(), salt);
    }
}
