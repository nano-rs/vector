//! nano fork (NAN-3770): named, revocable ingest tokens stored as SHA-256 hashes.
//!
//! A push source (`http_server`, `splunk_hec`) can point `valid_tokens_file` at a JSON file
//! that lists the SHA-256 of every accepted token with the id and name of the credential it
//! belongs to:
//!
//! ```json
//! {
//!   "version": 1,
//!   "fallback_enabled": true,
//!   "tokens": [
//!     { "sha256": "<64 lowercase hex chars>", "id": "<credential id>", "name": "<label>" }
//!   ]
//! }
//! ```
//!
//! The file holds no plaintext token. It is re-read when its modification time, size or inode
//! changes, checked at most once per `reload_interval_secs` on the request path, so adding or
//! revoking a token takes effect within that interval with no config reload and no listener
//! restart.
//!
//! A request whose token matches an entry is authenticated, and every event it produces gets
//! the credential in event metadata: `%nano_credential_id` and `%nano_credential_name`
//! (overwriting any value, so a sender cannot forge them). A request that matches no entry
//! falls back to the source's other authentication (`valid_tokens` for `splunk_hec`, `auth`
//! for `http_server`), unless the file sets `"fallback_enabled": false`, in which case it is
//! refused.
//!
//! Each authenticated request increments the internal counter `hashed_token_requests_total`
//! with a `credential_id` label (bounded by the number of listed tokens).
//!
//! A missing file is an empty token list with the fallback enabled, so a deployment that has
//! not issued a token yet still boots and accepts its static token. A file that fails to
//! parse keeps the last good snapshot (or, at startup, the empty one) and logs an error; it
//! never opens the source.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant, SystemTime},
};

use serde::Deserialize;
use vector_lib::{configurable::configurable_component, event::Event};
use vrl::value::Value;

/// Event metadata key holding the id of the credential that authenticated the request.
pub const CREDENTIAL_ID_METADATA_KEY: &str = "nano_credential_id";
/// Event metadata key holding the name of the credential that authenticated the request.
pub const CREDENTIAL_NAME_METADATA_KEY: &str = "nano_credential_name";
/// Internal counter of requests authenticated by a file token, labelled `credential_id`.
pub const CREDENTIAL_REQUESTS_METRIC: &str = "hashed_token_requests_total";

const fn default_reload_interval_secs() -> u64 {
    2
}

/// A file of SHA-256-hashed tokens, reloaded on change (nano fork).
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HashedTokensFileConfig {
    /// Path of the JSON tokens file.
    ///
    /// The file lists the SHA-256 (lowercase hex) of each accepted token with the id and name
    /// of its credential. A missing file is treated as an empty list.
    #[configurable(metadata(docs::examples = "/etc/vector/dynamic/ingest-tokens/tokens.json"))]
    pub path: PathBuf,

    /// How often, at most, the file is checked for changes, in seconds.
    ///
    /// A token added to or removed from the file takes effect within this interval.
    #[serde(default = "default_reload_interval_secs")]
    #[configurable(metadata(docs::type_unit = "seconds"))]
    #[configurable(metadata(docs::human_name = "Reload Interval"))]
    pub reload_interval_secs: u64,
}

/// The credential a token belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HashedCredential {
    /// Credential id, stamped as `%nano_credential_id`.
    pub id: String,
    /// Credential name, stamped as `%nano_credential_name`.
    pub name: String,
}

impl HashedCredential {
    /// Writes the credential into the event's metadata, overwriting existing values.
    pub fn stamp(&self, event: &mut Event) {
        let metadata = event.metadata_mut().value_mut();
        metadata.insert(
            vrl::path!(CREDENTIAL_ID_METADATA_KEY),
            Value::from(self.id.clone()),
        );
        metadata.insert(
            vrl::path!(CREDENTIAL_NAME_METADATA_KEY),
            Value::from(self.name.clone()),
        );
    }
}

/// Result of checking a presented token against the file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenCheck {
    /// The token is listed.
    Matched(Arc<HashedCredential>),
    /// The token is not listed (or no token was presented).
    NotListed {
        /// Whether the source may still try its other authentication.
        fallback_enabled: bool,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokensFile {
    version: u32,
    #[serde(default = "crate::serde::default_true")]
    fallback_enabled: bool,
    #[serde(default)]
    tokens: Vec<TokenEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenEntry {
    sha256: String,
    id: String,
    #[serde(default)]
    name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    modified: Option<SystemTime>,
    len: u64,
    #[cfg(unix)]
    inode: u64,
}

impl Fingerprint {
    fn of(metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt as _;
        Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }
}

#[derive(Debug, Default)]
struct Snapshot {
    /// `None` when the file was missing at the last check.
    fingerprint: Option<Fingerprint>,
    tokens: HashMap<[u8; 32], Arc<HashedCredential>>,
    fallback_enabled: bool,
}

impl Snapshot {
    fn empty() -> Self {
        Self {
            fingerprint: None,
            tokens: HashMap::new(),
            fallback_enabled: true,
        }
    }
}

/// The loaded token file, shared by every request of one source.
#[derive(Debug)]
pub struct HashedTokenStore {
    path: PathBuf,
    interval: Duration,
    snapshot: RwLock<Snapshot>,
    last_check: Mutex<Instant>,
}

impl HashedTokenStore {
    /// Loads the file now. Never fails: see the module docs for missing and invalid files.
    pub fn new(config: &HashedTokensFileConfig) -> Self {
        let store = Self {
            path: config.path.clone(),
            interval: Duration::from_secs(config.reload_interval_secs),
            snapshot: RwLock::new(Snapshot::empty()),
            last_check: Mutex::new(Instant::now()),
        };
        store.reload_if_changed();
        store
    }

    /// Checks a presented token (without any `Bearer ` / `Splunk ` scheme prefix).
    pub fn check(&self, token: Option<&str>) -> TokenCheck {
        self.maybe_reload();
        let snapshot = self.snapshot.read().expect("token snapshot lock poisoned");
        let matched = token
            .filter(|token| !token.is_empty())
            .and_then(|token| snapshot.tokens.get(&openssl::sha::sha256(token.as_bytes())));
        match matched {
            Some(credential) => {
                // Per-credential request count, so the control plane can show "last used"
                // from the metrics endpoint. Bounded by the number of issued tokens.
                #[allow(clippy::disallowed_macros)]
                metrics::counter!(
                    CREDENTIAL_REQUESTS_METRIC,
                    "credential_id" => credential.id.clone()
                )
                .increment(1);
                TokenCheck::Matched(Arc::clone(credential))
            }
            None => TokenCheck::NotListed {
                fallback_enabled: snapshot.fallback_enabled,
            },
        }
    }

    /// Checks the `Authorization` header value against `<scheme> <token>`.
    pub fn check_authorization(&self, header: Option<&str>, scheme: &str) -> TokenCheck {
        let token = header.and_then(|value| {
            value
                .strip_prefix(scheme)
                .and_then(|rest| rest.strip_prefix(' '))
        });
        self.check(token)
    }

    fn maybe_reload(&self) {
        // One request per interval stats the file; the others never wait for it.
        let Ok(mut last_check) = self.last_check.try_lock() else {
            return;
        };
        if last_check.elapsed() < self.interval {
            return;
        }
        *last_check = Instant::now();
        drop(last_check);
        self.reload_if_changed();
    }

    fn reload_if_changed(&self) {
        let fingerprint = match fs::metadata(&self.path) {
            Ok(metadata) => Some(Fingerprint::of(&metadata)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                error!(
                    message = "Unable to stat the hashed tokens file; keeping the loaded tokens.",
                    path = %self.path.display(),
                    %error,
                    internal_log_rate_limit = true,
                );
                return;
            }
        };
        if self
            .snapshot
            .read()
            .expect("token snapshot lock poisoned")
            .fingerprint
            == fingerprint
            && fingerprint.is_some()
        {
            return;
        }
        let Some(fingerprint) = fingerprint else {
            let mut snapshot = self.snapshot.write().expect("token snapshot lock poisoned");
            if snapshot.fingerprint.is_some() {
                warn!(
                    message = "Hashed tokens file removed; no file tokens are accepted.",
                    path = %self.path.display(),
                );
            }
            *snapshot = Snapshot::empty();
            return;
        };
        match load(&self.path) {
            Ok((tokens, fallback_enabled)) => {
                info!(
                    message = "Loaded hashed tokens file.",
                    path = %self.path.display(),
                    tokens = tokens.len(),
                    fallback_enabled,
                );
                *self.snapshot.write().expect("token snapshot lock poisoned") = Snapshot {
                    fingerprint: Some(fingerprint),
                    tokens,
                    fallback_enabled,
                };
            }
            Err(error) => {
                // Remember the fingerprint so a bad file is reported once, not per request.
                self.snapshot
                    .write()
                    .expect("token snapshot lock poisoned")
                    .fingerprint = Some(fingerprint);
                error!(
                    message = "Invalid hashed tokens file; keeping the previously loaded tokens.",
                    path = %self.path.display(),
                    %error,
                    internal_log_rate_limit = true,
                );
            }
        }
    }
}

type LoadedTokens = (HashMap<[u8; 32], Arc<HashedCredential>>, bool);

fn load(path: &Path) -> Result<LoadedTokens, String> {
    let data = fs::read(path).map_err(|error| error.to_string())?;
    let file: TokensFile = serde_json::from_slice(&data).map_err(|error| error.to_string())?;
    if file.version != 1 {
        return Err(format!("unsupported version {}", file.version));
    }
    let mut tokens = HashMap::with_capacity(file.tokens.len());
    for entry in file.tokens {
        let digest = decode_sha256(&entry.sha256)
            .ok_or_else(|| format!("credential {:?}: sha256 is not 64 hex characters", entry.id))?;
        if entry.id.is_empty() {
            return Err("an entry has an empty id".to_owned());
        }
        let credential = Arc::new(HashedCredential {
            id: entry.id,
            name: entry.name,
        });
        if tokens.insert(digest, credential).is_some() {
            return Err("two entries share one sha256".to_owned());
        }
    }
    Ok((tokens, file.fallback_enabled))
}

fn decode_sha256(hex: &str) -> Option<[u8; 32]> {
    let bytes = hex.as_bytes();
    if bytes.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, pair) in bytes.chunks_exact(2).enumerate() {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out[i] = (high * 16 + low) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::event::LogEvent;

    fn sha256_hex(token: &str) -> String {
        openssl::sha::sha256(token.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    fn write(path: &Path, body: &str) {
        // Atomic replace, as nanosiem writes it.
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, body).unwrap();
        fs::rename(&tmp, path).unwrap();
    }

    fn file_with(tokens: &[(&str, &str, &str)], fallback_enabled: bool) -> String {
        let entries: Vec<_> = tokens
            .iter()
            .map(|(token, id, name)| {
                serde_json::json!({"sha256": sha256_hex(token), "id": id, "name": name})
            })
            .collect();
        serde_json::json!({"version": 1, "fallback_enabled": fallback_enabled, "tokens": entries})
            .to_string()
    }

    fn store(path: &Path, interval: u64) -> HashedTokenStore {
        HashedTokenStore::new(&HashedTokensFileConfig {
            path: path.to_path_buf(),
            reload_interval_secs: interval,
        })
    }

    #[test]
    fn missing_file_is_empty_with_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir.path().join("absent.json"), 0);
        assert_eq!(
            store.check(Some("anything")),
            TokenCheck::NotListed {
                fallback_enabled: true
            }
        );
    }

    #[test]
    fn matches_listed_token_only() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "sender a")], true));
        let store = store(&path, 0);
        assert_eq!(
            store.check(Some("tok-a")),
            TokenCheck::Matched(Arc::new(HashedCredential {
                id: "id-a".into(),
                name: "sender a".into()
            }))
        );
        assert!(matches!(
            store.check(Some("tok-b")),
            TokenCheck::NotListed { .. }
        ));
        assert!(matches!(store.check(None), TokenCheck::NotListed { .. }));
        assert!(matches!(
            store.check(Some("")),
            TokenCheck::NotListed { .. }
        ));
    }

    #[test]
    fn authorization_scheme_is_required() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "a")], true));
        let store = store(&path, 0);
        assert!(matches!(
            store.check_authorization(Some("Splunk tok-a"), "Splunk"),
            TokenCheck::Matched(_)
        ));
        assert!(matches!(
            store.check_authorization(Some("Bearer tok-a"), "Splunk"),
            TokenCheck::NotListed { .. }
        ));
        assert!(matches!(
            store.check_authorization(Some("tok-a"), "Bearer"),
            TokenCheck::NotListed { .. }
        ));
    }

    #[test]
    fn revocation_takes_effect_after_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(
            &path,
            &file_with(&[("tok-a", "id-a", "a"), ("tok-b", "id-b", "b")], true),
        );
        let store = store(&path, 0);
        assert!(matches!(store.check(Some("tok-b")), TokenCheck::Matched(_)));
        write(&path, &file_with(&[("tok-a", "id-a", "a")], true));
        assert!(matches!(
            store.check(Some("tok-b")),
            TokenCheck::NotListed { .. }
        ));
        assert!(matches!(store.check(Some("tok-a")), TokenCheck::Matched(_)));
    }

    #[test]
    fn reload_is_rate_limited() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "a")], true));
        let store = store(&path, 3600);
        write(&path, &file_with(&[], true));
        // Within the interval the old snapshot still answers.
        assert!(matches!(store.check(Some("tok-a")), TokenCheck::Matched(_)));
    }

    #[test]
    fn fallback_can_be_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "a")], false));
        let store = store(&path, 0);
        assert_eq!(
            store.check(Some("other")),
            TokenCheck::NotListed {
                fallback_enabled: false
            }
        );
    }

    #[test]
    fn invalid_file_keeps_previous_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "a")], true));
        let store = store(&path, 0);
        write(
            &path,
            "{\"version\": 1, \"tokens\": [{\"sha256\": \"zz\", \"id\": \"x\"}]}",
        );
        assert!(matches!(store.check(Some("tok-a")), TokenCheck::Matched(_)));
        write(&path, "not json");
        assert!(matches!(store.check(Some("tok-a")), TokenCheck::Matched(_)));
    }

    #[test]
    fn invalid_file_at_startup_accepts_no_file_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, "{\"version\": 2, \"tokens\": []}");
        let store = store(&path, 0);
        assert_eq!(
            store.check(Some("tok-a")),
            TokenCheck::NotListed {
                fallback_enabled: true
            }
        );
    }

    #[test]
    fn removed_file_drops_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tokens.json");
        write(&path, &file_with(&[("tok-a", "id-a", "a")], false));
        let store = store(&path, 0);
        fs::remove_file(&path).unwrap();
        assert_eq!(
            store.check(Some("tok-a")),
            TokenCheck::NotListed {
                fallback_enabled: true
            }
        );
    }

    #[test]
    fn stamp_overwrites_metadata() {
        let mut event = Event::Log(LogEvent::from("message"));
        event.metadata_mut().value_mut().insert(
            vrl::path!(CREDENTIAL_ID_METADATA_KEY),
            Value::from("forged"),
        );
        HashedCredential {
            id: "id-a".into(),
            name: "a".into(),
        }
        .stamp(&mut event);
        let metadata = event.metadata().value();
        assert_eq!(
            metadata.get(vrl::path!(CREDENTIAL_ID_METADATA_KEY)),
            Some(&Value::from("id-a"))
        );
        assert_eq!(
            metadata.get(vrl::path!(CREDENTIAL_NAME_METADATA_KEY)),
            Some(&Value::from("a"))
        );
    }

    #[test]
    fn config_defaults_interval() {
        let config: HashedTokensFileConfig = toml::from_str("path = \"/tmp/tokens.json\"").unwrap();
        assert_eq!(config.reload_interval_secs, 2);
        assert!(toml::from_str::<HashedTokensFileConfig>("path = \"/x\"\nbogus = 1").is_err());
    }
}
