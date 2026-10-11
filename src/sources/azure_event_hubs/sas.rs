//! Event Hubs connection strings and Shared Access Signature (SAS) credentials.
//!
//! `azure_messaging_eventhubs` 0.15 authenticates exclusively through a
//! [`TokenCredential`], so connection-string (SAS) authentication is provided here by a credential
//! that signs SAS tokens from the shared access key, or that hands out a pre-formed signature.

use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use azure_core::{
    credentials::{AccessToken, TokenCredential, TokenRequestOptions},
    error::{Error as AzureError, ErrorKind as AzureErrorKind},
    time::OffsetDateTime,
};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use openssl::{hash::MessageDigest, pkey::PKey, sign::Signer};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use snafu::Snafu;

/// Characters percent-encoded in SAS tokens. Matches `encodeURIComponent` / the Azure SDKs: every
/// character except `A-Z a-z 0-9 - _ . ! ~ * ' ( )` is escaped.
const SAS_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// Lifetime of tokens signed from a shared access key. The SDK refreshes tokens before they expire.
const SAS_TOKEN_TTL: Duration = Duration::from_secs(60 * 60);

/// Default plain-AMQP port of the Event Hubs emulator.
const EMULATOR_AMQP_PORT: u16 = 5672;

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum ConnectionStringError {
    #[snafu(display("connection string segment `{segment}` is not a `key=value` pair"))]
    InvalidSegment { segment: String },
    #[snafu(display("connection string is missing `Endpoint`"))]
    MissingEndpoint,
    #[snafu(display("connection string `Endpoint` must look like `sb://<namespace host>/`"))]
    InvalidEndpoint,
    #[snafu(display(
        "connection string must contain either `SharedAccessKeyName` and `SharedAccessKey`, or `SharedAccessSignature`"
    ))]
    MissingCredentials,
    #[snafu(display("connection string `SharedAccessKey` is empty"))]
    EmptyKey,
}

/// Credentials carried by a connection string.
#[derive(Clone, PartialEq, Eq)]
pub enum SasCredentials {
    /// A named shared access key. Tokens are signed and renewed locally.
    Key { key_name: String, key: String },
    /// A pre-formed `SharedAccessSignature ...` token. It cannot be renewed.
    Signature(String),
}

impl fmt::Debug for SasCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key { key_name, .. } => f
                .debug_struct("Key")
                .field("key_name", key_name)
                .field("key", &"**REDACTED**")
                .finish(),
            Self::Signature(_) => f.debug_tuple("Signature").field(&"**REDACTED**").finish(),
        }
    }
}

/// A parsed Event Hubs connection string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventHubsConnectionString {
    /// Fully qualified namespace host, for example `myns.servicebus.windows.net` (may carry a port).
    pub namespace: String,
    /// The `EntityPath` (Event Hub name), when present.
    pub entity_path: Option<String>,
    pub credentials: SasCredentials,
    /// `UseDevelopmentEmulator=true`: connect over plain AMQP to the local emulator.
    pub use_development_emulator: bool,
}

impl EventHubsConnectionString {
    pub fn parse(s: &str) -> Result<Self, ConnectionStringError> {
        let mut endpoint = None;
        let mut key_name = None;
        let mut key = None;
        let mut signature = None;
        let mut entity_path = None;
        let mut use_development_emulator = false;

        for segment in s.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            // Values (keys and signatures) may themselves contain `=`, so split on the first one.
            let Some((name, value)) = segment.split_once('=') else {
                return Err(ConnectionStringError::InvalidSegment {
                    segment: segment.split('=').next().unwrap_or_default().to_string(),
                });
            };
            let value = value.trim();
            match name.trim().to_ascii_lowercase().as_str() {
                "endpoint" => endpoint = Some(value.to_string()),
                "sharedaccesskeyname" => key_name = Some(value.to_string()),
                "sharedaccesskey" => key = Some(value.to_string()),
                "sharedaccesssignature" => signature = Some(value.to_string()),
                "entitypath" if !value.is_empty() => entity_path = Some(value.to_string()),
                "usedevelopmentemulator" => {
                    use_development_emulator = value.eq_ignore_ascii_case("true")
                }
                // Unknown keys are ignored, as the other Azure SDKs do.
                _ => {}
            }
        }

        let endpoint = endpoint.ok_or(ConnectionStringError::MissingEndpoint)?;
        let namespace = endpoint
            .split_once("://")
            .map_or(endpoint.as_str(), |(_, rest)| rest)
            .trim_end_matches('/')
            .to_ascii_lowercase();
        if namespace.is_empty() || namespace.contains('/') {
            return Err(ConnectionStringError::InvalidEndpoint);
        }

        let credentials = match (key_name, key, signature) {
            (_, _, Some(signature)) => SasCredentials::Signature(signature),
            (Some(key_name), Some(key), None) => {
                if key.is_empty() {
                    return Err(ConnectionStringError::EmptyKey);
                }
                SasCredentials::Key { key_name, key }
            }
            _ => return Err(ConnectionStringError::MissingCredentials),
        };

        Ok(Self {
            namespace,
            entity_path,
            credentials,
            use_development_emulator,
        })
    }

    /// The namespace host without a port, as used for client and checkpoint identity.
    pub fn namespace_host(&self) -> &str {
        self.namespace
            .split_once(':')
            .map_or(self.namespace.as_str(), |(host, _)| host)
    }

    /// The plain AMQP endpoint of the development emulator, if this connection string targets it.
    pub fn emulator_endpoint(&self) -> Option<String> {
        self.use_development_emulator.then(|| {
            if self.namespace.contains(':') {
                format!("amqp://{}", self.namespace)
            } else {
                format!("amqp://{}:{EMULATOR_AMQP_PORT}", self.namespace)
            }
        })
    }
}

/// A [`TokenCredential`] that produces Event Hubs SAS tokens.
pub struct SasTokenCredential {
    resource_uri: String,
    credentials: SasCredentials,
}

impl fmt::Debug for SasTokenCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SasTokenCredential")
            .field("resource_uri", &self.resource_uri)
            .field("credentials", &self.credentials)
            .finish()
    }
}

impl SasTokenCredential {
    /// Creates a credential scoped to the given Event Hub in the namespace.
    pub fn new(namespace_host: &str, event_hub_name: &str, credentials: SasCredentials) -> Self {
        Self {
            resource_uri: format!(
                "https://{}/{}",
                namespace_host.to_ascii_lowercase(),
                event_hub_name.to_ascii_lowercase()
            ),
            credentials,
        }
    }

    fn sign(&self, key_name: &str, key: &str, expiry: u64) -> Result<String, AzureError> {
        let encoded_uri = utf8_percent_encode(&self.resource_uri, SAS_ENCODE_SET).to_string();
        let string_to_sign = format!("{encoded_uri}\n{expiry}");
        // The service uses the key's UTF-8 bytes (not its base64 decoding) as the HMAC key.
        let signature = PKey::hmac(key.as_bytes())
            .and_then(|pkey| {
                let mut signer = Signer::new(MessageDigest::sha256(), &pkey)?;
                signer.update(string_to_sign.as_bytes())?;
                signer.sign_to_vec()
            })
            .map_err(|error| {
                AzureError::with_message(
                    AzureErrorKind::Credential,
                    format!("failed to sign Event Hubs SAS token: {error}"),
                )
            })?;
        let signature = BASE64_STANDARD.encode(signature);
        Ok(format!(
            "SharedAccessSignature sr={encoded_uri}&sig={}&se={expiry}&skn={}",
            utf8_percent_encode(&signature, SAS_ENCODE_SET),
            utf8_percent_encode(key_name, SAS_ENCODE_SET),
        ))
    }
}

/// Extracts the `se=` (expiry, Unix seconds) field of a pre-formed SAS token.
fn signature_expiry(signature: &str) -> Option<u64> {
    signature
        .trim_start_matches("SharedAccessSignature")
        .trim()
        .split('&')
        .find_map(|field| field.strip_prefix("se="))
        .and_then(|se| se.parse().ok())
}

#[async_trait::async_trait]
impl TokenCredential for SasTokenCredential {
    async fn get_token(
        &self,
        _scopes: &[&str],
        _options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        match &self.credentials {
            SasCredentials::Key { key_name, key } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                let expiry = (now + SAS_TOKEN_TTL).as_secs();
                let token = self.sign(key_name, key, expiry)?;
                Ok(AccessToken::new(
                    token,
                    OffsetDateTime::now_utc() + SAS_TOKEN_TTL,
                ))
            }
            SasCredentials::Signature(signature) => {
                let expires_on = signature_expiry(signature)
                    .and_then(|se| i64::try_from(se).ok())
                    .and_then(|se| OffsetDateTime::from_unix_timestamp(se).ok())
                    .ok_or_else(|| {
                        AzureError::with_message(
                            AzureErrorKind::Credential,
                            "the connection string `SharedAccessSignature` has no valid `se` expiry",
                        )
                    })?;
                Ok(AccessToken::new(signature.clone(), expires_on))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_CS: &str = "Endpoint=sb://MyNs.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=abc+def/ghi=;EntityPath=my-hub";

    #[test]
    fn parses_key_connection_string() {
        let cs = EventHubsConnectionString::parse(KEY_CS).unwrap();
        assert_eq!(cs.namespace, "myns.servicebus.windows.net");
        assert_eq!(cs.entity_path.as_deref(), Some("my-hub"));
        assert_eq!(
            cs.credentials,
            SasCredentials::Key {
                key_name: "listen".into(),
                key: "abc+def/ghi=".into()
            }
        );
        assert!(!cs.use_development_emulator);
        assert_eq!(cs.emulator_endpoint(), None);
    }

    #[test]
    fn parses_signature_and_ignores_empty_segments() {
        let cs = EventHubsConnectionString::parse(
            "Endpoint=sb://ns.servicebus.windows.net;;SharedAccessSignature=SharedAccessSignature sr=x&sig=y%3D&se=1700000000&skn=k;",
        )
        .unwrap();
        assert_eq!(cs.entity_path, None);
        assert_eq!(
            cs.credentials,
            SasCredentials::Signature(
                "SharedAccessSignature sr=x&sig=y%3D&se=1700000000&skn=k".into()
            )
        );
        assert_eq!(
            signature_expiry("SharedAccessSignature sr=x&sig=y%3D&se=1700000000&skn=k"),
            Some(1_700_000_000)
        );
    }

    #[test]
    fn parses_emulator_connection_string() {
        let cs = EventHubsConnectionString::parse(
            "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
        )
        .unwrap();
        assert_eq!(
            cs.emulator_endpoint().as_deref(),
            Some("amqp://localhost:5672")
        );
        assert_eq!(cs.namespace_host(), "localhost");

        let cs = EventHubsConnectionString::parse(
            "Endpoint=sb://localhost:5673;SharedAccessKeyName=a;SharedAccessKey=b;UseDevelopmentEmulator=true",
        )
        .unwrap();
        assert_eq!(
            cs.emulator_endpoint().as_deref(),
            Some("amqp://localhost:5673")
        );
        assert_eq!(cs.namespace_host(), "localhost");
    }

    #[test]
    fn rejects_bad_connection_strings() {
        assert_eq!(
            EventHubsConnectionString::parse("SharedAccessKeyName=a;SharedAccessKey=b"),
            Err(ConnectionStringError::MissingEndpoint)
        );
        assert_eq!(
            EventHubsConnectionString::parse("Endpoint=sb://ns/;SharedAccessKey=b"),
            Err(ConnectionStringError::MissingCredentials)
        );
        assert_eq!(
            EventHubsConnectionString::parse(
                "Endpoint=sb://ns/;SharedAccessKeyName=a;SharedAccessKey="
            ),
            Err(ConnectionStringError::EmptyKey)
        );
        assert_eq!(
            EventHubsConnectionString::parse("Endpoint=sb://ns/path/;SharedAccessSignature=x"),
            Err(ConnectionStringError::InvalidEndpoint)
        );
        let err = EventHubsConnectionString::parse("Endpoint=sb://ns/;garbage").unwrap_err();
        assert!(matches!(err, ConnectionStringError::InvalidSegment { .. }));
    }

    #[test]
    fn debug_redacts_secrets() {
        let cs = EventHubsConnectionString::parse(KEY_CS).unwrap();
        let rendered = format!("{cs:?}");
        assert!(!rendered.contains("abc+def"), "{rendered}");
        let cred = SasTokenCredential::new("ns", "hub", cs.credentials);
        assert!(!format!("{cred:?}").contains("abc+def"));
    }

    #[test]
    fn signs_known_vector() {
        // Reference value computed independently (Python hmac/hashlib/urllib.parse.quote(safe='')).
        let cred = SasTokenCredential::new(
            "myns.servicebus.windows.net",
            "my-hub",
            SasCredentials::Key {
                key_name: "listen".into(),
                key: "c2VjcmV0".into(),
            },
        );
        let token = cred.sign("listen", "c2VjcmV0", 1_700_000_000).unwrap();
        assert_eq!(
            token,
            "SharedAccessSignature sr=https%3A%2F%2Fmyns.servicebus.windows.net%2Fmy-hub&sig=LjGw%2Br6%2BT5ipiW1KFdyQDftD%2FhYbKLWP6QWpe6QFeAw%3D&se=1700000000&skn=listen"
        );
    }

    #[tokio::test]
    async fn key_credential_returns_fresh_token() {
        let cred = SasTokenCredential::new(
            "ns.servicebus.windows.net",
            "hub",
            SasCredentials::Key {
                key_name: "k".into(),
                key: "secret".into(),
            },
        );
        let token = cred.get_token(&[], None).await.unwrap();
        assert!(token.token.secret().starts_with("SharedAccessSignature sr="));
        assert!(token.expires_on > OffsetDateTime::now_utc() + Duration::from_secs(50 * 60));
    }
}
