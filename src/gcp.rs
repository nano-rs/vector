#![allow(missing_docs)]
use std::{
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use base64::prelude::{BASE64_URL_SAFE, Engine as _};
pub use goauth::scopes::Scope;
use goauth::{
    GoErr,
    auth::{JwtClaims, Token, TokenErr},
    credentials::Credentials,
};
use http::{Uri, uri::PathAndQuery};
use http_1::{Uri as UriV1, header::AUTHORIZATION as AUTHORIZATION_V1};
use http_body_util::{BodyExt as _, Collected};
use hyper::header::AUTHORIZATION;
use smpl_jwt::Jwt;
use snafu::{ResultExt, Snafu};
use tokio::sync::watch;
use vector_http::{HttpError, empty_body};
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::{config::ProxyConfig, http::client_v1::HttpClient, tls::MaybeTlsSettings};

mod credentials;
use credentials::{AuthorizedUser, CredentialKind, ExternalAccount, ImpersonatedServiceAccount};

/// Header that bills a request to a quota project. Sent only when the credentials file names
/// one (`quota_project_id`), as Google's client libraries do.
const USER_PROJECT_HEADER: &str = "x-goog-user-project";

const SERVICE_ACCOUNT_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

// See https://cloud.google.com/compute/docs/access/authenticate-workloads#applications
const METADATA_TOKEN_EXPIRY_MARGIN_SECS: u64 = 200;

const METADATA_TOKEN_ERROR_RETRY_SECS: u64 = 2;

pub const PUBSUB_URL: &str = "https://pubsub.googleapis.com";

pub static PUBSUB_ADDRESS: LazyLock<String> = LazyLock::new(|| {
    std::env::var("EMULATOR_ADDRESS").unwrap_or_else(|_| "http://localhost:8681".into())
});

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum GcpError {
    #[snafu(display("This requires one of api_key or credentials_path to be defined"))]
    MissingAuth,
    #[snafu(display("Invalid GCP credentials: {}", source))]
    InvalidCredentials { source: GoErr },
    #[snafu(display("Invalid GCP API key: {}", source))]
    InvalidApiKey { source: base64::DecodeError },
    #[snafu(display("Healthcheck endpoint forbidden"))]
    HealthcheckForbidden,
    #[snafu(display("Invalid RSA key in GCP credentials: {}", source))]
    InvalidRsaKey { source: GoErr },
    #[snafu(display("Failed to get OAuth token: {}", source))]
    GetToken { source: GoErr },
    #[snafu(display("Failed to get OAuth token text: {}", source))]
    GetTokenBytes { source: hyper_1::Error },
    #[snafu(display("Failed to get implicit GCP token: {}", source))]
    GetImplicitToken { source: HttpError },
    #[snafu(display("Failed to parse OAuth token JSON: {}", source))]
    TokenFromJson { source: TokenErr },
    #[snafu(display("Failed to parse OAuth token JSON text: {}", source))]
    TokenJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to build HTTP client: {}", source))]
    BuildHttpClient { source: crate::Error },
    #[snafu(display("Cannot read GCP credentials file {:?}: {}", path, source))]
    ReadCredentials {
        path: String,
        source: std::io::Error,
    },
    #[snafu(display(
        "Unsupported GCP credentials type {:?}: use service_account, authorized_user, external_account (file or url source) or impersonated_service_account",
        kind
    ))]
    UnsupportedCredentials { kind: String },
    #[snafu(display("Invalid GCP {} credentials: {}", kind, reason))]
    MalformedCredentials { kind: &'static str, reason: String },
    #[snafu(display(
        "GCP token request to {} failed (HTTP {}): {}",
        endpoint,
        status,
        error
    ))]
    TokenEndpoint {
        endpoint: String,
        status: u16,
        error: String,
    },
    #[snafu(display("Failed to obtain the external_account subject token: {}", reason))]
    SubjectToken { reason: String },
}

/// Configuration of the authentication strategy for interacting with GCP services.
// TODO: We're duplicating the "either this or that" verbiage for each field because this struct gets flattened into the
// component config types, which means all that's carried over are the fields, not the type itself.
//
// Seems like we really really have it as a nested field -- i.e. `auth.api_key` -- which is a closer fit to how we do
// similar things in configuration (TLS, framing, decoding, etc.). Doing so would let us embed the type itself, and
// hoist up the common documentation bits to the docs for the type rather than the fields.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct GcpAuthConfig {
    /// An [API key][gcp_api_key].
    ///
    /// Either an API key or a path to a service account credentials JSON file can be specified.
    ///
    /// If both are unset, the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is checked for a filename. If no
    /// filename is named, an attempt is made to fetch an instance service account for the compute instance the program is
    /// running on. If this is not on a GCE instance, then you must define it with an API key or service account
    /// credentials JSON file.
    ///
    /// [gcp_api_key]: https://cloud.google.com/docs/authentication/api-keys
    pub api_key: Option<SensitiveString>,

    /// Path to a [service account][gcp_service_account_credentials] credentials JSON file, or to any other
    /// Application Default Credentials file: `authorized_user` (written by
    /// `gcloud auth application-default login`), `external_account` (workload identity federation
    /// with a file or URL credential source) or `impersonated_service_account` (with
    /// `authorized_user` source credentials). Only service-account keys carry a project id; the
    /// project always comes from the component's own configuration.
    ///
    /// Either an API key or a path to a service account credentials JSON file can be specified.
    ///
    /// If both are unset, the `GOOGLE_APPLICATION_CREDENTIALS` environment variable is checked for a filename. If no
    /// filename is named, an attempt is made to fetch an instance service account for the compute instance the program is
    /// running on. If this is not on a GCE instance, then you must define it with an API key or service account
    /// credentials JSON file.
    ///
    /// [gcp_service_account_credentials]: https://cloud.google.com/docs/authentication/production#manually
    pub credentials_path: Option<String>,

    /// Skip all authentication handling. For use with integration tests only.
    #[serde(default, skip_serializing)]
    #[configurable(metadata(docs::hidden))]
    pub skip_authentication: bool,
}

impl GcpAuthConfig {
    /// # Errors
    /// Returns an error if credentials or the API key are invalid, or token retrieval fails.
    pub async fn build(&self, scope: Scope) -> crate::Result<GcpAuthenticator> {
        Ok(if self.skip_authentication {
            GcpAuthenticator::None
        } else {
            let gap = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
            let creds_path = self.credentials_path.as_ref().or(gap.as_ref());
            match (&creds_path, &self.api_key) {
                (Some(path), _) => GcpAuthenticator::from_file(path, scope).await?,
                (None, Some(api_key)) => GcpAuthenticator::from_api_key(api_key.inner())?,
                (None, None) => GcpAuthenticator::new_implicit().await?,
            }
        })
    }
}

#[derive(Clone, Debug)]
pub enum GcpAuthenticator {
    Credentials(Arc<InnerCreds>),
    ApiKey(Box<str>),
    None,
}

#[derive(Debug)]
pub struct InnerCreds {
    source: TokenSource,
    token: RwLock<Token>,
    quota_project: Option<String>,
}

/// Where access tokens come from. Every variant except `Metadata` is a credentials file.
enum TokenSource {
    ServiceAccount(Credentials, Scope),
    AuthorizedUser(AuthorizedUser),
    ExternalAccount(ExternalAccount, Scope),
    ImpersonatedServiceAccount(ImpersonatedServiceAccount, Scope),
    Metadata,
}

/// Names the kind only: goauth's `Credentials` derives a `Debug` that prints the private key.
impl std::fmt::Debug for TokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ServiceAccount(..) => "ServiceAccount",
            Self::AuthorizedUser(_) => "AuthorizedUser",
            Self::ExternalAccount(..) => "ExternalAccount",
            Self::ImpersonatedServiceAccount(..) => "ImpersonatedServiceAccount",
            Self::Metadata => "Metadata",
        })
    }
}

impl TokenSource {
    /// Parse a credentials file of any supported `type`. The project is never read from it:
    /// only service-account keys carry one, and components take theirs from their own config.
    fn from_json(raw: &str, scope: Scope) -> Result<(Self, Option<String>), GcpError> {
        Ok(match CredentialKind::detect(raw)? {
            CredentialKind::ServiceAccount => {
                // goauth's own parse error ("missing field ...") names a field, never a value.
                let creds = raw
                    .parse::<Credentials>()
                    .context(InvalidCredentialsSnafu)?;
                (Self::ServiceAccount(creds, scope), None)
            }
            CredentialKind::AuthorizedUser => {
                let creds = AuthorizedUser::from_json(raw)?;
                let quota = creds.quota_project_id.clone();
                (Self::AuthorizedUser(creds), quota)
            }
            CredentialKind::ExternalAccount => {
                let creds = ExternalAccount::from_json(raw)?;
                let quota = creds.quota_project_id.clone();
                (Self::ExternalAccount(creds, scope), quota)
            }
            CredentialKind::ImpersonatedServiceAccount => {
                let creds = ImpersonatedServiceAccount::from_json(raw)?;
                let quota = creds.quota_project_id.clone();
                (Self::ImpersonatedServiceAccount(creds, scope), quota)
            }
        })
    }

    async fn fetch(&self) -> crate::Result<Token> {
        Ok(match self {
            Self::ServiceAccount(creds, scope) => fetch_token(creds, scope).await?,
            Self::AuthorizedUser(creds) => creds.fetch_token().await?,
            Self::ExternalAccount(creds, scope) => creds.fetch_token(&scope.url()).await?,
            Self::ImpersonatedServiceAccount(creds, scope) => {
                creds.fetch_token(&scope.url()).await?
            }
            Self::Metadata => get_token_implicit().await?,
        })
    }
}

impl GcpAuthenticator {
    async fn from_file(path: &str, scope: Scope) -> crate::Result<Self> {
        let raw = std::fs::read_to_string(path).context(ReadCredentialsSnafu { path })?;
        Self::from_json(&raw, scope).await
    }

    async fn from_json(raw: &str, scope: Scope) -> crate::Result<Self> {
        let (source, quota_project) = TokenSource::from_json(raw, scope)?;
        let token = RwLock::new(source.fetch().await?);
        Ok(Self::Credentials(Arc::new(InnerCreds {
            source,
            token,
            quota_project,
        })))
    }

    async fn new_implicit() -> crate::Result<Self> {
        let token = RwLock::new(get_token_implicit().await?);
        Ok(Self::Credentials(Arc::new(InnerCreds {
            source: TokenSource::Metadata,
            token,
            quota_project: None,
        })))
    }

    /// The quota project named by the credentials file, if any. Callers that build requests
    /// themselves (gRPC) send it as `x-goog-user-project`; [`Self::apply`] does so already.
    #[must_use]
    pub fn quota_project(&self) -> Option<&str> {
        match self {
            Self::Credentials(inner) => inner.quota_project.as_deref(),
            Self::ApiKey(_) | Self::None => None,
        }
    }

    fn from_api_key(api_key: &str) -> crate::Result<Self> {
        BASE64_URL_SAFE
            .decode(api_key)
            .context(InvalidApiKeySnafu)?;
        Ok(Self::ApiKey(api_key.into()))
    }

    #[must_use]
    pub fn make_token(&self) -> Option<String> {
        match self {
            Self::Credentials(inner) => Some(inner.make_token()),
            Self::ApiKey(_) | Self::None => None,
        }
    }

    /// # Panics
    /// Panics if the token lock is poisoned, the token is not a valid header value, or
    /// adding the API key produces an invalid URI.
    pub fn apply<T>(&self, request: &mut http::Request<T>) {
        if let Some(token) = self.make_token() {
            request
                .headers_mut()
                .insert(AUTHORIZATION, token.parse().unwrap());
        }
        if let Some(project) = self.quota_project().and_then(|p| p.parse().ok()) {
            request.headers_mut().insert(USER_PROJECT_HEADER, project);
        }
        self.apply_uri(request.uri_mut());
    }

    /// Applies authentication to a native `http 1` request, mirroring [`Self::apply`].
    ///
    /// # Panics
    /// Panics if the token lock is poisoned, the token is not a valid header value, or
    /// adding the API key produces an invalid URI.
    pub fn apply_v1<T>(&self, request: &mut http_1::Request<T>) {
        if let Some(token) = self.make_token() {
            request
                .headers_mut()
                .insert(AUTHORIZATION_V1, token.parse().unwrap());
        }
        if let Some(project) = self.quota_project().and_then(|p| p.parse().ok()) {
            request.headers_mut().insert(USER_PROJECT_HEADER, project);
        }
        self.apply_uri_v1(request.uri_mut());
    }

    /// # Panics
    /// Panics if adding the API key produces an invalid URI.
    pub fn apply_uri(&self, uri: &mut Uri) {
        match self {
            Self::Credentials(_) | Self::None => (),
            Self::ApiKey(api_key) => {
                let mut parts = uri.clone().into_parts();
                let path = parts
                    .path_and_query
                    .as_ref()
                    .map_or("/", PathAndQuery::path);
                let paq = format!("{path}?key={api_key}");
                // The API key is verified above to only contain
                // URL-safe characters. That key is added to a path
                // that came from a successfully parsed URI. As such,
                // re-parsing the string cannot fail.
                parts.path_and_query =
                    Some(paq.parse().expect("Could not re-parse path and query"));
                *uri = Uri::from_parts(parts).expect("Could not re-parse URL");
            }
        }
    }

    /// # Panics
    /// Panics if adding the API key produces an invalid URI.
    pub fn apply_uri_v1(&self, uri: &mut UriV1) {
        match self {
            Self::Credentials(_) | Self::None => (),
            Self::ApiKey(api_key) => {
                let mut parts = uri.clone().into_parts();
                let path = parts
                    .path_and_query
                    .as_ref()
                    .map_or("/", http_1::uri::PathAndQuery::path);
                let paq = format!("{path}?key={api_key}");
                // The API key is verified above to only contain
                // URL-safe characters. That key is added to a path
                // that came from a successfully parsed URI. As such,
                // re-parsing the string cannot fail.
                parts.path_and_query =
                    Some(paq.parse().expect("Could not re-parse path and query"));
                *uri = UriV1::from_parts(parts).expect("Could not re-parse URL");
            }
        }
    }

    #[expect(
        clippy::must_use_candidate,
        reason = "Refresh subscription is optional"
    )]
    pub fn spawn_regenerate_token(&self) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        crate::spawn_in_current_span(self.clone().token_regenerator(sender));
        receiver
    }

    async fn token_regenerator(self, sender: watch::Sender<()>) {
        match self {
            Self::Credentials(inner) => {
                let mut expires_in = u64::from(inner.token.read().unwrap().expires_in());
                loop {
                    let deadline = Duration::from_secs(
                        expires_in
                            .saturating_sub(METADATA_TOKEN_EXPIRY_MARGIN_SECS)
                            .max(METADATA_TOKEN_ERROR_RETRY_SECS),
                    );
                    debug!(
                        deadline = deadline.as_secs(),
                        "Sleeping before refreshing GCP authentication token.",
                    );
                    tokio::time::sleep(deadline).await;
                    match inner.regenerate_token().await {
                        Ok(()) => {
                            sender.send_replace(());
                            debug!("GCP authentication token renewed.");
                            // Rather than an expected fresh token, the Metadata Server may return
                            // the same (cached) token during the last 300 seconds of its lifetime.
                            // This scenario is handled by retrying the token refresh after the
                            // METADATA_TOKEN_ERROR_RETRY_SECS period when a fresh token is expected
                            expires_in = u64::from(inner.token.read().unwrap().expires_in());
                        }
                        Err(error) => {
                            error!(
                                message = "Failed to update GCP authentication token.",
                                %error
                            );
                            expires_in = METADATA_TOKEN_EXPIRY_MARGIN_SECS;
                        }
                    }
                }
            }
            Self::ApiKey(_) | Self::None => {
                // This keeps the sender end of the watch open without
                // actually sending anything, effectively creating an
                // empty watch stream.
                sender.closed().await;
            }
        }
    }
}

impl InnerCreds {
    async fn regenerate_token(&self) -> crate::Result<()> {
        let token = self.source.fetch().await?;
        *self.token.write().unwrap() = token;
        Ok(())
    }

    fn make_token(&self) -> String {
        let token = self.token.read().unwrap();
        format!("{} {}", token.token_type(), token.access_token())
    }
}

async fn fetch_token(creds: &Credentials, scope: &Scope) -> crate::Result<Token> {
    let claims = JwtClaims::new(
        creds.iss(),
        std::slice::from_ref(scope),
        creds.token_uri(),
        None,
        None,
    );
    let rsa_key = creds.rsa_key().context(InvalidRsaKeySnafu)?;
    let jwt = Jwt::new(claims, rsa_key, None);

    debug!(
        message = "Fetching GCP authentication token.",
        project = ?creds.project(),
        iss = ?creds.iss(),
        token_uri = ?creds.token_uri(),
    );
    goauth::get_token(&jwt, creds)
        .await
        .context(GetTokenSnafu)
        .map_err(Into::into)
}

async fn get_token_implicit() -> Result<Token, GcpError> {
    debug!("Fetching implicit GCP authentication token.");
    let req = http_1::Request::get(SERVICE_ACCOUNT_TOKEN_URL)
        .header("Metadata-Flavor", "Google")
        .body(empty_body())
        .unwrap();

    let proxy = ProxyConfig::from_env();
    let res = HttpClient::new(MaybeTlsSettings::Raw(()), &proxy)
        .context(BuildHttpClientSnafu)?
        .send(req)
        .await
        .context(GetImplicitTokenSnafu)?;

    let body = res.into_body();
    let bytes = body
        .collect()
        .await
        .map(Collected::to_bytes)
        .context(GetTokenBytesSnafu)?;

    // Token::from_str is irresponsible and may panic!
    match serde_json::from_slice::<Token>(&bytes) {
        Ok(token) => Ok(token),
        Err(error) => Err(match serde_json::from_slice::<TokenErr>(&bytes) {
            Ok(error) => GcpError::TokenFromJson { source: error },
            Err(_) => GcpError::TokenJsonFromStr { source: error },
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_downcast_matches;

    #[tokio::test]
    async fn fails_missing_creds() {
        let error = build_auth("").await.expect_err("build failed to error");
        assert_downcast_matches!(error, GcpError, GcpError::GetImplicitToken { .. });
        // This should be a more relevant error
    }

    #[tokio::test]
    async fn skip_authentication() {
        let auth = build_auth(indoc::indoc! {r#"
            skip_authentication: true
            api_key: "testing"
        "#})
        .await
        .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::None));
    }

    #[tokio::test]
    async fn uses_api_key() {
        let key = crate::test_util::random_string(16);

        let auth = build_auth(&format!("api_key: \"{key}\""))
            .await
            .expect("build_auth failed");
        assert!(matches!(auth, GcpAuthenticator::ApiKey(..)));

        assert_eq!(
            apply_uri(&auth, "http://example.com"),
            format!("http://example.com/?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/"),
            format!("http://example.com/?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/path"),
            format!("http://example.com/path?key={key}")
        );
        assert_eq!(
            apply_uri(&auth, "http://example.com/path1/"),
            format!("http://example.com/path1/?key={key}")
        );
    }

    #[tokio::test]
    async fn fails_bad_api_key() {
        let error = build_auth(r#"api_key: "abc%xyz""#)
            .await
            .expect_err("build failed to error");
        assert_downcast_matches!(error, GcpError, GcpError::InvalidApiKey { .. });
    }

    fn apply_uri(auth: &GcpAuthenticator, uri: &str) -> String {
        let mut uri: Uri = uri.parse().unwrap();
        auth.apply_uri(&mut uri);
        uri.to_string()
    }

    async fn build_auth(yaml: &str) -> crate::Result<GcpAuthenticator> {
        let config: GcpAuthConfig = serde_yaml::from_str(yaml).expect("Invalid YAML");
        config.build(Scope::Compute).await
    }
}
