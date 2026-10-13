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
use http_body::{Body as _, Collected};
use hyper::header::AUTHORIZATION;
use smpl_jwt::Jwt;
use snafu::{ResultExt, Snafu};
use tokio::sync::watch;
use vector_lib::{
    NamedInternalEvent,
    configurable::configurable_component,
    counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
    sensitive_string::SensitiveString,
};

use crate::{
    config::ProxyConfig,
    http::{HttpClient, HttpError},
};

const SERVICE_ACCOUNT_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

// See https://cloud.google.com/compute/docs/access/authenticate-workloads#applications
const METADATA_TOKEN_EXPIRY_MARGIN_SECS: u64 = 200;

const METADATA_TOKEN_ERROR_RETRY_SECS: u64 = 2;

/// Upper bound of the retry backoff while a deferred authenticator has no token yet.
const DEFERRED_TOKEN_MAX_RETRY_SECS: u64 = 60;

/// How long a deferred authenticator waits for a token (the first one at build, and each retry
/// while it has none) before treating the attempt as failed. A token endpoint that hangs must not
/// hang the build, which would keep every other component of the Vector from starting.
const DEFERRED_TOKEN_TIMEOUT: Duration = Duration::from_secs(10);

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
    GetTokenBytes { source: hyper::Error },
    #[snafu(display("Failed to get implicit GCP token: {}", source))]
    GetImplicitToken { source: HttpError },
    #[snafu(display("Failed to parse OAuth token JSON: {}", source))]
    TokenFromJson { source: TokenErr },
    #[snafu(display("Failed to parse OAuth token JSON text: {}", source))]
    TokenJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to build HTTP client: {}", source))]
    BuildHttpClient { source: HttpError },
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

    /// Path to a [service account][gcp_service_account_credentials] credentials JSON file.
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

    /// Like [`Self::build`], but a failure to obtain the first token does not fail the build.
    ///
    /// Configuration errors (unreadable or malformed credentials file, invalid RSA key, invalid
    /// API key) still fail. Any other failure to get the first token (refused credentials, an
    /// unreachable token endpoint) is logged, and the authenticator starts without a token: it
    /// applies no `Authorization` header until the token regenerator spawned by
    /// [`GcpAuthenticator::spawn_regenerate_token`] gets one, retrying with a backoff starting at
    /// 2 seconds, and signals that receiver when it does.
    ///
    /// Meant for sources, which should start and recover by themselves once the credentials work,
    /// instead of failing the whole configuration.
    pub async fn build_deferred(&self, scope: Scope) -> crate::Result<GcpAuthenticator> {
        Ok(if self.skip_authentication {
            GcpAuthenticator::None
        } else {
            let gap = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
            let creds_path = self.credentials_path.as_ref().or(gap.as_ref());
            match (&creds_path, &self.api_key) {
                (Some(path), _) => GcpAuthenticator::from_file_deferred(path, scope).await?,
                (None, Some(api_key)) => GcpAuthenticator::from_api_key(api_key.inner())?,
                (None, None) => {
                    GcpAuthenticator::new_implicit_deferred(
                        SERVICE_ACCOUNT_TOKEN_URL.into(),
                        DEFERRED_TOKEN_TIMEOUT,
                    )
                    .await?
                }
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
    creds: Option<(Credentials, Scope)>,
    /// Where implicit (`creds == None`) tokens are fetched from.
    metadata_url: Box<str>,
    /// `None` only for a deferred authenticator that has not obtained its first token yet.
    token: RwLock<Option<Token>>,
}

impl GcpAuthenticator {
    async fn from_file(path: &str, scope: Scope) -> crate::Result<Self> {
        let creds = Credentials::from_file(path).context(InvalidCredentialsSnafu)?;
        let token = fetch_token(&creds, &scope).await?;
        Ok(Self::with_token(Some((creds, scope)), Some(token)))
    }

    async fn new_implicit() -> crate::Result<Self> {
        let token = get_token_implicit(SERVICE_ACCOUNT_TOKEN_URL).await?;
        Ok(Self::with_token(None, Some(token)))
    }

    async fn from_file_deferred(path: &str, scope: Scope) -> crate::Result<Self> {
        let creds = Credentials::from_file(path).context(InvalidCredentialsSnafu)?;
        // Checked here so that a malformed key fails the build instead of being retried.
        creds.rsa_key().context(InvalidRsaKeySnafu)?;
        let token =
            match tokio::time::timeout(DEFERRED_TOKEN_TIMEOUT, fetch_token(&creds, &scope)).await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err(timed_out(DEFERRED_TOKEN_TIMEOUT)),
            };
        Ok(Self::with_token(
            Some((creds, scope)),
            deferred_first_token(token),
        ))
    }

    pub(crate) async fn new_implicit_deferred(
        metadata_url: String,
        timeout: Duration,
    ) -> crate::Result<Self> {
        let token = match tokio::time::timeout(timeout, get_token_implicit(&metadata_url)).await {
            Ok(Err(error @ GcpError::BuildHttpClient { .. })) => return Err(error.into()),
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(_) => Err(timed_out(timeout)),
        };
        Ok(Self::Credentials(Arc::new(InnerCreds {
            creds: None,
            metadata_url: metadata_url.into(),
            token: RwLock::new(deferred_first_token(token)),
        })))
    }

    fn with_token(creds: Option<(Credentials, Scope)>, token: Option<Token>) -> Self {
        Self::Credentials(Arc::new(InnerCreds {
            creds,
            metadata_url: SERVICE_ACCOUNT_TOKEN_URL.into(),
            token: RwLock::new(token),
        }))
    }

    fn from_api_key(api_key: &str) -> crate::Result<Self> {
        BASE64_URL_SAFE
            .decode(api_key)
            .context(InvalidApiKeySnafu)?;
        Ok(Self::ApiKey(api_key.into()))
    }

    pub fn make_token(&self) -> Option<String> {
        match self {
            Self::Credentials(inner) => inner.make_token(),
            Self::ApiKey(_) | Self::None => None,
        }
    }

    /// Whether this is a deferred authenticator still waiting for its first token (see
    /// [`GcpAuthConfig::build_deferred`]). Requests made meanwhile are unauthenticated.
    pub fn awaiting_token(&self) -> bool {
        match self {
            Self::Credentials(inner) => inner.token.read().unwrap().is_none(),
            Self::ApiKey(_) | Self::None => false,
        }
    }

    pub fn apply<T>(&self, request: &mut http::Request<T>) {
        if let Some(token) = self.make_token() {
            request
                .headers_mut()
                .insert(AUTHORIZATION, token.parse().unwrap());
        }
        self.apply_uri(request.uri_mut());
    }

    /// Applies authentication to a native `http 1` request, mirroring [`Self::apply`].
    pub fn apply_v1<T>(&self, request: &mut http_1::Request<T>) {
        if let Some(token) = self.make_token() {
            request
                .headers_mut()
                .insert(AUTHORIZATION_V1, token.parse().unwrap());
        }
        self.apply_uri_v1(request.uri_mut());
    }

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

    fn apply_uri_v1(&self, uri: &mut UriV1) {
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

    pub fn spawn_regenerate_token(&self) -> watch::Receiver<()> {
        let (sender, receiver) = watch::channel(());
        crate::spawn_in_current_span(self.clone().token_regenerator(sender));
        receiver
    }

    async fn token_regenerator(self, sender: watch::Sender<()>) {
        match self {
            Self::Credentials(inner) => {
                // Without a token yet (deferred), the first retry is due right away.
                let mut expires_in = inner.expires_in().unwrap_or(0);
                // The backoff only applies while there is no token at all: once there is one,
                // a failed refresh is retried every METADATA_TOKEN_ERROR_RETRY_SECS as before.
                let mut no_token_retry = METADATA_TOKEN_ERROR_RETRY_SECS;
                loop {
                    let deadline = if inner.has_token() {
                        Duration::from_secs(
                            expires_in
                                .saturating_sub(METADATA_TOKEN_EXPIRY_MARGIN_SECS)
                                .max(METADATA_TOKEN_ERROR_RETRY_SECS),
                        )
                    } else {
                        let retry = no_token_retry;
                        no_token_retry = (no_token_retry * 2).min(DEFERRED_TOKEN_MAX_RETRY_SECS);
                        Duration::from_secs(retry)
                    };
                    debug!(
                        deadline = deadline.as_secs(),
                        "Sleeping before refreshing GCP authentication token.",
                    );
                    tokio::time::sleep(deadline).await;
                    let had_token = inner.has_token();
                    let result = if had_token {
                        inner.regenerate_token().await
                    } else {
                        // Still deferred: a hanging token endpoint must not stall the retries.
                        tokio::time::timeout(DEFERRED_TOKEN_TIMEOUT, inner.regenerate_token())
                            .await
                            .unwrap_or_else(|_| Err(timed_out(DEFERRED_TOKEN_TIMEOUT).into()))
                    };
                    match result {
                        Ok(()) => {
                            sender.send_replace(());
                            debug!("GCP authentication token renewed.");
                            // Rather than an expected fresh token, the Metadata Server may return
                            // the same (cached) token during the last 300 seconds of its lifetime.
                            // This scenario is handled by retrying the token refresh after the
                            // METADATA_TOKEN_ERROR_RETRY_SECS period when a fresh token is expected
                            expires_in = inner.expires_in().unwrap_or(0);
                        }
                        Err(error) if !had_token => {
                            emit!(GcpAuthTokenDeferredError {
                                error: &error.to_string(),
                                retry_in: Duration::from_secs(no_token_retry),
                            });
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
                sender.closed().await
            }
        }
    }
}

impl InnerCreds {
    async fn regenerate_token(&self) -> crate::Result<()> {
        let token = match &self.creds {
            Some((creds, scope)) => fetch_token(creds, scope).await?,
            None => get_token_implicit(&self.metadata_url).await?,
        };
        *self.token.write().unwrap() = Some(token);
        Ok(())
    }

    fn make_token(&self) -> Option<String> {
        let token = self.token.read().unwrap();
        let token = token.as_ref()?;
        Some(format!("{} {}", token.token_type(), token.access_token()))
    }

    fn has_token(&self) -> bool {
        self.token.read().unwrap().is_some()
    }

    fn expires_in(&self) -> Option<u64> {
        let token = self.token.read().unwrap();
        token.as_ref().map(|token| token.expires_in() as u64)
    }
}

/// The first token of a deferred authenticator: a failure is logged, not returned.
fn deferred_first_token(token: Result<Token, String>) -> Option<Token> {
    match token {
        Ok(token) => Some(token),
        Err(error) => {
            emit!(GcpAuthTokenDeferredError {
                error: &error,
                retry_in: Duration::from_secs(METADATA_TOKEN_ERROR_RETRY_SECS),
            });
            None
        }
    }
}

fn timed_out(timeout: Duration) -> String {
    format!(
        "timed out after {} s waiting for the token endpoint",
        timeout.as_secs_f64()
    )
}

/// A deferred authenticator (see [`GcpAuthConfig::build_deferred`]) could not get a token, so
/// its component runs without one and retries. Counted as a component error, so refused
/// credentials show up in `component_errors_total`.
#[derive(Debug, NamedInternalEvent)]
struct GcpAuthTokenDeferredError<'a> {
    error: &'a str,
    retry_in: Duration,
}

impl InternalEvent for GcpAuthTokenDeferredError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to get GCP authentication token; running without one and retrying.",
            error = %self.error,
            retry_in_secs = self.retry_in.as_secs_f64(),
            error_code = "failed_getting_token",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_getting_token",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
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

async fn get_token_implicit(url: &str) -> Result<Token, GcpError> {
    debug!("Fetching implicit GCP authentication token.");
    let req = http::Request::get(url)
        .header("Metadata-Flavor", "Google")
        .body(hyper::Body::empty())
        .unwrap();

    let proxy = ProxyConfig::from_env();
    let res = HttpClient::new(None, &proxy)
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

/// A fake GCE metadata server for tests of deferred authentication.
#[cfg(test)]
pub(crate) mod test_util {
    use std::{
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use hyper::{
        Body, Response, Server, StatusCode,
        service::{make_service_fn, service_fn},
    };
    use tokio::time::Duration;

    use crate::test_util::addr::{PortGuard, next_addr};

    pub(crate) struct FakeMetadata {
        pub(crate) url: String,
        requests: Arc<AtomicUsize>,
        _port: PortGuard,
    }

    impl FakeMetadata {
        /// Refuses the first `refusals` token requests the way the token endpoint refuses
        /// revoked credentials (403 with an OAuth error), then serves token `token-<n>` for the
        /// n-th request.
        pub(crate) fn start(refusals: usize) -> Self {
            Self::start_with(refusals, None)
        }

        /// Answers the first token request only after `delay` (a hanging endpoint), then serves
        /// tokens right away.
        pub(crate) fn start_hanging_first(delay: Duration) -> Self {
            Self::start_with(0, Some(delay))
        }

        fn start_with(refusals: usize, first_delay: Option<Duration>) -> Self {
            let (port, addr) = next_addr();
            let requests = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&requests);
            let make_service = make_service_fn(move |_| {
                let counter = Arc::clone(&counter);
                async move {
                    Ok::<_, Infallible>(service_fn(move |request: hyper::Request<Body>| {
                        let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                        let flavored = request
                            .headers()
                            .get("Metadata-Flavor")
                            .is_some_and(|value| value == "Google");
                        let (status, body) = if !flavored {
                            (
                                StatusCode::BAD_REQUEST,
                                "missing Metadata-Flavor".to_string(),
                            )
                        } else if n <= refusals {
                            (
                                StatusCode::FORBIDDEN,
                                r#"{"error":"access_denied","error_description":"refused by the test"}"#
                                    .to_string(),
                            )
                        } else {
                            (
                                StatusCode::OK,
                                format!(
                                    r#"{{"access_token":"token-{n}","token_type":"Bearer","expires_in":3599}}"#
                                ),
                            )
                        };
                        let response = Response::builder()
                            .status(status)
                            .body(Body::from(body))
                            .unwrap();
                        let delay = first_delay.filter(|_| n == 1);
                        async move {
                            if let Some(delay) = delay {
                                tokio::time::sleep(delay).await;
                            }
                            Ok::<_, Infallible>(response)
                        }
                    }))
                }
            });
            tokio::spawn(Server::bind(&addr).serve(make_service));
            Self {
                url: format!("http://{addr}/token"),
                requests,
                _port: port,
            }
        }

        pub(crate) fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
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

    /// A refused first token does not fail a deferred build: the authenticator starts without
    /// a token and gets one, with backoff (2 s, then 4 s), once the credentials work.
    #[tokio::test]
    async fn deferred_implicit_token_is_retried_until_it_works() {
        let metadata = test_util::FakeMetadata::start(2);
        let auth =
            GcpAuthenticator::new_implicit_deferred(metadata.url.clone(), DEFERRED_TOKEN_TIMEOUT)
                .await
                .expect("a refused token must not fail the build");
        assert!(auth.awaiting_token());
        assert_eq!(auth.make_token(), None);
        let mut request = http::Request::get("http://example.com").body(()).unwrap();
        auth.apply(&mut request);
        assert!(request.headers().get(AUTHORIZATION).is_none());

        let start = tokio::time::Instant::now();
        let mut regenerated = auth.spawn_regenerate_token();
        tokio::time::timeout(Duration::from_secs(30), regenerated.changed())
            .await
            .expect("no token after the credentials started working")
            .unwrap();
        assert!(!auth.awaiting_token());
        assert_eq!(auth.make_token().as_deref(), Some("Bearer token-3"));
        assert_eq!(metadata.requests(), 3);
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(6),
            "retried too fast: {elapsed:?}"
        );
    }

    /// A token endpoint that hangs does not hang the build: the first fetch times out, the
    /// authenticator starts without a token, and the retries get one once the endpoint answers.
    #[tokio::test]
    async fn deferred_implicit_token_endpoint_hanging_does_not_hang_the_build() {
        let metadata = test_util::FakeMetadata::start_hanging_first(Duration::from_secs(30));
        let start = tokio::time::Instant::now();
        let auth = GcpAuthenticator::new_implicit_deferred(
            metadata.url.clone(),
            Duration::from_millis(500),
        )
        .await
        .expect("a hanging token endpoint must not fail the build");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "build waited {:?}",
            start.elapsed()
        );
        assert!(auth.awaiting_token());

        let mut regenerated = auth.spawn_regenerate_token();
        tokio::time::timeout(Duration::from_secs(30), regenerated.changed())
            .await
            .expect("no token after the endpoint started answering")
            .unwrap();
        assert_eq!(auth.make_token().as_deref(), Some("Bearer token-2"));
    }

    #[tokio::test]
    async fn deferred_implicit_with_a_working_token_starts_with_it() {
        let metadata = test_util::FakeMetadata::start(0);
        let auth =
            GcpAuthenticator::new_implicit_deferred(metadata.url.clone(), DEFERRED_TOKEN_TIMEOUT)
                .await
                .unwrap();
        assert!(!auth.awaiting_token());
        assert_eq!(auth.make_token().as_deref(), Some("Bearer token-1"));
    }

    #[tokio::test]
    async fn deferred_build_still_fails_on_bad_configuration() {
        let error = build_deferred_auth(r#"api_key: "abc%xyz""#)
            .await
            .expect_err("a bad API key must fail the build");
        assert_downcast_matches!(error, GcpError, GcpError::InvalidApiKey { .. });

        let error = build_deferred_auth(r#"credentials_path: "/nonexistent/credentials.json""#)
            .await
            .expect_err("a missing credentials file must fail the build");
        assert_downcast_matches!(error, GcpError, GcpError::InvalidCredentials { .. });

        let path = crate::test_util::temp_file();
        std::fs::write(
            &path,
            serde_json::json!({
                "type": "service_account",
                "project_id": "project",
                "private_key_id": "id",
                "private_key": "not a key",
                "client_email": "vector@project.iam.gserviceaccount.com",
                "client_id": "1",
                "auth_uri": "http://127.0.0.1:1/auth",
                "token_uri": "http://127.0.0.1:1/token",
                "auth_provider_x509_cert_url": "http://127.0.0.1:1/certs",
                "client_x509_cert_url": "http://127.0.0.1:1/cert",
            })
            .to_string(),
        )
        .unwrap();
        let error = build_deferred_auth(&format!("credentials_path: {:?}", path.to_str().unwrap()))
            .await
            .expect_err("a malformed private key must fail the build");
        assert_downcast_matches!(error, GcpError, GcpError::InvalidRsaKey { .. });

        let auth = build_deferred_auth("skip_authentication: true")
            .await
            .unwrap();
        assert!(matches!(auth, GcpAuthenticator::None));
    }

    async fn build_deferred_auth(yaml: &str) -> crate::Result<GcpAuthenticator> {
        let config: GcpAuthConfig = serde_yaml::from_str(yaml).expect("Invalid YAML");
        config.build_deferred(Scope::PubSub).await
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
