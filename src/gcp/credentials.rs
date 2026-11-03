//! GCP credential files other than service-account keys (nano fork, NAN-4935).
//!
//! `goauth` parses only `service_account` keys, so a credentials file of any other `type`
//! failed with `missing field project_id`. Many organizations block service-account key
//! creation (the `iam.disableServiceAccountKeyCreation` org policy), which left them without
//! a way to authenticate outside GCE. This module adds the other Application Default
//! Credentials shapes that Google's client libraries accept from a file:
//!
//! * `authorized_user`: the OAuth refresh-token flow against `token_uri`, which is what
//!   `gcloud auth application-default login` writes.
//! * `external_account`: workload identity federation. The subject token comes from a file or
//!   a URL (text or JSON), is exchanged at the STS `token_url`, and is optionally exchanged
//!   again for a service-account token through `service_account_impersonation_url`.
//! * `impersonated_service_account` whose `source_credentials` are `authorized_user`: what
//!   `gcloud auth application-default login --impersonate-service-account` writes.
//!
//! None of these shapes carry a project id. Components already take the project from their
//! own config (for example `gcp_pubsub.project`), so nothing here reads one.
//!
//! Secrets (refresh tokens, client secrets, subject tokens, access tokens) never appear in
//! `Debug` output, log events or error messages. Parse errors report only the missing field
//! or the line and column, never serde's "invalid type: string ..." text, which can echo a
//! value.

use std::{collections::BTreeMap, fmt, time::Duration};

use base64::prelude::{BASE64_STANDARD, Engine as _};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use goauth::auth::Token;
use http_body_util::{BodyExt as _, Collected};
use serde::{Deserialize, de::DeserializeOwned};
use vector_http::{empty_body, full_body};

use super::GcpError;
use crate::{config::ProxyConfig, http::client_v1::HttpClient, tls::MaybeTlsSettings};

pub(super) const DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const TOKEN_EXCHANGE_GRANT: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
const ACCESS_TOKEN_TYPE: &str = "urn:ietf:params:oauth:token-type:access_token";
const DEFAULT_IMPERSONATION_LIFETIME_SECS: u64 = 3600;
/// Upper bound on a subject-token or token-endpoint response we are willing to buffer.
const MAX_RESPONSE_BYTES: usize = 1 << 20;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The `type` of a credentials JSON file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CredentialKind {
    ServiceAccount,
    AuthorizedUser,
    ExternalAccount,
    ImpersonatedServiceAccount,
}

impl CredentialKind {
    /// Classify a credentials file by its `type` field. A file without `type` is treated as a
    /// service-account key, which is what the previous (goauth-only) code assumed.
    pub(super) fn detect(raw: &str) -> Result<Self, GcpError> {
        #[derive(Deserialize)]
        struct Probe {
            #[serde(rename = "type")]
            kind: Option<String>,
        }
        let probe: Probe = parse("credentials", raw)?;
        match probe.kind.as_deref() {
            None | Some("service_account") => Ok(Self::ServiceAccount),
            Some("authorized_user") => Ok(Self::AuthorizedUser),
            Some("external_account") => Ok(Self::ExternalAccount),
            Some("impersonated_service_account") => Ok(Self::ImpersonatedServiceAccount),
            Some(other) => Err(GcpError::UnsupportedCredentials {
                // `type` is a short enum-like string, not a secret.
                kind: other.chars().take(64).collect(),
            }),
        }
    }

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::ServiceAccount => "service_account",
            Self::AuthorizedUser => "authorized_user",
            Self::ExternalAccount => "external_account",
            Self::ImpersonatedServiceAccount => "impersonated_service_account",
        }
    }
}

/// Deserialize credential JSON without letting serde's message echo a value.
pub(super) fn parse<T: DeserializeOwned>(kind: &'static str, raw: &str) -> Result<T, GcpError> {
    serde_json::from_str(raw).map_err(|error| GcpError::MalformedCredentials {
        kind,
        reason: redacted_reason(&error),
    })
}

fn redacted_reason(error: &serde_json::Error) -> String {
    let text = error.to_string();
    // "missing field `x` at line ..": names a field, never a value.
    if text.starts_with("missing field") {
        return text;
    }
    match error.classify() {
        serde_json::error::Category::Syntax | serde_json::error::Category::Eof => format!(
            "not valid JSON (line {}, column {})",
            error.line(),
            error.column()
        ),
        _ => format!(
            "unexpected value shape (line {}, column {})",
            error.line(),
            error.column()
        ),
    }
}

/// A secret string whose `Debug` never prints the value.
#[derive(Clone, Deserialize, Default)]
#[serde(transparent)]
pub(super) struct Secret(String);

impl Secret {
    pub(super) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("**REDACTED**")
    }
}

/// `type: authorized_user`, as written by `gcloud auth application-default login`.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct AuthorizedUser {
    client_id: String,
    client_secret: Secret,
    refresh_token: Secret,
    #[serde(default)]
    token_uri: Option<String>,
    #[serde(default)]
    pub(super) quota_project_id: Option<String>,
}

impl AuthorizedUser {
    pub(super) fn from_json(raw: &str) -> Result<Self, GcpError> {
        let creds: Self = parse("authorized_user", raw)?;
        if creds.refresh_token.expose().is_empty() {
            return Err(GcpError::MalformedCredentials {
                kind: "authorized_user",
                reason: "refresh_token is empty".into(),
            });
        }
        Ok(creds)
    }

    fn token_uri(&self) -> &str {
        self.token_uri
            .as_deref()
            .filter(|uri| !uri.is_empty())
            .unwrap_or(DEFAULT_TOKEN_URI)
    }

    /// Exchange the refresh token for an access token.
    ///
    /// No `scope` is sent: a refresh grant may only narrow the scopes the user consented to,
    /// and Google rejects a narrower scope for ADC with `invalid_scope`. ADC is granted
    /// `cloud-platform`, which covers every GCP component in Vector.
    pub(super) async fn fetch_token(&self) -> Result<Token, GcpError> {
        debug!(
            message = "Fetching GCP authentication token.",
            credential_type = "authorized_user",
            token_uri = %self.token_uri(),
        );
        let body = form(&[
            ("grant_type", "refresh_token"),
            ("client_id", &self.client_id),
            ("client_secret", self.client_secret.expose()),
            ("refresh_token", self.refresh_token.expose()),
        ]);
        let response: OAuthTokenResponse = post_form(self.token_uri(), body, None).await?;
        response.into_token()
    }
}

/// `type: external_account` (workload identity federation).
#[derive(Clone, Debug, Deserialize)]
pub(super) struct ExternalAccount {
    audience: String,
    subject_token_type: String,
    token_url: String,
    credential_source: CredentialSource,
    #[serde(default)]
    service_account_impersonation_url: Option<String>,
    #[serde(default)]
    service_account_impersonation: Option<ImpersonationOptions>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    client_secret: Option<Secret>,
    #[serde(default)]
    workforce_pool_user_project: Option<String>,
    #[serde(default)]
    pub(super) quota_project_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct ImpersonationOptions {
    #[serde(default)]
    token_lifetime_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct CredentialSource {
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    format: Option<SubjectTokenFormat>,
    #[serde(default)]
    environment_id: Option<String>,
    #[serde(default)]
    executable: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Deserialize)]
struct SubjectTokenFormat {
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    subject_token_field_name: Option<String>,
}

impl ExternalAccount {
    pub(super) fn from_json(raw: &str) -> Result<Self, GcpError> {
        let creds: Self = parse("external_account", raw)?;
        let source = &creds.credential_source;
        if source.environment_id.is_some() {
            return Err(GcpError::UnsupportedCredentials {
                kind: "external_account with an AWS credential_source (environment_id)".into(),
            });
        }
        if source.executable.is_some() {
            return Err(GcpError::UnsupportedCredentials {
                kind: "external_account with an executable credential_source".into(),
            });
        }
        if source.file.is_some() == source.url.is_some() {
            return Err(GcpError::MalformedCredentials {
                kind: "external_account",
                reason: "credential_source must set exactly one of file or url".into(),
            });
        }
        if let Some(format) = &source.format {
            match format.kind.as_deref() {
                None | Some("text") => {}
                Some("json") if format.subject_token_field_name.is_some() => {}
                Some("json") => {
                    return Err(GcpError::MalformedCredentials {
                        kind: "external_account",
                        reason: "credential_source.format type json needs subject_token_field_name"
                            .into(),
                    });
                }
                Some(_) => {
                    return Err(GcpError::MalformedCredentials {
                        kind: "external_account",
                        reason: "credential_source.format type must be text or json".into(),
                    });
                }
            }
        }
        Ok(creds)
    }

    /// Read the subject token (the external identity's credential) from the configured source.
    async fn subject_token(&self) -> Result<Secret, GcpError> {
        let source = &self.credential_source;
        let raw = if let Some(path) = &source.file {
            tokio::fs::read_to_string(path)
                .await
                .map_err(|error| GcpError::SubjectToken {
                    reason: format!("cannot read credential_source.file {path:?}: {error}"),
                })?
        } else {
            let url = source.url.as_deref().unwrap_or_default();
            let mut builder = http_1::Request::get(url);
            for (name, value) in &source.headers {
                builder = builder.header(name.as_str(), value.as_str());
            }
            let request = builder
                .body(empty_body())
                .map_err(|error| GcpError::SubjectToken {
                    reason: format!("invalid credential_source.url request: {error}"),
                })?;
            let (status, bytes) = send(request).await?;
            if !status.is_success() {
                return Err(GcpError::SubjectToken {
                    reason: format!("credential_source.url returned HTTP {status}"),
                });
            }
            String::from_utf8(bytes.to_vec()).map_err(|_| GcpError::SubjectToken {
                reason: "credential_source.url returned a non-UTF-8 body".into(),
            })?
        };

        let token = match &source.format {
            Some(SubjectTokenFormat {
                kind: Some(kind),
                subject_token_field_name: Some(field),
            }) if kind == "json" => {
                let value: serde_json::Value =
                    serde_json::from_str(&raw).map_err(|_| GcpError::SubjectToken {
                        reason: "subject token source is not valid JSON".into(),
                    })?;
                value
                    .get(field)
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| GcpError::SubjectToken {
                        reason: format!("subject token JSON has no string field {field:?}"),
                    })?
            }
            _ => raw.trim().to_owned(),
        };
        if token.is_empty() {
            return Err(GcpError::SubjectToken {
                reason: "subject token is empty".into(),
            });
        }
        Ok(Secret(token))
    }

    pub(super) async fn fetch_token(&self, scope_url: &str) -> Result<Token, GcpError> {
        debug!(
            message = "Fetching GCP authentication token.",
            credential_type = "external_account",
            token_url = %self.token_url,
            impersonation = self.service_account_impersonation_url.is_some(),
        );
        let subject = self.subject_token().await?;
        // With impersonation the STS token only needs to call IAM Credentials, which takes
        // cloud-platform; the component scope is requested on the impersonated token.
        let sts_scope = if self.service_account_impersonation_url.is_some() {
            CLOUD_PLATFORM_SCOPE
        } else {
            scope_url
        };
        let client_auth = match (&self.client_id, &self.client_secret) {
            (Some(id), Some(secret)) => Some(basic_auth(id, secret.expose())),
            _ => None,
        };
        // Workforce pools bill the user project when there is no client authentication.
        let options = match (&self.workforce_pool_user_project, &client_auth) {
            (Some(project), None) => {
                Some(serde_json::json!({ "userProject": project }).to_string())
            }
            _ => None,
        };
        let mut fields = vec![
            ("grant_type", TOKEN_EXCHANGE_GRANT),
            ("audience", self.audience.as_str()),
            ("scope", sts_scope),
            ("requested_token_type", ACCESS_TOKEN_TYPE),
            ("subject_token", subject.expose()),
            ("subject_token_type", self.subject_token_type.as_str()),
        ];
        if let Some(options) = &options {
            fields.push(("options", options));
        }
        let sts: OAuthTokenResponse =
            post_form(&self.token_url, form(&fields), client_auth.as_deref()).await?;
        let sts = sts.into_token()?;

        match &self.service_account_impersonation_url {
            None => Ok(sts),
            Some(url) => {
                let lifetime = self
                    .service_account_impersonation
                    .as_ref()
                    .and_then(|o| o.token_lifetime_seconds)
                    .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS);
                impersonate(url, sts.access_token(), scope_url, lifetime).await
            }
        }
    }
}

/// `type: impersonated_service_account`, with `authorized_user` source credentials.
#[derive(Clone, Debug)]
pub(super) struct ImpersonatedServiceAccount {
    source: AuthorizedUser,
    url: String,
    pub(super) quota_project_id: Option<String>,
}

impl ImpersonatedServiceAccount {
    pub(super) fn from_json(raw: &str) -> Result<Self, GcpError> {
        #[derive(Deserialize)]
        struct Raw {
            service_account_impersonation_url: String,
            source_credentials: serde_json::Value,
            #[serde(default)]
            quota_project_id: Option<String>,
        }
        let parsed: Raw = parse("impersonated_service_account", raw)?;
        let source_raw = parsed.source_credentials.to_string();
        match CredentialKind::detect(&source_raw)? {
            CredentialKind::AuthorizedUser => {}
            other => {
                return Err(GcpError::UnsupportedCredentials {
                    kind: format!(
                        "impersonated_service_account with {} source_credentials",
                        other.as_str()
                    ),
                });
            }
        }
        Ok(Self {
            source: AuthorizedUser::from_json(&source_raw)?,
            url: parsed.service_account_impersonation_url,
            quota_project_id: parsed.quota_project_id,
        })
    }

    pub(super) async fn fetch_token(&self, scope_url: &str) -> Result<Token, GcpError> {
        let source = self.source.fetch_token().await?;
        impersonate(
            &self.url,
            source.access_token(),
            scope_url,
            DEFAULT_IMPERSONATION_LIFETIME_SECS,
        )
        .await
    }
}

/// `POST .../serviceAccounts/<email>:generateAccessToken`.
async fn impersonate(
    url: &str,
    bearer: &str,
    scope_url: &str,
    lifetime_secs: u64,
) -> Result<Token, GcpError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Response {
        access_token: Secret,
        expire_time: String,
    }
    debug!(message = "Impersonating GCP service account.", url = %url);
    let body = serde_json::json!({
        "scope": [scope_url],
        "lifetime": format!("{lifetime_secs}s"),
    })
    .to_string();
    let request = http_1::Request::post(url)
        .header(http_1::header::CONTENT_TYPE, "application/json")
        .header(http_1::header::AUTHORIZATION, format!("Bearer {bearer}"))
        .body(full_body(Bytes::from(body)))
        .map_err(|error| GcpError::TokenEndpoint {
            endpoint: url.into(),
            status: 0,
            error: format!("invalid request: {error}"),
        })?;
    let (status, bytes) = send(request).await?;
    if !status.is_success() {
        return Err(endpoint_error(url, status, &bytes));
    }
    let response: Response =
        serde_json::from_slice(&bytes).map_err(|error| GcpError::TokenEndpoint {
            endpoint: url.into(),
            status: status.as_u16(),
            error: redacted_reason(&error),
        })?;
    let expires_at = DateTime::parse_from_rfc3339(&response.expire_time).map_err(|_| {
        GcpError::TokenEndpoint {
            endpoint: url.into(),
            status: status.as_u16(),
            error: "expireTime is not RFC 3339".into(),
        }
    })?;
    let expires_in = (expires_at.with_timezone(&Utc) - Utc::now())
        .num_seconds()
        .max(0);
    make_token(
        response.access_token.expose(),
        "Bearer",
        u32::try_from(expires_in).unwrap_or(u32::MAX),
    )
}

/// The OAuth 2.0 token response shared by the refresh grant and STS.
#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: Secret,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<u32>,
}

impl OAuthTokenResponse {
    fn into_token(self) -> Result<Token, GcpError> {
        make_token(
            self.access_token.expose(),
            self.token_type.as_deref().unwrap_or("Bearer"),
            // STS may omit expires_in when it mirrors the subject token; Google documents an
            // hour as the default lifetime.
            self.expires_in.unwrap_or(3600),
        )
    }
}

/// goauth's `Token` has private fields; it is built through its `Deserialize` impl.
pub(super) fn make_token(
    access_token: &str,
    token_type: &str,
    expires_in: u32,
) -> Result<Token, GcpError> {
    serde_json::from_value(serde_json::json!({
        "access_token": access_token,
        "token_type": token_type,
        "expires_in": expires_in,
    }))
    .map_err(|_| GcpError::TokenEndpoint {
        endpoint: String::new(),
        status: 0,
        error: "could not build token".into(),
    })
}

fn form(fields: &[(&str, &str)]) -> String {
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(fields)
        .finish()
}

fn basic_auth(id: &str, secret: &str) -> String {
    format!("Basic {}", BASE64_STANDARD.encode(format!("{id}:{secret}")))
}

async fn post_form<T: DeserializeOwned>(
    url: &str,
    body: String,
    authorization: Option<&str>,
) -> Result<T, GcpError> {
    let mut builder = http_1::Request::post(url).header(
        http_1::header::CONTENT_TYPE,
        "application/x-www-form-urlencoded",
    );
    if let Some(authorization) = authorization {
        builder = builder.header(http_1::header::AUTHORIZATION, authorization);
    }
    let request = builder
        .body(full_body(Bytes::from(body)))
        .map_err(|error| GcpError::TokenEndpoint {
            endpoint: url.into(),
            status: 0,
            error: format!("invalid request: {error}"),
        })?;
    let (status, bytes) = send(request).await?;
    if !status.is_success() {
        return Err(endpoint_error(url, status, &bytes));
    }
    serde_json::from_slice(&bytes).map_err(|error| GcpError::TokenEndpoint {
        endpoint: url.into(),
        status: status.as_u16(),
        error: redacted_reason(&error),
    })
}

/// Report a token-endpoint failure by its OAuth `error` / `error_description` (or Google API
/// `error.status` / `error.message`) only. The body is never echoed wholesale.
fn endpoint_error(url: &str, status: http_1::StatusCode, bytes: &[u8]) -> GcpError {
    let value: Option<serde_json::Value> = serde_json::from_slice(bytes).ok();
    let field =
        |v: &serde_json::Value, k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
    let error = value
        .as_ref()
        .and_then(|v| match v.get("error") {
            Some(serde_json::Value::String(code)) => Some(match field(v, "error_description") {
                Some(desc) => format!("{code}: {desc}"),
                None => code.clone(),
            }),
            Some(obj @ serde_json::Value::Object(_)) => {
                let status = field(obj, "status").unwrap_or_default();
                let message = field(obj, "message").unwrap_or_default();
                Some(format!("{status}: {message}"))
            }
            _ => None,
        })
        .map_or_else(
            || "no OAuth error in response".to_owned(),
            |text| text.chars().take(300).collect(),
        );
    GcpError::TokenEndpoint {
        endpoint: url.into(),
        status: status.as_u16(),
        error,
    }
}

async fn send(
    request: http_1::Request<vector_http::RequestBody>,
) -> Result<(http_1::StatusCode, Bytes), GcpError> {
    let endpoint = request.uri().to_string();
    let proxy = ProxyConfig::from_env();
    let tls = MaybeTlsSettings::enable_client().map_err(|error| GcpError::TokenEndpoint {
        endpoint: endpoint.clone(),
        status: 0,
        error: format!("TLS setup failed: {error}"),
    })?;
    let client =
        HttpClient::new(tls, &proxy).map_err(|source| GcpError::BuildHttpClient { source })?;
    let response = tokio::time::timeout(REQUEST_TIMEOUT, client.send(request))
        .await
        .map_err(|_| GcpError::TokenEndpoint {
            endpoint: endpoint.clone(),
            status: 0,
            error: "timed out".into(),
        })?
        .map_err(|error| GcpError::TokenEndpoint {
            endpoint: endpoint.clone(),
            status: 0,
            error: format!("request failed: {error}"),
        })?;
    let status = response.status();
    let body = http_body_util::Limited::new(response.into_body(), MAX_RESPONSE_BYTES);
    let bytes =
        body.collect()
            .await
            .map(Collected::to_bytes)
            .map_err(|_| GcpError::TokenEndpoint {
                endpoint,
                status: status.as_u16(),
                error: "could not read response body".into(),
            })?;
    Ok((status, bytes))
}

#[cfg(test)]
#[path = "credentials_tests.rs"]
mod tests;
