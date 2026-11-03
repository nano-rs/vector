//! Tests for non-service-account GCP credentials (NAN-4935).

use std::io::Write as _;

use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_string_contains, header, method, path},
};

use super::*;
use crate::gcp::{GcpAuthenticator, Scope, egress::test_support};

/// A wiremock server exempted (on this thread only) from the https/googleapis host rule.
async fn mock() -> MockServer {
    let server = MockServer::start().await;
    test_support::allow_mock_server(&server.uri());
    server
}

const REFRESH_TOKEN: &str = "1//refresh-SECRET-value";
const CLIENT_SECRET: &str = "client-SECRET-value";
const SUBJECT_TOKEN: &str = "subject-SECRET-jwt";
const IMPERSONATION_PATH: &str =
    "/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken";

fn authorized_user_json(token_uri: Option<&str>, quota: Option<&str>) -> String {
    let mut v = serde_json::json!({
        "type": "authorized_user",
        "client_id": "123.apps.googleusercontent.com",
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "account": "",
        "universe_domain": "googleapis.com",
    });
    if let Some(uri) = token_uri {
        v["token_uri"] = uri.into();
    }
    if let Some(q) = quota {
        v["quota_project_id"] = q.into();
    }
    v.to_string()
}

fn token_body(token: &str) -> serde_json::Value {
    serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": 3599,
        "scope": "https://www.googleapis.com/auth/cloud-platform",
    })
}

fn temp_file(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn assert_no_secrets(text: &str) {
    for secret in [
        REFRESH_TOKEN,
        CLIENT_SECRET,
        SUBJECT_TOKEN,
        "SECRET",
        "ya29.",
    ] {
        assert!(!text.contains(secret), "secret leaked into: {text}");
    }
}

#[test]
fn detects_every_supported_kind() {
    let kind = |raw: &str| CredentialKind::detect(raw).unwrap();
    assert_eq!(
        kind(r#"{"type":"service_account"}"#),
        CredentialKind::ServiceAccount
    );
    // No type: what the goauth-only code assumed.
    assert_eq!(
        kind(r#"{"project_id":"p"}"#),
        CredentialKind::ServiceAccount
    );
    assert_eq!(
        kind(r#"{"type":"authorized_user"}"#),
        CredentialKind::AuthorizedUser
    );
    assert_eq!(
        kind(r#"{"type":"external_account"}"#),
        CredentialKind::ExternalAccount
    );
    assert_eq!(
        kind(r#"{"type":"impersonated_service_account"}"#),
        CredentialKind::ImpersonatedServiceAccount
    );
}

#[test]
fn unknown_kind_is_refused_by_name() {
    let error = CredentialKind::detect(r#"{"type":"gdch_service_account"}"#).unwrap_err();
    assert!(
        matches!(error, GcpError::UnsupportedCredentials { ref kind } if kind == "gdch_service_account")
    );
}

#[test]
fn authorized_user_missing_field_names_the_field() {
    let raw = r#"{"type":"authorized_user","client_id":"x","client_secret":"y"}"#;
    let error = AuthorizedUser::from_json(raw).unwrap_err().to_string();
    assert!(error.contains("missing field `refresh_token`"), "{error}");
}

#[test]
fn authorized_user_parse_errors_never_echo_values() {
    // serde would say `invalid type: string "1//refresh-SECRET-value", expected u32`-style text.
    let raw = format!(
        r#"{{"type":"authorized_user","client_id":{{"x":1}},"client_secret":"{CLIENT_SECRET}","refresh_token":"{REFRESH_TOKEN}"}}"#
    );
    let error = AuthorizedUser::from_json(&raw).unwrap_err().to_string();
    assert_no_secrets(&error);
    let raw = format!(r#"{{"type":"authorized_user","refresh_token":"{REFRESH_TOKEN}"#);
    let error = AuthorizedUser::from_json(&raw).unwrap_err().to_string();
    assert!(error.contains("not valid JSON"), "{error}");
    assert_no_secrets(&error);
}

#[test]
fn debug_output_redacts_secrets() {
    let creds = AuthorizedUser::from_json(&authorized_user_json(None, None)).unwrap();
    assert_no_secrets(&format!("{creds:?}"));
}

#[tokio::test]
async fn authorized_user_refresh_flow() {
    let server = mock().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(header("content-type", "application/x-www-form-urlencoded"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains(
            "refresh_token=1%2F%2Frefresh-SECRET-value",
        ))
        .and(body_string_contains("client_secret=client-SECRET-value"))
        .and(body_string_contains(
            "client_id=123.apps.googleusercontent.com",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.user")))
        .expect(1)
        .mount(&server)
        .await;

    let uri = format!("{}/token", server.uri());
    let creds = AuthorizedUser::from_json(&authorized_user_json(Some(&uri), None)).unwrap();
    let token = creds.fetch_token().await.unwrap();
    assert_eq!(token.access_token(), "ya29.user");
    assert_eq!(token.token_type(), "Bearer");
    assert_eq!(token.expires_in(), 3599);

    // A refresh grant must not narrow scopes: Google answers invalid_scope for ADC.
    let requests = server.received_requests().await.unwrap();
    let body = String::from_utf8(requests[0].body.clone()).unwrap();
    assert!(!body.contains("scope="), "{body}");
}

#[test]
fn authorized_user_defaults_to_googles_token_endpoint() {
    let creds = AuthorizedUser::from_json(&authorized_user_json(None, None)).unwrap();
    assert_eq!(creds.token_uri(), DEFAULT_TOKEN_URI);
}

#[tokio::test]
async fn token_endpoint_error_reports_oauth_error_only() {
    let server = mock().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid_grant",
            "error_description": "Token has been expired or revoked.",
        })))
        .mount(&server)
        .await;
    let uri = format!("{}/token", server.uri());
    let creds = AuthorizedUser::from_json(&authorized_user_json(Some(&uri), None)).unwrap();
    let error = creds.fetch_token().await.unwrap_err().to_string();
    assert!(error.contains("HTTP 400"), "{error}");
    assert!(
        error.contains("invalid_grant: Token has been expired or revoked."),
        "{error}"
    );
    assert_no_secrets(&error);
}

#[tokio::test]
async fn authenticator_from_authorized_user_file() {
    let server = mock().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.file")))
        .mount(&server)
        .await;
    let uri = format!("{}/token", server.uri());
    let file = temp_file(&authorized_user_json(Some(&uri), Some("billing-proj")));

    let auth = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .expect("authorized_user credentials build");
    assert_eq!(auth.make_token().as_deref(), Some("Bearer ya29.file"));
    assert_eq!(auth.quota_project(), Some("billing-proj"));

    let mut request = http_1::Request::get("https://pubsub.googleapis.com/v1/x")
        .body(())
        .unwrap();
    auth.apply_v1(&mut request);
    assert_eq!(request.headers()["x-goog-user-project"], "billing-proj");
    assert_eq!(request.headers()["authorization"], "Bearer ya29.file");

    // Regeneration goes through the same refresh flow.
    let crate::gcp::GcpAuthenticator::Credentials(inner) = &auth else {
        panic!("expected credentials")
    };
    inner.regenerate_token().await.unwrap();
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_no_secrets(&format!("{auth:?}"));
}

#[tokio::test]
async fn no_quota_header_without_quota_project() {
    let server = mock().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.q")))
        .mount(&server)
        .await;
    let uri = format!("{}/token", server.uri());
    let file = temp_file(&authorized_user_json(Some(&uri), None));
    let auth = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .unwrap();
    let mut request = http_1::Request::get("https://x/").body(()).unwrap();
    auth.apply_v1(&mut request);
    assert!(!request.headers().contains_key("x-goog-user-project"));
}

#[tokio::test]
async fn missing_credentials_file_is_a_read_error() {
    let error = GcpAuthenticator::from_file("/nonexistent/nano-4935.json", Scope::PubSub)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Cannot read GCP credentials file"),
        "{error}"
    );
}

fn external_account_json(server: &str, source: &serde_json::Value, impersonate: bool) -> String {
    let mut v = serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/q",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": format!("{server}/v1/token"),
        "credential_source": source.clone(),
    });
    if impersonate {
        v["service_account_impersonation_url"] = format!(
            "{server}/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken"
        )
        .into();
        v["service_account_impersonation"] = serde_json::json!({"token_lifetime_seconds": 1800});
    }
    v.to_string()
}

#[tokio::test]
async fn external_account_file_source_without_impersonation() {
    let server = mock().await;
    Mock::given(method("POST"))
        .and(path("/v1/token"))
        .and(body_string_contains(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange",
        ))
        .and(body_string_contains("subject_token=subject-SECRET-jwt"))
        .and(body_string_contains(
            "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fpubsub",
        ))
        .and(body_string_contains(
            "requested_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "ya29.sts",
            "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "token_type": "Bearer",
            "expires_in": 3600,
        })))
        .expect(1)
        .mount(&server)
        .await;
    let root = tempfile::tempdir().unwrap();
    test_support::set_file_root(Some(root.path().to_path_buf()));
    let subject = root.path().join("token");
    std::fs::write(&subject, format!("{SUBJECT_TOKEN}\n")).unwrap();
    let raw = external_account_json(
        &server.uri(),
        &serde_json::json!({"file": subject.to_str().unwrap()}),
        false,
    );
    let file = temp_file(&raw);
    let auth = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .expect("external_account builds");
    assert_eq!(auth.make_token().as_deref(), Some("Bearer ya29.sts"));
}

#[tokio::test]
async fn external_account_url_json_source_with_impersonation() {
    let server = mock().await;
    Mock::given(method("GET"))
        .and(path("/subject"))
        .and(header("authorization", "Bearer hdr-SECRET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "access_token": SUBJECT_TOKEN })),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/token"))
        .and(body_string_contains("subject_token=subject-SECRET-jwt"))
        // The STS token only calls IAM Credentials; the component scope goes on the SA token.
        .and(body_string_contains(
            "scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.sts")))
        .expect(1)
        .mount(&server)
        .await;
    let expire = (Utc::now() + chrono::Duration::seconds(1800)).to_rfc3339();
    Mock::given(method("POST"))
        .and(path(
            "/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken",
        ))
        .and(header("authorization", "Bearer ya29.sts"))
        .and(body_string_contains(
            "https://www.googleapis.com/auth/pubsub",
        ))
        .and(body_string_contains("\"lifetime\":\"1800s\""))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": "ya29.sa",
            "expireTime": expire,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let raw = external_account_json(
        &server.uri(),
        &serde_json::json!({
            "url": format!("{}/subject", server.uri()),
            "headers": {"Authorization": "Bearer hdr-SECRET"},
            "format": {"type": "json", "subject_token_field_name": "access_token"},
        }),
        true,
    );
    let creds = ExternalAccount::from_json(&raw).unwrap();
    let token = creds
        .fetch_token(&Scope::PubSub.url())
        .await
        .expect("impersonated token");
    assert_eq!(token.access_token(), "ya29.sa");
    assert!(
        (1700..=1800).contains(&token.expires_in()),
        "{}",
        token.expires_in()
    );
    assert_no_secrets(&format!("{creds:?}"));
}

#[test]
fn external_account_refuses_unsupported_sources() {
    let aws = external_account_json(
        "https://sts.googleapis.com",
        &serde_json::json!({"environment_id": "aws1", "region_url": "http://169.254.169.254"}),
        false,
    );
    assert!(matches!(
        ExternalAccount::from_json(&aws).unwrap_err(),
        GcpError::UnsupportedCredentials { .. }
    ));
    let exe = external_account_json(
        "https://sts.googleapis.com",
        &serde_json::json!({"executable": {"command": "/bin/true"}}),
        false,
    );
    assert!(matches!(
        ExternalAccount::from_json(&exe).unwrap_err(),
        GcpError::UnsupportedCredentials { .. }
    ));
    let both = external_account_json(
        "https://sts.googleapis.com",
        &serde_json::json!({"file": "/a", "url": "http://b"}),
        false,
    );
    assert!(matches!(
        ExternalAccount::from_json(&both).unwrap_err(),
        GcpError::MalformedCredentials { .. }
    ));
}

#[tokio::test]
async fn impersonated_service_account_from_authorized_user() {
    let server = mock().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(token_body("ya29.user")))
        .mount(&server)
        .await;
    let expire = (Utc::now() + chrono::Duration::seconds(3600)).to_rfc3339();
    Mock::given(method("POST"))
        .and(path(IMPERSONATION_PATH))
        .and(header("authorization", "Bearer ya29.user"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "accessToken": "ya29.impersonated",
            "expireTime": expire,
        })))
        .mount(&server)
        .await;
    let source: serde_json::Value = serde_json::from_str(&authorized_user_json(
        Some(&format!("{}/token", server.uri())),
        None,
    ))
    .unwrap();
    let raw = serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": format!("{}{IMPERSONATION_PATH}", server.uri()),
        "source_credentials": source,
        "delegates": [],
    })
    .to_string();
    let file = temp_file(&raw);
    let auth = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .expect("impersonated credentials build");
    assert_eq!(
        auth.make_token().as_deref(),
        Some("Bearer ya29.impersonated")
    );
}

#[test]
fn impersonated_service_account_refuses_other_sources() {
    let raw = serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": "https://iam/x",
        "source_credentials": {"type": "external_account"},
    })
    .to_string();
    assert!(matches!(
        ImpersonatedServiceAccount::from_json(&raw).unwrap_err(),
        GcpError::UnsupportedCredentials { .. }
    ));
}

#[tokio::test]
async fn service_account_parse_errors_never_echo_values() {
    // A type mismatch on a later field: serde's own message would quote the value.
    let raw = r#"{"type":"service_account","project_id":"p","private_key_id":"k","private_key":"-----SECRET-----","client_email":"a@b","client_id":"1","auth_uri":"u","token_uri":7,"auth_provider_x509_cert_url":"u","client_x509_cert_url":"u"}"#;
    let file = temp_file(raw);
    let error = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("Invalid GCP service_account credentials"),
        "{error}"
    );
    assert_no_secrets(&error);

    let file = temp_file(r#"{"type":"service_account","project_id":"p"}"#);
    let error = GcpAuthenticator::from_file(file.path().to_str().unwrap(), Scope::PubSub)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing field"), "{error}");
}

#[test]
fn authenticator_debug_never_prints_the_api_key() {
    let auth = GcpAuthenticator::ApiKey("AIza-SECRET-key".into());
    assert_eq!(format!("{auth:?}"), "ApiKey(**REDACTED**)");
}
