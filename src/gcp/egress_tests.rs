//! Egress policy tests (NAN-4935 review P1): credential files cannot reach local secrets or
//! non-Google endpoints.

use std::net::IpAddr;

use super::{
    GoogleEndpoint, check_google_endpoint, check_subject_url_form, is_public, read_subject_file,
    resolve_subject_url, test_support,
};
use crate::gcp::{
    GcpError, Scope, credentials::ExternalAccount, credentials::ImpersonatedServiceAccount,
};

fn refused(result: Result<impl std::fmt::Debug, GcpError>) -> String {
    match result {
        Err(error @ GcpError::EgressRefused { .. }) => error.to_string(),
        other => panic!("expected EgressRefused, got {other:?}"),
    }
}

#[test]
fn google_endpoints_accepted() {
    for (url, kind) in [
        (
            "https://oauth2.googleapis.com/token",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://sts.googleapis.com/v1/token",
            GoogleEndpoint::StsToken,
        ),
        // Regional / Private Service Connect forms are *.googleapis.com.
        (
            "https://sts.us-east1.rep.googleapis.com/v1/token",
            GoogleEndpoint::StsToken,
        ),
        (
            "https://oauth2-myendpoint.p.googleapis.com/token",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken",
            GoogleEndpoint::Impersonation,
        ),
    ] {
        check_google_endpoint(url, kind).unwrap_or_else(|e| panic!("{url}: {e}"));
    }
}

#[test]
fn non_google_endpoints_refused() {
    for (url, kind) in [
        (
            "http://oauth2.googleapis.com/token",
            GoogleEndpoint::OAuthToken,
        ),
        ("https://evil.example/token", GoogleEndpoint::OAuthToken),
        (
            "https://oauth2.googleapis.com.evil.example/token",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://evilgoogleapis.com/token",
            GoogleEndpoint::OAuthToken,
        ),
        ("https://142.250.64.10/token", GoogleEndpoint::OAuthToken),
        ("https://[2607:f8b0::1]/token", GoogleEndpoint::OAuthToken),
        ("https://127.0.0.1/token", GoogleEndpoint::OAuthToken),
        ("http://127.0.0.1:9/token", GoogleEndpoint::OAuthToken),
        (
            "https://user:pw@oauth2.googleapis.com/token",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://oauth2.googleapis.com:8443/token",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://oauth2.googleapis.com/token?x=1",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://oauth2.googleapis.com/other",
            GoogleEndpoint::OAuthToken,
        ),
        (
            "https://sts.googleapis.com/v1/oauthtoken",
            GoogleEndpoint::StsToken,
        ),
        ("https://evil.example/v1/token", GoogleEndpoint::StsToken),
        (
            "https://evil.example/v1/projects/-/serviceAccounts/sa@p.iam.gserviceaccount.com:generateAccessToken",
            GoogleEndpoint::Impersonation,
        ),
        (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa:signJwt",
            GoogleEndpoint::Impersonation,
        ),
        (
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/a/b:generateAccessToken",
            GoogleEndpoint::Impersonation,
        ),
    ] {
        assert!(check_google_endpoint(url, kind).is_err(), "{url} accepted");
    }
}

fn external(token_url: &str, source: &serde_json::Value, impersonation: Option<&str>) -> String {
    let mut v = serde_json::json!({
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/q",
        "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
        "token_url": token_url,
        "credential_source": source.clone(),
    });
    if let Some(url) = impersonation {
        v["service_account_impersonation_url"] = url.into();
    }
    v.to_string()
}

#[test]
fn metadata_url_with_attacker_token_url_refused() {
    test_support::set_metadata_allowed(Some(true));
    let raw = external(
        "https://attacker.example/v1/token",
        &serde_json::json!({
            "url": "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=x",
            "headers": {"Metadata-Flavor": "Google"},
        }),
        None,
    );
    let error = refused(ExternalAccount::from_json(&raw));
    assert!(error.contains("token_url"), "{error}");
    test_support::set_metadata_allowed(None);
}

#[test]
fn metadata_url_needs_operator_opt_in() {
    let url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity";
    test_support::set_metadata_allowed(Some(false));
    let error = refused(check_subject_url_form(url));
    assert!(error.contains("VECTOR_GCP_WIF_ALLOW_METADATA"), "{error}");
    refused(check_subject_url_form(
        "http://169.254.169.254/metadata/identity",
    ));
    test_support::set_metadata_allowed(Some(true));
    check_subject_url_form(url).expect("allowed under the opt-in");
    // Even under the opt-in, only plain http on port 80.
    refused(check_subject_url_form(
        "http://metadata.google.internal:8080/x",
    ));
    test_support::set_metadata_allowed(None);
}

#[test]
fn arbitrary_impersonation_url_refused() {
    let raw = external(
        "https://sts.googleapis.com/v1/token",
        &serde_json::json!({"file": "/var/run/secrets/nano-gcp-wif/token"}),
        Some("https://attacker.example/v1/projects/-/serviceAccounts/x:generateAccessToken"),
    );
    refused(ExternalAccount::from_json(&raw));

    let raw = serde_json::json!({
        "type": "impersonated_service_account",
        "service_account_impersonation_url": "https://attacker.example/collect",
        "source_credentials": {
            "type": "authorized_user", "client_id": "c", "client_secret": "s", "refresh_token": "r",
        },
    })
    .to_string();
    refused(ImpersonatedServiceAccount::from_json(&raw));
}

#[test]
fn authorized_user_attacker_token_uri_refused() {
    let raw = serde_json::json!({
        "type": "authorized_user", "client_id": "c", "client_secret": "s", "refresh_token": "r",
        "token_uri": "http://attacker.example/token",
    })
    .to_string();
    refused(crate::gcp::credentials::AuthorizedUser::from_json(&raw));
}

#[tokio::test]
async fn service_account_attacker_token_uri_refused() {
    let raw = serde_json::json!({
        "type": "service_account", "project_id": "p", "private_key_id": "k",
        "private_key": "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
        "client_email": "sa@p.iam.gserviceaccount.com", "client_id": "1",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://attacker.example/token",
        "auth_provider_x509_cert_url": "u", "client_x509_cert_url": "u",
    })
    .to_string();
    let error = crate::gcp::GcpAuthenticator::from_json(&raw, Scope::PubSub)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("token_uri refused"), "{error}");
}

#[test]
fn file_source_outside_root_refused() {
    let root = tempfile::tempdir().unwrap();
    test_support::set_file_root(Some(root.path().to_path_buf()));

    // /proc/self/environ (absent on macOS, refused either way), /dev/zero, k8s SA token.
    for path in [
        "/proc/self/environ",
        "/dev/zero",
        "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "/etc/hosts",
    ] {
        refused(read_subject_file(path));
    }

    // A symlink inside the root that points outside it is resolved and re-checked.
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(outside.path(), "secret-from-outside").unwrap();
    let link = root.path().join("link");
    std::os::unix::fs::symlink(outside.path(), &link).unwrap();
    let error = refused(read_subject_file(link.to_str().unwrap()));
    assert!(error.contains("must be under"), "{error}");

    // `..` escapes are refused lexically.
    let escape = format!("{}/../etc/hosts", root.path().display());
    refused(read_subject_file(&escape));
    refused(read_subject_file("relative/token"));

    // A regular file inside the root is read.
    let token = root.path().join("token");
    std::fs::write(&token, "jwt-value\n").unwrap();
    assert_eq!(
        read_subject_file(token.to_str().unwrap()).unwrap(),
        "jwt-value\n"
    );
    test_support::set_file_root(None);
}

#[test]
fn file_source_caps_and_requires_regular_files() {
    // Even when the operator points the root at /dev, /dev/zero is not a regular file.
    test_support::set_file_root(Some("/dev".into()));
    let error = refused(read_subject_file("/dev/zero"));
    assert!(error.contains("regular file"), "{error}");

    let root = tempfile::tempdir().unwrap();
    test_support::set_file_root(Some(root.path().to_path_buf()));
    let big = root.path().join("big");
    std::fs::write(&big, vec![b'a'; 64 * 1024 + 1]).unwrap();
    let error = refused(read_subject_file(big.to_str().unwrap()));
    assert!(error.contains("64 KiB"), "{error}");
    let dir = root.path().join("dir");
    std::fs::create_dir(&dir).unwrap();
    refused(read_subject_file(dir.to_str().unwrap()));
    test_support::set_file_root(None);
}

#[tokio::test]
async fn url_source_requires_https_public_dns_host() {
    test_support::set_metadata_allowed(Some(false));
    for url in [
        "http://idp.example.com/token",
        "https://10.0.0.5/token",
        "https://127.0.0.1/token",
        "https://[::1]/token",
        "https://169.254.169.254/token",
        "https://user:pw@idp.example.com/token",
    ] {
        refused(check_subject_url_form(url));
    }
    // DNS names that resolve to loopback are refused at resolution time.
    let error = refused(resolve_subject_url("https://localhost/token").await);
    assert!(error.contains("non-public"), "{error}");
    test_support::set_metadata_allowed(None);
}

#[test]
fn public_address_classification() {
    for ip in ["8.8.8.8", "142.250.64.10", "2607:f8b0:4004:800::200e"] {
        assert!(is_public(ip.parse::<IpAddr>().unwrap()), "{ip}");
    }
    for ip in [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "255.255.255.255",
        "224.0.0.1",
        "198.18.0.1",
        "192.0.2.1",
        "::1",
        "fe80::1",
        "fd00::1",
        "::ffff:127.0.0.1",
        "::ffff:169.254.169.254",
        "64:ff9b::a00:1",
        "2001:db8::1",
    ] {
        assert!(!is_public(ip.parse::<IpAddr>().unwrap()), "{ip}");
    }
}
