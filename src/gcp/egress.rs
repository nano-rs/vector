//! Egress policy for GCP credential files (NAN-4935 review P1).
//!
//! A credentials file is tenant-supplied data. Without a policy, it could point Vector at
//! arbitrary endpoints and local files: a `token_url` on an attacker host would receive
//! whatever subject token Vector read, an impersonation URL would receive a live Google
//! access token, and a `credential_source.file` of `/proc/self/environ` or a Kubernetes
//! service-account token would be read and forwarded. So every endpoint and source a
//! credentials file names goes through this module:
//!
//! * **Token endpoints** (`token_uri`, `token_url`, `service_account_impersonation_url`):
//!   HTTPS, the default port, no userinfo, a DNS host that is `googleapis.com` or a
//!   subdomain of it (regional and Private Service Connect forms are all
//!   `*.googleapis.com`), and an exact path per endpoint kind.
//! * **`credential_source.file`**: a regular file under one operator-configured root
//!   (`VECTOR_GCP_WIF_FILE_ROOT`, default [`DEFAULT_FILE_ROOT`]). The path and the root are
//!   both canonicalized, so symlinks are resolved and re-checked, and reads are capped at
//!   [`MAX_SUBJECT_TOKEN_BYTES`]. `/proc`, `/dev` and the Kubernetes service-account token
//!   are reachable only when the operator points the root at them.
//! * **`credential_source.url`**: HTTPS to a DNS host whose every resolved address is
//!   public. The connection is pinned to those addresses, does not use a proxy and does not
//!   follow redirects. The GCE/GKE metadata server (plain HTTP, link-local) is allowed only
//!   when the operator sets `VECTOR_GCP_WIF_ALLOW_METADATA=true`.
//!
//! The settings are environment variables of the Vector process on purpose: whoever can
//! write a credentials file cannot also widen the policy.

use std::{
    io::Read as _,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Component, Path, PathBuf},
};

use url::{Host, Url};

use super::GcpError;

/// Default root for `external_account` file credential sources.
pub(super) const DEFAULT_FILE_ROOT: &str = "/var/run/secrets/nano-gcp-wif";
pub(super) const FILE_ROOT_ENV: &str = "VECTOR_GCP_WIF_FILE_ROOT";
pub(super) const ALLOW_METADATA_ENV: &str = "VECTOR_GCP_WIF_ALLOW_METADATA";
/// Upper bound on a subject token read from a file or URL.
pub(super) const MAX_SUBJECT_TOKEN_BYTES: u64 = 64 * 1024;

const METADATA_HOSTS: &[&str] = &["metadata.google.internal", "169.254.169.254"];
const METADATA_ADDR: Ipv4Addr = Ipv4Addr::new(169, 254, 169, 254);

/// The token endpoints a credentials file may name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum GoogleEndpoint {
    /// OAuth 2.0 token endpoint (`authorized_user.token_uri`, `service_account.token_uri`).
    OAuthToken,
    /// Security Token Service (`external_account.token_url`).
    StsToken,
    /// IAM Credentials `generateAccessToken` (`service_account_impersonation_url`).
    Impersonation,
}

impl GoogleEndpoint {
    const fn field(self) -> &'static str {
        match self {
            Self::OAuthToken => "token_uri",
            Self::StsToken => "token_url",
            Self::Impersonation => "service_account_impersonation_url",
        }
    }

    fn path_allowed(self, path: &str) -> bool {
        match self {
            Self::OAuthToken => path == "/token",
            Self::StsToken => path == "/v1/token",
            Self::Impersonation => path
                .strip_prefix("/v1/projects/-/serviceAccounts/")
                .and_then(|rest| rest.strip_suffix(":generateAccessToken"))
                .is_some_and(|account| {
                    !account.is_empty()
                        && account
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || "@.-_".contains(c))
                }),
        }
    }
}

fn refused(what: &'static str, reason: impl Into<String>) -> GcpError {
    GcpError::EgressRefused {
        what,
        reason: reason.into(),
    }
}

/// Check a token endpoint named by a credentials file.
pub(super) fn check_google_endpoint(raw: &str, kind: GoogleEndpoint) -> Result<(), GcpError> {
    let what = kind.field();
    let url = Url::parse(raw).map_err(|_| refused(what, "not a valid URL"))?;
    #[cfg(test)]
    if test_support::is_mock_endpoint(&url) {
        return kind
            .path_allowed(url.path())
            .then_some(())
            .ok_or_else(|| refused(what, "path is not the Google endpoint path"));
    }
    if url.scheme() != "https" {
        return Err(refused(what, "must use https"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(refused(what, "must not carry userinfo"));
    }
    if url.port().is_some_and(|port| port != 443) {
        return Err(refused(what, "must use the default https port"));
    }
    match url.host() {
        Some(Host::Domain(host)) if is_googleapis_host(host) => {}
        Some(Host::Domain(_)) => {
            return Err(refused(
                what,
                "host is not googleapis.com or a subdomain of it",
            ));
        }
        _ => {
            return Err(refused(
                what,
                "must name a googleapis.com host, not an IP address",
            ));
        }
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(refused(what, "must not carry a query or fragment"));
    }
    if !kind.path_allowed(url.path()) {
        return Err(refused(what, "path is not the Google endpoint path"));
    }
    Ok(())
}

fn is_googleapis_host(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "googleapis.com"
        || host.strip_suffix(".googleapis.com").is_some_and(|label| {
            !label.is_empty()
                && label.split('.').all(|l| {
                    !l.is_empty() && l.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                })
        })
}

/// The configured file root, canonicalized.
pub(super) fn file_root() -> Result<PathBuf, GcpError> {
    #[cfg(test)]
    if let Some(root) = test_support::file_root() {
        return root
            .canonicalize()
            .map_err(|_| refused("credential_source.file", "the file root does not exist"));
    }
    let root = std::env::var_os(FILE_ROOT_ENV)
        .filter(|v| !v.is_empty())
        .map_or_else(|| PathBuf::from(DEFAULT_FILE_ROOT), PathBuf::from);
    root.canonicalize().map_err(|_| {
        refused(
            "credential_source.file",
            format!(
                "the file root {} does not exist (set {FILE_ROOT_ENV} to the directory holding \
                 workload identity tokens)",
                root.display()
            ),
        )
    })
}

/// Check a `credential_source.file` path lexically (when the credentials file is parsed; the
/// token file itself may not exist yet). [`read_subject_file`] repeats the full check.
pub(super) fn check_subject_file_lexically(raw: &str) -> Result<(), GcpError> {
    let path = Path::new(raw);
    if !path.is_absolute() {
        return Err(refused(
            "credential_source.file",
            "must be an absolute path",
        ));
    }
    if path
        .components()
        .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(refused(
            "credential_source.file",
            "must not contain . or .. segments",
        ));
    }
    Ok(())
}

/// Read a subject token file under the configured root: canonicalized (symlinks resolved and
/// re-checked), regular files only, at most [`MAX_SUBJECT_TOKEN_BYTES`].
#[cfg(test)]
pub(super) fn read_subject_file(raw: &str) -> Result<String, GcpError> {
    read_subject_file_in(raw, &file_root()?)
}

/// [`read_subject_file`] against an already-resolved root (for blocking-pool callers, which
/// resolve the root on the calling thread).
pub(super) fn read_subject_file_in(raw: &str, root: &Path) -> Result<String, GcpError> {
    const WHAT: &str = "credential_source.file";
    check_subject_file_lexically(raw)?;
    let path = Path::new(raw)
        .canonicalize()
        .map_err(|e| refused(WHAT, format!("cannot resolve the path: {}", e.kind())))?;
    if !path.starts_with(root) {
        return Err(refused(
            WHAT,
            format!("must be under {} ({FILE_ROOT_ENV})", root.display()),
        ));
    }
    let file = std::fs::File::open(&path)
        .map_err(|e| refused(WHAT, format!("cannot open: {}", e.kind())))?;
    let metadata = file
        .metadata()
        .map_err(|e| refused(WHAT, format!("cannot stat: {}", e.kind())))?;
    if !metadata.is_file() {
        return Err(refused(WHAT, "must be a regular file"));
    }
    let mut buf = Vec::new();
    file.take(MAX_SUBJECT_TOKEN_BYTES + 1)
        .read_to_end(&mut buf)
        .map_err(|e| refused(WHAT, format!("cannot read: {}", e.kind())))?;
    if buf.len() as u64 > MAX_SUBJECT_TOKEN_BYTES {
        return Err(refused(WHAT, "is larger than 64 KiB"));
    }
    String::from_utf8(buf).map_err(|_| refused(WHAT, "is not UTF-8"))
}

fn metadata_allowed() -> bool {
    #[cfg(test)]
    if let Some(allowed) = test_support::metadata_allowed() {
        return allowed;
    }
    std::env::var(ALLOW_METADATA_ENV).is_ok_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// Check a `credential_source.url` without resolving it (at parse time).
pub(super) fn check_subject_url_form(raw: &str) -> Result<Url, GcpError> {
    const WHAT: &str = "credential_source.url";
    let url = Url::parse(raw).map_err(|_| refused(WHAT, "not a valid URL"))?;
    #[cfg(test)]
    if test_support::is_mock_endpoint(&url) {
        return Ok(url);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(refused(WHAT, "must not carry userinfo"));
    }
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    if METADATA_HOSTS.contains(&host.as_str()) {
        if !metadata_allowed() {
            return Err(refused(
                WHAT,
                format!(
                    "the metadata server is allowed only when the operator sets {ALLOW_METADATA_ENV}=true"
                ),
            ));
        }
        if url.scheme() != "http" || url.port().is_some_and(|p| p != 80) {
            return Err(refused(
                WHAT,
                "the metadata server is reached over http on port 80",
            ));
        }
        return Ok(url);
    }
    if url.scheme() != "https" {
        return Err(refused(WHAT, "must use https"));
    }
    match url.host() {
        Some(Host::Domain(_)) => Ok(url),
        _ => Err(refused(WHAT, "must name a DNS host, not an IP address")),
    }
}

/// Resolve a `credential_source.url` and return the addresses to pin the connection to.
/// Every resolved address must be public, except the metadata server under its opt-in.
pub(super) async fn resolve_subject_url(raw: &str) -> Result<(Url, Vec<SocketAddr>), GcpError> {
    const WHAT: &str = "credential_source.url";
    let url = check_subject_url_form(raw)?;
    #[cfg(test)]
    if test_support::is_mock_endpoint(&url) {
        return Ok((url, Vec::new()));
    }
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    let port = url.port_or_known_default().unwrap_or(443);
    if METADATA_HOSTS.contains(&host.as_str()) {
        // Pinned to the one metadata address whatever the name resolves to.
        return Ok((url, vec![SocketAddr::new(IpAddr::V4(METADATA_ADDR), port)]));
    }
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|e| refused(WHAT, format!("cannot resolve the host: {}", e.kind())))?
        .collect();
    if addrs.is_empty() {
        return Err(refused(WHAT, "the host resolved to no addresses"));
    }
    if let Some(bad) = addrs.iter().find(|a| !is_public(a.ip())) {
        return Err(refused(
            WHAT,
            format!("the host resolves to a non-public address ({})", bad.ip()),
        ));
    }
    Ok((url, addrs))
}

/// Whether an address is globally routable: not loopback, private, CGNAT, link-local
/// (including the metadata server), ULA, multicast, documentation, benchmarking,
/// unspecified or broadcast. IPv4-mapped IPv6 addresses are judged as IPv4.
pub(super) fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_public_v4(v4),
            None => is_public_v6(v6),
        },
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || a == 0
        || (a == 100 && (64..128).contains(&b)) // CGNAT
        || (a == 192 && b == 0 && c == 0) // IETF protocol assignments
        || (a == 198 && (18..20).contains(&b)) // benchmarking
        || a >= 240)
}

const fn is_public_v6(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (first & 0xfe00) == 0xfc00 // unique local
        || (first & 0xffc0) == 0xfe80 // link local
        || (first == 0x2001 && ip.segments()[1] == 0x0db8) // documentation
        || (first == 0x0064 && ip.segments()[1] == 0xff9b)) // NAT64 (could reach v4 private)
}

/// Fetch a URL subject token over a connection pinned to `addrs`, with no proxy, no
/// redirects and a capped body.
pub(super) async fn fetch_pinned(
    url: &Url,
    addrs: &[SocketAddr],
    headers: &[(String, String)],
) -> Result<String, GcpError> {
    const WHAT: &str = "credential_source.url";
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30));
    if let Some(host) = url.host_str()
        && !addrs.is_empty()
    {
        builder = builder.resolve_to_addrs(host, addrs);
    }
    let client = builder
        .build()
        .map_err(|e| refused(WHAT, format!("cannot build the client: {e}")))?;
    let mut request = client.get(url.clone());
    for (name, value) in headers {
        request = request.header(name.as_str(), value.as_str());
    }
    let mut response = request
        .send()
        .await
        .map_err(|e| refused(WHAT, format!("request failed: {}", e.without_url())))?;
    if !response.status().is_success() {
        return Err(GcpError::SubjectToken {
            reason: format!("credential_source.url returned HTTP {}", response.status()),
        });
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| refused(WHAT, format!("cannot read the body: {}", e.without_url())))?
    {
        body.extend_from_slice(&chunk);
        if body.len() as u64 > MAX_SUBJECT_TOKEN_BYTES {
            return Err(refused(WHAT, "response is larger than 64 KiB"));
        }
    }
    String::from_utf8(body).map_err(|_| GcpError::SubjectToken {
        reason: "credential_source.url returned a non-UTF-8 body".into(),
    })
}

/// Test-only overrides, per thread so parallel tests do not interfere.
#[cfg(test)]
pub(super) mod test_support {
    use std::{cell::RefCell, path::PathBuf};

    use url::Url;

    thread_local! {
        static FILE_ROOT: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
        static ALLOW_METADATA: RefCell<Option<bool>> = const { RefCell::new(None) };
        static MOCK_PORTS: RefCell<Vec<u16>> = const { RefCell::new(Vec::new()) };
    }

    pub(in crate::gcp) fn set_file_root(root: Option<PathBuf>) {
        FILE_ROOT.with(|r| *r.borrow_mut() = root);
    }

    pub(in crate::gcp) fn file_root() -> Option<PathBuf> {
        FILE_ROOT.with(|r| r.borrow().clone())
    }

    pub(in crate::gcp) fn set_metadata_allowed(allowed: Option<bool>) {
        ALLOW_METADATA.with(|r| *r.borrow_mut() = allowed);
    }

    pub(in crate::gcp) fn metadata_allowed() -> Option<bool> {
        ALLOW_METADATA.with(|r| *r.borrow())
    }

    /// Exempt one local mock server (`http://127.0.0.1:<port>`) from the host and scheme
    /// rules. Paths are still checked. Any other loopback URL stays refused.
    pub(in crate::gcp) fn allow_mock_server(uri: &str) {
        let port = Url::parse(uri).unwrap().port().unwrap();
        MOCK_PORTS.with(|p| p.borrow_mut().push(port));
    }

    pub(in crate::gcp) fn is_mock_endpoint(url: &Url) -> bool {
        url.scheme() == "http"
            && url.host_str() == Some("127.0.0.1")
            && url
                .port()
                .is_some_and(|port| MOCK_PORTS.with(|p| p.borrow().contains(&port)))
    }
}

#[cfg(test)]
#[path = "egress_tests.rs"]
mod tests;
