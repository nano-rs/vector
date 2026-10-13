//! Common module between modules that use HTTP
#[cfg(all(
    feature = "sources-utils-http-auth",
    feature = "sources-utils-http-error"
))]
pub mod server_auth;

/// nano fork (NAN-3770): SHA-256-hashed ingest tokens reloaded from a file.
#[cfg(any(
    feature = "sources-utils-http-prelude",
    feature = "sources-splunk_hec"
))]
pub mod hashed_tokens;

#[cfg(feature = "sources-utils-http-error")]
mod error;

#[cfg(feature = "sources-utils-http-error")]
pub use error::ErrorMessage;
