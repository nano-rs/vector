//! FDR batch manifests: parsing, checksums and object classification.

use chrono::{DateTime, TimeZone, Utc};
use md5::Digest as _;
use serde::Deserialize;
use snafu::Snafu;
use vector_lib::configurable::configurable_component;

/// One FDR batch manifest, as delivered on the SQS queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Manifest {
    pub(super) cid: String,
    /// Batch time, in epoch milliseconds.
    pub(super) timestamp_ms: i64,
    pub(super) bucket: String,
    pub(super) path_prefix: Option<String>,
    pub(super) total_size: u64,
    pub(super) files: Vec<ManifestFile>,
}

/// One object listed in a manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ManifestFile {
    pub(super) path: String,
    pub(super) size: Option<u64>,
    pub(super) checksum: Option<String>,
    pub(super) kind: ObjectKind,
}

/// Whether an object is event data or a lookup snapshot (`fdrv2-*`, such as aidmaster).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObjectKind {
    Data,
    Lookup,
}

impl ObjectKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::Lookup => "lookup",
        }
    }
}

impl Manifest {
    /// Batch time as a UTC timestamp; `None` only for values outside chrono's range.
    pub(super) fn batch_time(&self) -> Option<DateTime<Utc>> {
        Utc.timestamp_millis_opt(self.timestamp_ms).single()
    }
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub(super) enum ManifestError {
    #[snafu(display("manifest is not valid JSON: {message}"))]
    Json { message: String },
    #[snafu(display("manifest field `{field}` is missing or empty"))]
    Missing { field: &'static str },
    #[snafu(display("manifest field `{field}` is not a non-negative integer: {value}"))]
    BadNumber { field: &'static str, value: String },
}

/// A JSON number that may also arrive as a string.
#[derive(Deserialize)]
#[serde(untagged)]
enum NumOrString {
    Num(serde_json::Number),
    Str(String),
}

impl NumOrString {
    fn as_i64(&self, field: &'static str) -> Result<i64, ManifestError> {
        let text = match self {
            Self::Num(number) => number.to_string(),
            Self::Str(text) => text.trim().to_owned(),
        };
        text.parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .ok_or(ManifestError::BadNumber { field, value: text })
    }

    fn as_u64(&self, field: &'static str) -> Result<u64, ManifestError> {
        #[allow(clippy::cast_sign_loss, reason = "as_i64 rejects negatives")]
        self.as_i64(field).map(|value| value as u64)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawManifest {
    cid: Option<String>,
    timestamp: Option<NumOrString>,
    total_size: Option<NumOrString>,
    bucket: Option<String>,
    path_prefix: Option<String>,
    #[serde(default)]
    files: Vec<RawFile>,
}

#[derive(Deserialize)]
struct RawFile {
    path: Option<String>,
    size: Option<NumOrString>,
    checksum: Option<String>,
}

/// Parses an SQS message body as an FDR manifest.
pub(super) fn parse_manifest(body: &str) -> Result<Manifest, ManifestError> {
    let raw: RawManifest = serde_json::from_str(body).map_err(|error| ManifestError::Json {
        message: error.to_string(),
    })?;
    let bucket = raw
        .bucket
        .filter(|bucket| !bucket.is_empty())
        .ok_or(ManifestError::Missing { field: "bucket" })?;
    let timestamp_ms = raw
        .timestamp
        .ok_or(ManifestError::Missing { field: "timestamp" })?
        .as_i64("timestamp")?;
    let path_prefix = raw.path_prefix.filter(|prefix| !prefix.is_empty());
    let prefix_kind = path_prefix.as_deref().map(classify);

    let mut files = Vec::with_capacity(raw.files.len());
    for file in raw.files {
        let path = file
            .path
            .filter(|path| !path.is_empty())
            .ok_or(ManifestError::Missing {
                field: "files[].path",
            })?;
        let size = file
            .size
            .map(|size| size.as_u64("files[].size"))
            .transpose()?;
        let kind = match (classify(&path), prefix_kind) {
            (ObjectKind::Lookup, _) | (_, Some(ObjectKind::Lookup)) => ObjectKind::Lookup,
            _ => ObjectKind::Data,
        };
        files.push(ManifestFile {
            path,
            size,
            checksum: file.checksum.filter(|checksum| !checksum.is_empty()),
            kind,
        });
    }
    let listed: u64 = files.iter().filter_map(|file| file.size).sum();
    let total_size = match raw.total_size {
        Some(total) => total.as_u64("totalSize")?.max(listed),
        None => listed,
    };

    Ok(Manifest {
        cid: raw.cid.unwrap_or_default(),
        timestamp_ms,
        bucket,
        path_prefix,
        total_size,
        files,
    })
}

/// Lookup snapshots have a path segment starting with `fdrv2` (`fdrv2-aidmaster-*`,
/// `fdrv2/managedassets/...`); everything else is event data.
pub(super) fn classify(path: &str) -> ObjectKind {
    if path.split('/').any(|segment| segment.starts_with("fdrv2")) {
        ObjectKind::Lookup
    } else {
        ObjectKind::Data
    }
}

/// How to treat manifest checksums.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChecksumMode {
    /// Verify every file. A missing checksum, one of unknown shape, a mismatch or a size
    /// mismatch fails the file, which releases the manifest for re-delivery.
    #[default]
    Verify,

    /// Verify every file, but only log a warning (and count an error) on failure.
    Warn,

    /// Do not hash files.
    Off,
}

/// The digest a manifest checksum names, by its shape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Expected {
    Md5([u8; 16]),
    Sha256([u8; 32]),
}

/// Reads a checksum: 32 hex digits are MD5, 64 are SHA-256. Anything else is `Err`.
pub(super) fn parse_checksum(checksum: Option<&str>) -> Result<Expected, String> {
    let Some(checksum) = checksum.map(str::trim) else {
        return Err("manifest lists no checksum".to_owned());
    };
    match checksum.len() {
        32 => {
            let mut out = [0u8; 16];
            hex::decode_to_slice(checksum, &mut out)
                .map(|()| Expected::Md5(out))
                .map_err(|_| format!("checksum {checksum:?} is not hex"))
        }
        64 => {
            let mut out = [0u8; 32];
            hex::decode_to_slice(checksum, &mut out)
                .map(|()| Expected::Sha256(out))
                .map_err(|_| format!("checksum {checksum:?} is not hex"))
        }
        len => Err(format!(
            "checksum of unknown shape ({len} characters; expected 32 for MD5 or 64 for SHA-256)"
        )),
    }
}

/// Incremental hasher for one file.
pub(super) enum Hasher {
    Md5(md5::Md5),
    Sha256(sha2::Sha256),
}

impl Hasher {
    pub(super) fn for_expected(expected: &Expected) -> Self {
        match expected {
            Expected::Md5(_) => Self::Md5(md5::Md5::new()),
            Expected::Sha256(_) => Self::Sha256(sha2::Sha256::new()),
        }
    }

    pub(super) fn update(&mut self, chunk: &[u8]) {
        match self {
            Self::Md5(hasher) => hasher.update(chunk),
            Self::Sha256(hasher) => sha2::Digest::update(hasher, chunk),
        }
    }

    /// Compares against the expected digest; `Err` describes a mismatch.
    pub(super) fn verify(self, expected: &Expected) -> Result<(), String> {
        let (actual, wanted) = match (self, expected) {
            (Self::Md5(hasher), Expected::Md5(wanted)) => {
                (hasher.finalize().to_vec(), wanted.to_vec())
            }
            (Self::Sha256(hasher), Expected::Sha256(wanted)) => {
                (sha2::Digest::finalize(hasher).to_vec(), wanted.to_vec())
            }
            _ => return Err("hasher does not match the expected digest kind".to_owned()),
        };
        if actual == wanted {
            Ok(())
        } else {
            Err(format!(
                "checksum mismatch: manifest {}, object {}",
                hex_string(&wanted),
                hex_string(&actual)
            ))
        }
    }
}

fn hex_string(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
        out
    })
}

/// Hashes a whole buffer; used by tests and fixtures.
#[cfg(test)]
pub(super) fn md5_hex(data: &[u8]) -> String {
    hex_string(&md5::Md5::digest(data))
}

#[cfg(test)]
pub(super) fn sha256_hex(data: &[u8]) -> String {
    hex_string(&sha2::Sha256::digest(data))
}
