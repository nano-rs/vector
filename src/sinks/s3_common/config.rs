use std::{
    collections::{BTreeMap, HashMap},
    sync::OnceLock,
};

use aws_sdk_s3::{
    Client as S3Client,
    operation::put_object::PutObjectError,
    types::{ObjectCannedAcl, ServerSideEncryption, StorageClass},
};
use aws_smithy_runtime_api::{
    client::{orchestrator::HttpResponse, result::SdkError},
    http::StatusCode,
};
use futures::FutureExt;
use regex::Regex;
use snafu::Snafu;
use vector_lib::configurable::configurable_component;

use super::service::{S3Request, S3Response, S3Service};
use crate::{
    aws::{AwsAuthentication, RegionOrEndpoint, create_client, is_retriable_error},
    common::s3::S3ClientBuilder,
    config::ProxyConfig,
    http::status,
    sinks::{Healthcheck, util::retries::RetryLogic},
    tls::TlsConfig,
};

/// Per-operation configuration when writing objects to S3.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct S3Options {
    /// Canned ACL to apply to the created objects.
    ///
    /// For more information, see [Canned ACL][canned_acl].
    ///
    /// [canned_acl]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
    pub acl: Option<S3CannedAcl>,

    /// Grants `READ`, `READ_ACP`, and `WRITE_ACP` permissions on the created objects to the named [grantee].
    ///
    /// This allows the grantee to read the created objects and their metadata, as well as read and
    /// modify the ACL on the created objects.
    ///
    /// [grantee]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#specifying-grantee
    #[configurable(metadata(
        docs::examples = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
    ))]
    #[configurable(metadata(docs::examples = "person@email.com"))]
    #[configurable(metadata(docs::examples = "http://acs.amazonaws.com/groups/global/AllUsers"))]
    pub grant_full_control: Option<String>,

    /// Grants `READ` permissions on the created objects to the named [grantee].
    ///
    /// This allows the grantee to read the created objects and their metadata.
    ///
    /// [grantee]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#specifying-grantee
    #[configurable(metadata(
        docs::examples = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
    ))]
    #[configurable(metadata(docs::examples = "person@email.com"))]
    #[configurable(metadata(docs::examples = "http://acs.amazonaws.com/groups/global/AllUsers"))]
    pub grant_read: Option<String>,

    /// Grants `READ_ACP` permissions on the created objects to the named [grantee].
    ///
    /// This allows the grantee to read the ACL on the created objects.
    ///
    /// [grantee]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#specifying-grantee
    #[configurable(metadata(
        docs::examples = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
    ))]
    #[configurable(metadata(docs::examples = "person@email.com"))]
    #[configurable(metadata(docs::examples = "http://acs.amazonaws.com/groups/global/AllUsers"))]
    pub grant_read_acp: Option<String>,

    /// Grants `WRITE_ACP` permissions on the created objects to the named [grantee].
    ///
    /// This allows the grantee to modify the ACL on the created objects.
    ///
    /// [grantee]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#specifying-grantee
    #[configurable(metadata(
        docs::examples = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"
    ))]
    #[configurable(metadata(docs::examples = "person@email.com"))]
    #[configurable(metadata(docs::examples = "http://acs.amazonaws.com/groups/global/AllUsers"))]
    pub grant_write_acp: Option<String>,

    /// The Server-side Encryption algorithm used when storing these objects.
    pub server_side_encryption: Option<S3ServerSideEncryption>,

    /// Specifies the ID of the AWS Key Management Service (AWS KMS) symmetrical customer managed
    /// customer master key (CMK) that is used for the created objects.
    ///
    /// Only applies when `server_side_encryption` is configured to use KMS.
    ///
    /// If not specified, Amazon S3 uses the AWS managed CMK in AWS to protect the data.
    #[configurable(metadata(docs::examples = "abcd1234"))]
    #[configurable(metadata(docs::templateable))]
    pub ssekms_key_id: Option<String>,

    /// The storage class for the created objects.
    ///
    /// See the [S3 Storage Classes][s3_storage_classes] for more details.
    ///
    /// [s3_storage_classes]: https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-class-intro.html
    #[serde(default)]
    pub storage_class: S3StorageClass,

    /// The tag-set for the object.
    #[configurable(metadata(docs::additional_props_description = "A single tag."))]
    #[configurable(metadata(docs::examples = "example_tags()"))]
    pub tags: Option<BTreeMap<String, String>>,

    /// Overrides what content encoding has been applied to the object.
    ///
    /// Directly comparable to the `Content-Encoding` HTTP header.
    ///
    /// If not specified, the compression scheme used dictates this value.
    #[configurable(metadata(docs::examples = "gzip"))]
    pub content_encoding: Option<String>,

    /// Overrides the MIME type of the object.
    ///
    /// Directly comparable to the `Content-Type` HTTP header.
    ///
    /// If not specified, the compression scheme used dictates this value.
    /// When `compression` is set to `none`, the value `text/x-log` is used.
    #[configurable(metadata(docs::examples = "application/gzip"))]
    pub content_type: Option<String>,
}

fn example_tags() -> HashMap<String, String> {
    HashMap::<_, _>::from_iter([
        ("Project".to_string(), "Blue".to_string()),
        ("Classification".to_string(), "confidential".to_string()),
        ("PHI".to_string(), "True".to_string()),
    ])
}

/// S3 storage classes.
///
/// More information on each storage class can be found in the [AWS documentation][aws_docs].
///
/// [aws_docs]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum S3StorageClass {
    /// Standard Redundancy.
    #[default]
    Standard,

    /// Reduced Redundancy.
    ReducedRedundancy,

    /// Intelligent Tiering.
    IntelligentTiering,

    /// Infrequently Accessed.
    StandardIa,

    /// High Performance (single Availability zone).
    ExpressOnezone,

    /// Infrequently Accessed (single Availability zone).
    OnezoneIa,

    /// Glacier Flexible Retrieval.
    Glacier,

    /// Glacier Instant Retrieval.
    GlacierIr,

    /// Glacier Deep Archive.
    DeepArchive,
}

impl From<S3StorageClass> for StorageClass {
    fn from(x: S3StorageClass) -> Self {
        match x {
            S3StorageClass::Standard => Self::Standard,
            S3StorageClass::ReducedRedundancy => Self::ReducedRedundancy,
            S3StorageClass::IntelligentTiering => Self::IntelligentTiering,
            S3StorageClass::StandardIa => Self::StandardIa,
            S3StorageClass::ExpressOnezone => Self::ExpressOnezone,
            S3StorageClass::OnezoneIa => Self::OnezoneIa,
            S3StorageClass::Glacier => Self::Glacier,
            S3StorageClass::GlacierIr => Self::GlacierIr,
            S3StorageClass::DeepArchive => Self::DeepArchive,
        }
    }
}

/// AWS S3 Server-Side Encryption algorithms.
///
/// More information on each algorithm can be found in the [AWS documentation][aws_docs].
///
/// [aws_docs]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/serv-side-encryption.html
#[configurable_component]
#[derive(Clone, Copy, Debug)]
pub enum S3ServerSideEncryption {
    /// Each object is encrypted with AES-256 using a unique key.
    ///
    /// This corresponds to the `SSE-S3` option.
    #[serde(rename = "AES256")]
    Aes256,

    /// Each object is encrypted with AES-256 using keys managed by AWS KMS.
    ///
    /// Depending on whether or not a KMS key ID is specified, this corresponds either to the
    /// `SSE-KMS` option (keys generated/managed by KMS) or the `SSE-C` option (keys generated by
    /// the customer, managed by KMS).
    #[serde(rename = "aws:kms")]
    AwsKms,
}

impl From<S3ServerSideEncryption> for ServerSideEncryption {
    fn from(x: S3ServerSideEncryption) -> Self {
        match x {
            S3ServerSideEncryption::Aes256 => Self::Aes256,
            S3ServerSideEncryption::AwsKms => Self::AwsKms,
        }
    }
}

/// S3 Canned ACLs.
///
/// For more information, see [Canned ACL][canned_acl].
///
/// [canned_acl]: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(rename_all = "kebab-case")]
pub enum S3CannedAcl {
    /// Bucket/object are private.
    ///
    /// The bucket/object owner is granted the `FULL_CONTROL` permission, and no one else has
    /// access.
    ///
    /// This is the default.
    #[default]
    Private,

    /// Bucket/object can be read publicly.
    ///
    /// The bucket/object owner is granted the `FULL_CONTROL` permission, and anyone in the
    /// `AllUsers` grantee group is granted the `READ` permission.
    PublicRead,

    /// Bucket/object can be read and written publicly.
    ///
    /// The bucket/object owner is granted the `FULL_CONTROL` permission, and anyone in the
    /// `AllUsers` grantee group is granted the `READ` and `WRITE` permissions.
    ///
    /// This is generally not recommended.
    PublicReadWrite,

    /// Bucket/object are private, and readable by EC2.
    ///
    /// The bucket/object owner is granted the `FULL_CONTROL` permission, and the AWS EC2 service is
    /// granted the `READ` permission for the purpose of reading Amazon Machine Image (AMI) bundles
    /// from the given bucket.
    AwsExecRead,

    /// Bucket/object can be read by authenticated users.
    ///
    /// The bucket/object owner is granted the `FULL_CONTROL` permission, and anyone in the
    /// `AuthenticatedUsers` grantee group is granted the `READ` permission.
    AuthenticatedRead,

    /// Object is private, except to the bucket owner.
    ///
    /// The object owner is granted the `FULL_CONTROL` permission, and the bucket owner is granted the `READ` permission.
    ///
    /// Only relevant when specified for an object: this canned ACL is otherwise ignored when
    /// specified for a bucket.
    BucketOwnerRead,

    /// Object is semi-private.
    ///
    /// Both the object owner and bucket owner are granted the `FULL_CONTROL` permission.
    ///
    /// Only relevant when specified for an object: this canned ACL is otherwise ignored when
    /// specified for a bucket.
    BucketOwnerFullControl,

    /// Bucket can have logs written.
    ///
    /// The `LogDelivery` grantee group is granted `WRITE` and `READ_ACP` permissions.
    ///
    /// Only relevant when specified for a bucket: this canned ACL is otherwise ignored when
    /// specified for an object.
    ///
    /// For more information about logs, see [Amazon S3 Server Access Logging][serverlogs].
    ///
    /// [serverlogs]: https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerLogs.html
    LogDeliveryWrite,
}

impl From<S3CannedAcl> for ObjectCannedAcl {
    fn from(x: S3CannedAcl) -> Self {
        match x {
            S3CannedAcl::Private => ObjectCannedAcl::Private,
            S3CannedAcl::PublicRead => ObjectCannedAcl::PublicRead,
            S3CannedAcl::PublicReadWrite => ObjectCannedAcl::PublicReadWrite,
            S3CannedAcl::AwsExecRead => ObjectCannedAcl::AwsExecRead,
            S3CannedAcl::AuthenticatedRead => ObjectCannedAcl::AuthenticatedRead,
            S3CannedAcl::BucketOwnerRead => ObjectCannedAcl::BucketOwnerRead,
            S3CannedAcl::BucketOwnerFullControl => ObjectCannedAcl::BucketOwnerFullControl,
            S3CannedAcl::LogDeliveryWrite => ObjectCannedAcl::from("log-delivery-write"),
        }
    }
}

fn is_retriable_response(res: &HttpResponse, errors_to_retry: Option<Vec<u16>>) -> bool {
    let status_code = res.status();

    match errors_to_retry {
        Some(error_codes) => error_codes.contains(&status_code.as_u16()),
        None => false,
    }
}

static CLOUDFLARE_EDGE_ERROR: OnceLock<Regex> = OnceLock::new();

/// Returns true when a 409 response was produced by the Cloudflare edge rather
/// than by the S3-compatible origin (R2).
///
/// Cloudflare answers some edge-side failures (for example `1001`, a DNS
/// resolution error inside Cloudflare) with HTTP 409 and a body of the form
/// `error code: 1xxx`, or an HTML error page carrying a `cf-error-code`. The
/// request never reached R2, so no object was written or modified and the PUT
/// is safe to repeat. A genuine S3/R2 409 carries an XML `<Error><Code>` body
/// (`OperationAborted`, `BucketAlreadyExists`, ...) and is not matched here,
/// so its handling is unchanged.
///
/// 5xx edge pages need no special case: every 5xx is already retriable.
/// Other 4xx edge pages (for example a 403 `1020` firewall block) are left
/// non-retriable, because they reflect a standing policy decision and retrying
/// them would wedge the sink rather than recover.
fn is_cloudflare_edge_conflict(res: &HttpResponse) -> bool {
    if res.status().as_u16() != status::CONFLICT {
        return false;
    }
    let Some(body) = res.body().bytes() else {
        return false;
    };
    let body = String::from_utf8_lossy(body);
    let re = CLOUDFLARE_EDGE_ERROR.get_or_init(|| {
        Regex::new(r#"(?i)\A\s*error code:\s*1\d{3}\s*\z|cf-error-code"?>\s*1\d{3}\s*<"#)
            .expect("invalid regex")
    });
    re.is_match(body.as_ref())
}

fn is_cloudflare_edge_error(error: &SdkError<PutObjectError, HttpResponse>) -> bool {
    match error {
        SdkError::ResponseError(err) => is_cloudflare_edge_conflict(err.raw()),
        SdkError::ServiceError(err) => is_cloudflare_edge_conflict(err.raw()),
        _ => false,
    }
}

fn should_retry_error(
    errors_to_retry: Option<Vec<u16>>,
    error: &SdkError<PutObjectError, HttpResponse>,
) -> bool {
    match error {
        SdkError::ResponseError(err) => is_retriable_response(err.raw(), errors_to_retry),
        SdkError::ServiceError(err) => is_retriable_response(err.raw(), errors_to_retry),
        _ => false,
    }
}

/// Retry strategy for S3 service calls.
///
/// Specifies a retry policy for S3 service calls.
///
/// For more information about error responses, see [Client Error Responses][error_responses].
///
/// [error_responses]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status#client_error_responses
#[configurable_component]
#[derive(Debug, Clone, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The retry strategy enum."))]
pub enum RetryStrategy {
    /// Don't retry any errors
    None,

    /// Default strategy. The following error types will be retried:
    /// - `TimeoutError`
    /// - `DispatchFailure`
    /// - `ResponseError` or `ServiceError` when:
    ///   - HTTP status is 5xx
    ///   - Status is 429 (Too Many Requests)
    ///   - `x-amz-retry-after` header is present
    ///   - HTTP status is 4xx and response body contains one of:
    ///     - `"RequestTimeout"`
    ///     - `"RequestExpired"`
    ///     - `"ThrottlingException"`
    ///   - HTTP status is 409 and the body is a Cloudflare edge error page
    ///     (`error code: 1xxx`), which never reached the S3-compatible origin
    /// - Fallback: Any unknown error variant
    #[default]
    Default,

    /// Retry on *all* errors
    All,

    /// Custom retry strategy
    Custom {
        /// Retry on these specific HTTP status codes
        status_codes: Vec<u16>,
    },
}

impl RetryLogic for RetryStrategy {
    type Error = SdkError<PutObjectError, HttpResponse>;
    type Request = S3Request;
    type Response = S3Response;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        match self {
            RetryStrategy::None => false,
            RetryStrategy::Default => is_retriable_error(error) || is_cloudflare_edge_error(error),
            RetryStrategy::All => true,
            RetryStrategy::Custom { status_codes } => {
                is_retriable_error(error)
                    || is_cloudflare_edge_error(error)
                    || should_retry_error(Some(status_codes.clone()), error)
            }
        }
    }
}

#[derive(Debug, Snafu)]
pub enum HealthcheckError {
    #[snafu(display("Invalid credentials"))]
    InvalidCredentials,
    #[snafu(display("Unknown bucket: {:?}", bucket))]
    UnknownBucket { bucket: String },
    #[snafu(display("Unknown status code: {}", status))]
    UnknownStatus { status: StatusCode },
}

pub fn build_healthcheck(bucket: String, client: S3Client) -> crate::Result<Healthcheck> {
    let healthcheck = async move {
        let req = client
            .head_bucket()
            .bucket(bucket.clone())
            .set_expected_bucket_owner(None)
            .send()
            .await;

        match req {
            Ok(_) => Ok(()),
            Err(error) => Err(match error {
                SdkError::ServiceError(inner) => {
                    let status = inner.into_raw().status();
                    match status.as_u16() {
                        status::FORBIDDEN => HealthcheckError::InvalidCredentials.into(),
                        status::NOT_FOUND => HealthcheckError::UnknownBucket { bucket }.into(),
                        _ => HealthcheckError::UnknownStatus { status }.into(),
                    }
                }
                error => error.into(),
            }),
        }
    };

    Ok(healthcheck.boxed())
}

pub async fn create_service(
    region: &RegionOrEndpoint,
    auth: &AwsAuthentication,
    proxy: &ProxyConfig,
    tls_options: Option<&TlsConfig>,
    force_path_style: impl Into<bool>,
) -> crate::Result<S3Service> {
    let endpoint = region.endpoint();
    let region = region.region();
    let force_path_style_value: bool = force_path_style.into();
    let client = create_client::<S3ClientBuilder>(
        &S3ClientBuilder {
            force_path_style: Some(force_path_style_value),
        },
        auth,
        region.clone(),
        endpoint,
        proxy,
        tls_options,
        None,
    )
    .await?;
    Ok(S3Service::new(client))
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::operation::put_object::PutObjectError;
    use aws_smithy_runtime_api::{
        client::{orchestrator::HttpResponse, result::SdkError},
        http::StatusCode,
    };
    use aws_smithy_types::{body::SdkBody, error::ErrorMetadata};
    use vector_lib::request_metadata::GroupedCountByteSize;

    use super::{RetryStrategy, S3StorageClass};
    use crate::{
        serde::json::to_string,
        sinks::{
            s3_common::service::S3Response,
            util::retries::{RetryAction, RetryLogic},
        },
    };

    fn put_error(status: u16, body: &'static str) -> SdkError<PutObjectError, HttpResponse> {
        let raw = HttpResponse::new(StatusCode::try_from(status).unwrap(), SdkBody::from(body));
        SdkError::service_error(
            PutObjectError::generic(ErrorMetadata::builder().build()),
            raw,
        )
    }

    const S3_OPERATION_ABORTED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>OperationAborted</Code><Message>A conflicting conditional operation is currently in progress against this resource.</Message></Error>"#;

    const S3_BUCKET_ALREADY_EXISTS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>BucketAlreadyExists</Code><Message>The requested bucket name is not available.</Message></Error>"#;

    const CF_HTML_1001: &str = r#"<!DOCTYPE html><html><head><title>DNS resolution error | example.r2.cloudflarestorage.com | Cloudflare</title></head>
<body><h1><span class="inline-block">DNS resolution error</span><span class="code-label">Error code <span class="cf-error-code">1001</span></span></h1></body></html>"#;

    // NAN-4619: R2 behind the Cloudflare edge answered a Bronze PUT with
    // `409` + `error code: 1001`; the sink dropped the batch as non-retriable.
    #[test]
    fn cloudflare_edge_409_is_retried() {
        let strategy = RetryStrategy::Default;
        assert!(strategy.is_retriable_error(&put_error(409, "error code: 1001")));
        assert!(strategy.is_retriable_error(&put_error(409, "error code: 1001\n")));
        assert!(strategy.is_retriable_error(&put_error(409, "Error Code: 1014")));
        assert!(strategy.is_retriable_error(&put_error(409, CF_HTML_1001)));

        let custom = RetryStrategy::Custom {
            status_codes: vec![418],
        };
        assert!(custom.is_retriable_error(&put_error(409, "error code: 1001")));
    }

    #[test]
    fn cloudflare_edge_5xx_is_retried() {
        let strategy = RetryStrategy::Default;
        assert!(strategy.is_retriable_error(&put_error(502, "error code: 502")));
        assert!(strategy.is_retriable_error(&put_error(520, "error code: 520")));
        assert!(strategy.is_retriable_error(&put_error(530, "error code: 1016")));
    }

    #[test]
    fn real_s3_409_is_unchanged() {
        let strategy = RetryStrategy::Default;
        assert!(!strategy.is_retriable_error(&put_error(409, S3_OPERATION_ABORTED)));
        assert!(!strategy.is_retriable_error(&put_error(409, S3_BUCKET_ALREADY_EXISTS)));
        assert!(!strategy.is_retriable_error(&put_error(409, "")));
        // A body that merely mentions an edge code inside other text is not an edge page.
        assert!(!strategy.is_retriable_error(&put_error(
            409,
            "<Error><Code>OperationAborted</Code><Message>error code: 1001</Message></Error>"
        )));
    }

    #[test]
    fn cloudflare_edge_non_409_4xx_is_unchanged() {
        // A firewall block is a standing policy; retrying would only wedge the sink.
        let strategy = RetryStrategy::Default;
        assert!(!strategy.is_retriable_error(&put_error(403, "error code: 1020")));
    }

    #[test]
    fn none_strategy_still_never_retries() {
        assert!(!RetryStrategy::None.is_retriable_error(&put_error(409, "error code: 1001")));
    }

    #[test]
    fn ok_response_is_successful() {
        let response = S3Response::for_test(GroupedCountByteSize::new_untagged());
        assert!(matches!(
            RetryStrategy::Default.should_retry_response(&response),
            RetryAction::Successful
        ));
    }

    #[test]
    fn storage_class_names() {
        for &(name, storage_class) in &[
            ("DEEP_ARCHIVE", S3StorageClass::DeepArchive),
            ("GLACIER", S3StorageClass::Glacier),
            ("GLACIER_IR", S3StorageClass::GlacierIr),
            ("INTELLIGENT_TIERING", S3StorageClass::IntelligentTiering),
            ("EXPRESS_ONEZONE", S3StorageClass::ExpressOnezone),
            ("ONEZONE_IA", S3StorageClass::OnezoneIa),
            ("REDUCED_REDUNDANCY", S3StorageClass::ReducedRedundancy),
            ("STANDARD", S3StorageClass::Standard),
            ("STANDARD_IA", S3StorageClass::StandardIa),
        ] {
            assert_eq!(name, to_string(storage_class));
            let result: S3StorageClass = serde_json::from_str(&format!("{name:?}"))
                .unwrap_or_else(|error| panic!("Unparsable storage class name {name:?}: {error}"));
            assert_eq!(result, storage_class);
        }
    }
}
