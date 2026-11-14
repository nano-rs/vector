//! Service implementation for the `Clickhouse` sink.

use std::fmt::Write as _;

use bytes::Bytes;
use http::Uri;
use http_1::{
    Request, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE},
};
use snafu::ResultExt;
use tracing::debug;
use vector_http::HttpError;

use super::{config::QuerySettingsConfig, sink::PartitionKey};
use crate::{
    http::Auth,
    sinks::{
        UriParseSnafu,
        clickhouse::config::Format,
        prelude::*,
        util::{
            http::{HttpRequest, RetryStrategy},
            http_v1::{HttpResponse, HttpServiceRequestBuilder},
            retries::RetryAction,
        },
    },
};

#[derive(Debug, Default, Clone)]
pub(crate) struct ClickhouseRetryLogic;

impl RetryLogic for ClickhouseRetryLogic {
    type Error = HttpError;
    type Request = HttpRequest<PartitionKey>;
    type Response = HttpResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        error.is_retriable()
    }

    fn should_retry_response(&self, response: &Self::Response) -> RetryAction<Self::Request> {
        let status = response.response.status();

        if !status.is_success() {
            debug!(
                message = "HTTP response.",
                %status,
                body = %String::from_utf8_lossy(response.response.body()),
            );
        }

        // nano (NAN-5390): an authentication or access refusal is an operator error
        // (wrong password, user dropped, grant regression, user `networks` that no
        // longer admit this host), never a property of the batch. Upstream's default
        // strategy treats the 401/403 ClickHouse answers with as permanent and drops
        // the batch, after the source has already acknowledged it. Retry it instead,
        // so the batch stays in the sink's buffer and `when_full = "block"` applies
        // back-pressure until access is restored.
        if let Some(reason) = access_refusal(status, response.response.body()) {
            return RetryAction::Retry(reason.into());
        }

        match status {
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body = response.response.body();

                // Currently, ClickHouse returns 500's incorrect data and type mismatch errors.
                // This attempts to check if the body starts with `Code: {code_num}` and to not
                // retry those errors.
                //
                // Reference: https://github.com/vectordotdev/vector/pull/693#issuecomment-517332654
                // Error code definitions: https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Common/ErrorCodes.cpp
                //
                // Fix already merged: https://github.com/ClickHouse/ClickHouse/pull/6271
                if body.starts_with(b"Code: 117") {
                    RetryAction::DontRetry("incorrect data".into())
                } else if body.starts_with(b"Code: 53") {
                    RetryAction::DontRetry("type mismatch".into())
                } else {
                    RetryAction::Retry(String::from_utf8_lossy(body).to_string().into())
                }
            }
            _ => RetryStrategy::Default.retry_action(status),
        }
    }
}

/// ClickHouse error codes that mean "this user may not do this right now", whatever HTTP
/// status the server mapped them to: `UNKNOWN_USER` (192), `WRONG_PASSWORD` (193),
/// `REQUIRED_PASSWORD` (194), `IP_ADDRESS_NOT_ALLOWED` (195), `ACCESS_DENIED` (497) and
/// `AUTHENTICATION_FAILED` (516).
const ACCESS_REFUSAL_CODES: [u32; 6] = [192, 193, 194, 195, 497, 516];

/// Parses the leading `Code: <n>.` of a ClickHouse error body.
fn clickhouse_error_code(body: &[u8]) -> Option<u32> {
    let rest = body.strip_prefix(b"Code: ")?;
    let digits = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    std::str::from_utf8(&rest[..digits]).ok()?.parse().ok()
}

/// Returns a retry reason when the response is an authentication or access refusal (nano,
/// NAN-5390): HTTP 401 or 403, or any error whose body carries one of
/// [`ACCESS_REFUSAL_CODES`].
fn access_refusal(status: StatusCode, body: &[u8]) -> Option<String> {
    if status.is_success() {
        return None;
    }
    let code = clickhouse_error_code(body);
    let refused = matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
        || code.is_some_and(|code| ACCESS_REFUSAL_CODES.contains(&code));
    if !refused {
        return None;
    }
    let detail = String::from_utf8_lossy(&body[..body.len().min(512)]);
    Some(format!(
        "ClickHouse refused access ({status}); the batch is held and retried until access is restored: {}",
        detail.trim_end()
    ))
}

#[derive(Debug, Clone)]
pub(super) struct ClickhouseServiceRequestBuilder {
    pub(super) auth: Option<Auth>,
    pub(super) endpoint: Uri,
    pub(super) skip_unknown_fields: Option<bool>,
    pub(super) date_time_best_effort: bool,
    pub(super) insert_random_shard: bool,
    pub(super) compression: Compression,
    pub(super) query_settings: QuerySettingsConfig,
}

impl HttpServiceRequestBuilder<PartitionKey> for ClickhouseServiceRequestBuilder {
    fn build(
        &self,
        mut request: HttpRequest<PartitionKey>,
    ) -> Result<Request<Bytes>, crate::Error> {
        let metadata = request.get_additional_metadata();

        let uri = set_uri_query(
            &self.endpoint,
            &metadata.database,
            &metadata.table,
            metadata.format,
            self.skip_unknown_fields,
            self.date_time_best_effort,
            self.insert_random_shard,
            self.query_settings,
        )?;

        // Extract format before taking payload to avoid borrow checker issues
        let format = metadata.format;
        let payload = request.take_payload();

        // Set content type based on format
        let content_type = match format {
            Format::ArrowStream => "application/vnd.apache.arrow.stream",
            _ => "application/x-ndjson",
        };

        let mut builder = Request::post(uri.to_string())
            .header(CONTENT_TYPE, content_type)
            .header(CONTENT_LENGTH, payload.len());
        if let Some(ce) = self.compression.content_encoding() {
            builder = builder.header(CONTENT_ENCODING, ce);
        }
        let mut request = builder.body(payload).map_err(crate::Error::from)?;
        if let Some(auth) = &self.auth {
            auth.apply_v1(&mut request);
        }

        Ok(request)
    }
}

fn append_param<T: ToString>(uri: &mut String, key: &str, value: Option<T>) {
    if let Some(val) = value {
        write!(uri, "{key}={}&", val.to_string()).expect("writing to a String cannot fail");
    }
}
fn append_param_bool(uri: &mut String, key: &str, value: Option<bool>) {
    if let Some(val) = value {
        write!(uri, "{key}={}&", u8::from(val)).expect("writing to a String cannot fail");
    }
}

#[allow(clippy::too_many_arguments)]
fn set_uri_query(
    uri: &Uri,
    database: &str,
    table: &str,
    format: Format,
    skip_unknown: Option<bool>,
    date_time_best_effort: bool,
    insert_random_shard: bool,
    query_settings: QuerySettingsConfig,
) -> crate::Result<Uri> {
    // Use ClickHouse query parameters with the Identifier type (introduced in 21.12) so
    // the server handles identifier quoting — no client-side escaping required.
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair(
            "query",
            &format!("INSERT INTO {{database:Identifier}}.{{table:Identifier}} FORMAT {format}"),
        )
        .append_pair("param_database", database)
        .append_pair("param_table", table)
        .finish();

    let mut uri = uri.to_string();
    if !uri.ends_with('/') {
        uri.push('/');
    }

    uri.push_str("?input_format_import_nested_json=1&");
    append_param_bool(&mut uri, "input_format_skip_unknown_fields", skip_unknown);
    if date_time_best_effort {
        uri.push_str("date_time_input_format=best_effort&");
    }
    if insert_random_shard {
        uri.push_str("insert_distributed_one_random_shard=1&");
    }
    append_param_bool(
        &mut uri,
        "async_insert",
        query_settings.async_insert_settings.enabled,
    );
    append_param_bool(
        &mut uri,
        "wait_for_async_insert",
        query_settings.async_insert_settings.wait_for_processing,
    );
    append_param(
        &mut uri,
        "wait_for_async_insert_timeout",
        query_settings
            .async_insert_settings
            .wait_for_processing_timeout,
    );
    append_param_bool(
        &mut uri,
        "async_insert_deduplicate",
        query_settings.async_insert_settings.deduplicate,
    );
    append_param(
        &mut uri,
        "async_insert_max_data_size",
        query_settings.async_insert_settings.max_data_size,
    );
    append_param(
        &mut uri,
        "async_insert_max_query_number",
        query_settings.async_insert_settings.max_query_number,
    );
    uri.push_str(query.as_str());

    uri.parse::<Uri>()
        .context(UriParseSnafu)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::super::config::AsyncInsertSettingsConfig;
    use super::*;

    fn parse_query_params(uri: &Uri) -> std::collections::HashMap<String, String> {
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    }

    fn response(status: StatusCode, body: &'static str) -> HttpResponse {
        HttpResponse {
            events_byte_size: GroupedCountByteSize::new_untagged(),
            raw_byte_size: 0,
            response: http_1::Response::builder()
                .status(status)
                .body(Bytes::from_static(body.as_bytes()))
                .unwrap(),
        }
    }

    fn retries(status: StatusCode, body: &'static str) -> bool {
        matches!(
            ClickhouseRetryLogic.should_retry_response(&response(status, body)),
            RetryAction::Retry(_)
        )
    }

    // NAN-5390: auth and access refusals are held and retried, never dropped.
    #[test]
    fn access_refusals_are_retried() {
        assert!(retries(
            StatusCode::FORBIDDEN,
            "Code: 516. DB::Exception: nanosiem: Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)"
        ));
        assert!(retries(StatusCode::UNAUTHORIZED, ""));
        assert!(retries(
            StatusCode::FORBIDDEN,
            "Code: 497. DB::Exception: nanosiem: Not enough privileges. (ACCESS_DENIED)"
        ));
        for code in ACCESS_REFUSAL_CODES {
            let body: &'static str = format!("Code: {code}. DB::Exception: refused").leak();
            assert!(
                retries(StatusCode::INTERNAL_SERVER_ERROR, body),
                "{code} on 500"
            );
            assert!(retries(StatusCode::BAD_REQUEST, body), "{code} on 400");
        }
    }

    #[test]
    fn data_errors_still_not_retried() {
        assert!(!retries(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Code: 117. DB::Exception: incorrect data"
        ));
        assert!(!retries(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Code: 53. DB::Exception: type mismatch"
        ));
        // Code 19 must not be read as a prefix of 192..195.
        assert!(!retries(
            StatusCode::BAD_REQUEST,
            "Code: 19. DB::Exception: x"
        ));
        assert!(!retries(
            StatusCode::BAD_REQUEST,
            "Code: 27. DB::Exception: parse"
        ));
        assert!(retries(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Code: 241. memory"
        ));
        assert!(matches!(
            ClickhouseRetryLogic.should_retry_response(&response(StatusCode::OK, "")),
            RetryAction::Successful
        ));
    }

    #[test]
    fn parses_clickhouse_error_code() {
        assert_eq!(
            clickhouse_error_code(b"Code: 516. DB::Exception"),
            Some(516)
        );
        assert_eq!(clickhouse_error_code(b"Code: 19. x"), Some(19));
        assert_eq!(clickhouse_error_code(b"code: 516."), None);
        assert_eq!(clickhouse_error_code(b"Code: ."), None);
        assert_eq!(clickhouse_error_code(b""), None);
    }

    #[test]
    fn encode_valid() {
        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            Format::JsonEachRow,
            Some(false),
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=0&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%7Bdatabase%3AIdentifier%7D.%7Btable%3AIdentifier%7D+FORMAT+JSONEachRow&\
                                     param_database=my_database&\
                                     param_table=my_table"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonEachRow,
            Some(false),
            false,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=0&\
                                     query=INSERT+INTO+%7Bdatabase%3AIdentifier%7D.%7Btable%3AIdentifier%7D+FORMAT+JSONEachRow&\
                                     param_database=my_database&\
                                     param_table=my_%22table%22"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            Some(true),
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     input_format_skip_unknown_fields=1&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%7Bdatabase%3AIdentifier%7D.%7Btable%3AIdentifier%7D+FORMAT+JSONAsObject&\
                                     param_database=my_database&\
                                     param_table=my_%22table%22"
        );

        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            None,
            true,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     date_time_input_format=best_effort&\
                                     query=INSERT+INTO+%7Bdatabase%3AIdentifier%7D.%7Btable%3AIdentifier%7D+FORMAT+JSONAsObject&\
                                     param_database=my_database&\
                                     param_table=my_%22table%22"
        );
    }

    #[test]
    fn encode_async_insert_settings() {
        let uri = set_uri_query(
            &"http://localhost:80".parse().unwrap(),
            "my_database",
            "my_\"table\"",
            Format::JsonAsObject,
            None,
            true,
            false,
            QuerySettingsConfig {
                async_insert_settings: AsyncInsertSettingsConfig {
                    enabled: Some(true),
                    wait_for_processing: Some(true),
                    wait_for_processing_timeout: Some(500),
                    ..AsyncInsertSettingsConfig::default()
                },
            },
        )
        .unwrap();
        assert_eq!(
            uri.to_string(),
            "http://localhost:80/?\
                                     input_format_import_nested_json=1&\
                                     date_time_input_format=best_effort&\
                                     async_insert=1&\
                                     wait_for_async_insert=1&\
                                     wait_for_async_insert_timeout=500&\
                                     query=INSERT+INTO+%7Bdatabase%3AIdentifier%7D.%7Btable%3AIdentifier%7D+FORMAT+JSONAsObject&\
                                     param_database=my_database&\
                                     param_table=my_%22table%22"
        );
    }

    #[test]
    fn identifier_params() {
        fn params(database: &str, table: &str) -> (String, String, String) {
            let uri = set_uri_query(
                &"http://localhost:80".parse().unwrap(),
                database,
                table,
                Format::JsonEachRow,
                None,
                false,
                false,
                QuerySettingsConfig::default(),
            )
            .unwrap();
            let p = parse_query_params(&uri);
            (
                p["query"].clone(),
                p["param_database"].clone(),
                p["param_table"].clone(),
            )
        }

        // The query template is always the same fixed string regardless of identifier content.
        let template = "INSERT INTO {database:Identifier}.{table:Identifier} FORMAT JSONEachRow";

        // Plain identifiers are passed through as-is.
        let (q, db, tbl) = params("my_db", "my_table");
        assert_eq!(q, template);
        assert_eq!(db, "my_db");
        assert_eq!(tbl, "my_table");

        // Special characters are passed as raw values; ClickHouse handles quoting.
        let (q, db, tbl) = params("my_db", r#"my_"table""#);
        assert_eq!(q, template);
        assert_eq!(db, "my_db");
        assert_eq!(tbl, r#"my_"table""#);

        // Injection payload: the database and table params are independent URL parameters,
        // so there is no SQL to break out of.
        let (q, db, tbl) = params(r#"valid_db"."other_table" --"#, "my_table");
        assert_eq!(q, template);
        assert_eq!(db, r#"valid_db"."other_table" --"#);
        assert_eq!(tbl, "my_table");

        // Backslash and quotes together.
        let (q, db, tbl) = params("db_with_\\\"", "my_table");
        assert_eq!(q, template);
        assert_eq!(db, "db_with_\\\"");
        assert_eq!(tbl, "my_table");
    }

    #[test]
    fn encode_invalid() {
        set_uri_query(
            &"localhost:80".parse().unwrap(),
            "my_database",
            "my_table",
            Format::JsonEachRow,
            Some(false),
            false,
            false,
            QuerySettingsConfig::default(),
        )
        .unwrap_err();
    }
}
