//! Common logic for sources that are HTTP clients.
//!
//! Specific HTTP client sources will:
//!   - Call build_url() to build the URL(s) to call.
//!   - Implement a specific context struct which:
//!       - Contains the data that source needs in order to process the HTTP responses into internal_events
//!       - Implements the HttpClient trait
//!   - Call call() supplying the generic inputs for calling and the source-specific
//!     context.

// Okta source only imports defaults but doesn't use the rest of the client
#![cfg_attr(feature = "sources-okta", allow(dead_code))]

use std::{collections::HashMap, future::ready, time::Duration};

use bytes::{Bytes, BytesMut};
use futures_util::{FutureExt, StreamExt, stream};
use http::{
    HeaderMap, Uri,
    header::{HeaderName, HeaderValue},
    response::Parts,
};
use hyper::{Body, Request};
use snafu::{ResultExt, Snafu};
use tokio_stream::wrappers::IntervalStream;
use vector_lib::{
    EstimatedJsonEncodedSizeOf, config::proxy::ProxyConfig, event::Event, json_size::JsonSize,
    shutdown::ShutdownSignal,
};

use crate::{
    SourceSender,
    http::{Auth, HttpClient, QueryParameterValue, QueryParameters},
    internal_events::{
        EndpointBytesReceived, HttpClientEventsReceived, HttpClientHttpError,
        HttpClientHttpResponseError, StreamClosedError,
    },
    sources::util::http::HttpMethod,
    tls::TlsSettings,
};

/// Contains the inputs generic to any http client.
pub(crate) struct GenericHttpClientInputs {
    /// Array of URLs to call.
    pub urls: Vec<Uri>,
    /// Interval between calls.
    pub interval: Duration,
    /// Timeout for the HTTP request.
    pub timeout: Duration,
    /// Maximum buffered HTTP response size in bytes.
    pub max_response_bytes: usize,
    /// Map of Header+Value to apply to HTTP request.
    pub headers: HeaderMap<HeaderValue>,
    /// Content type of the HTTP request, determined by the source.
    pub content_type: String,
    pub auth: Option<Auth>,
    pub tls: TlsSettings,
    pub proxy: ProxyConfig,
    pub shutdown: ShutdownSignal,
}

#[derive(Debug, Snafu)]
pub(crate) enum HeaderError {
    #[snafu(display("Invalid HTTP header name {name:?}: {source}"))]
    InvalidName {
        name: String,
        source: http::header::InvalidHeaderName,
    },
    #[snafu(display("Invalid value for HTTP header {name:?}: {source}"))]
    InvalidValue {
        name: String,
        source: http::header::InvalidHeaderValue,
    },
}

pub(crate) fn build_headers(
    headers: &HashMap<String, Vec<String>>,
) -> Result<HeaderMap<HeaderValue>, HeaderError> {
    let mut parsed = HeaderMap::new();

    for (name, values) in headers {
        let header_name =
            HeaderName::from_bytes(name.as_bytes()).with_context(|_| InvalidNameSnafu {
                name: name.to_owned(),
            })?;

        for value in values {
            let mut header_value =
                HeaderValue::from_bytes(value.as_bytes()).with_context(|_| InvalidValueSnafu {
                    name: name.to_owned(),
                })?;
            header_value.set_sensitive(true);
            parsed.append(header_name.clone(), header_value);
        }
    }

    Ok(parsed)
}

/// The default interval to call the HTTP endpoint if none is configured.
pub(crate) const fn default_interval() -> Duration {
    Duration::from_secs(15)
}

/// The default timeout for the HTTP request if none is configured.
pub(crate) const fn default_timeout() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum buffered response: 32 MiB.
pub(crate) const fn default_max_response_bytes() -> usize {
    32 * 1024 * 1024
}

fn scrape_interval(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

async fn collect_response(
    mut body: Body,
    max_response_bytes: usize,
) -> Result<Bytes, crate::Error> {
    use hyper::body::HttpBody;

    // Check each chunk before copying; never collect an unbounded body first.
    let mut bytes = BytesMut::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        if chunk.len() > max_response_bytes.saturating_sub(bytes.len()) {
            return Err(
                format!("HTTP response exceeds max_response_bytes ({max_response_bytes})").into(),
            );
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

async fn fetch_response(
    client: HttpClient,
    request: Request<Body>,
    timeout: Duration,
    max_response_bytes: usize,
) -> Result<(Parts, Bytes), crate::Error> {
    tokio::time::timeout(timeout, async move {
        let response = client.send(request).await?;
        let (header, body) = response.into_parts();
        let body = collect_response(body, max_response_bytes).await?;
        Ok((header, body))
    })
    .await
    .map_err(|_| -> crate::Error {
        format!(
            "Timeout error: request including response body exceeded {}s",
            timeout.as_secs_f64()
        )
        .into()
    })?
}

/// Builds the context, allowing the source-specific implementation to leverage data from the
/// config and the current HTTP request.
pub(crate) trait HttpClientBuilder {
    type Context: HttpClientContext;

    /// Called before the HTTP request is made to build out the context.
    fn build(&self, url: &Uri) -> Self::Context;
}

/// Methods that allow context-specific behavior during the scraping procedure.
pub(crate) trait HttpClientContext {
    /// Called after the HTTP request succeeds and returns the decoded/parsed Event array.
    fn on_response(&mut self, url: &Uri, header: &Parts, body: &Bytes) -> Option<Vec<Event>>;

    /// (Optional) Called if the HTTP response is not 200 ('OK').
    fn on_http_response_error(&self, _uri: &Uri, _header: &Parts) {}

    /// (Optional) Process the base URL before each request.
    /// Allows for dynamic query parameters that update at runtime.
    /// Returns a new URL if parameters need to be updated, or None to use the original URL.
    fn process_url(&self, _url: &Uri) -> Option<Uri> {
        None
    }

    /// (Optional) Get the request body to send with the HTTP request.
    /// Returns the body as a String if one should be sent, or None for an empty body.
    fn get_request_body(&self) -> Option<String> {
        None
    }

    // This function can be defined to enrich events with additional HTTP
    // metadata. This function should be used rather than internal enrichment so
    // that accurate byte count metrics can be emitted.
    fn enrich_events(&mut self, _events: &mut Vec<Event>) {}
}

/// Builds a url for the HTTP requests.
pub(crate) fn build_url(uri: &Uri, query: &QueryParameters) -> Uri {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    if let Some(query) = uri.query() {
        serializer.extend_pairs(url::form_urlencoded::parse(query.as_bytes()));
    };
    for (k, query_value) in query {
        match query_value {
            QueryParameterValue::SingleParam(param) => {
                serializer.append_pair(k, param.value());
            }
            QueryParameterValue::MultiParams(params) => {
                for v in params {
                    serializer.append_pair(k, v.value());
                }
            }
        };
    }
    let mut builder = Uri::builder();
    if let Some(scheme) = uri.scheme() {
        builder = builder.scheme(scheme.clone());
    };
    if let Some(authority) = uri.authority() {
        builder = builder.authority(authority.clone());
    };
    builder = builder.path_and_query(match serializer.finish() {
        query if !query.is_empty() => format!("{}?{query}", uri.path()),
        _ => uri.path().to_string(),
    });
    builder
        .build()
        .expect("Failed to build URI from parsed arguments")
}

/// Warns if the scrape timeout is greater than the scrape interval.
pub(crate) fn warn_if_interval_too_low(timeout: Duration, interval: Duration) {
    if timeout > interval {
        warn!(
            interval_secs = %interval.as_secs_f64(),
            timeout_secs = %timeout.as_secs_f64(),
            message = "Having a scrape timeout that exceeds the scrape interval can lead to excessive resource consumption.",
        );
    }
}

/// Calls one or more urls at an interval.
///   - The HTTP request is built per the options in provided generic inputs.
///   - The HTTP response is decoded/parsed into events by the specific context.
///   - The events are then sent to the output stream.
pub(crate) async fn call<
    B: HttpClientBuilder<Context = C> + Send + Clone,
    C: HttpClientContext + Send,
>(
    inputs: GenericHttpClientInputs,
    context_builder: B,
    mut out: SourceSender,
    http_method: HttpMethod,
) -> Result<(), ()> {
    // Building the HttpClient should not fail as it is just setting up the client with the
    // proxy and tls settings.
    let client =
        HttpClient::new(inputs.tls.clone(), &inputs.proxy).expect("Building HTTP client failed");
    let mut stream = IntervalStream::new(scrape_interval(inputs.interval))
        .take_until(inputs.shutdown)
        .map(move |_| stream::iter(inputs.urls.clone()))
        .flatten()
        .map(move |base_url| {
            let client = client.clone();
            let endpoint = base_url.to_string();

            let context_builder = context_builder.clone();
            let mut context = context_builder.build(&base_url);

            // Check if we need to process the URL dynamically (for updating VRL expressions)
            let url = context.process_url(&base_url).unwrap_or(base_url);

            let mut builder = match http_method {
                HttpMethod::Head => Request::head(&url),
                HttpMethod::Get => Request::get(&url),
                HttpMethod::Post => Request::post(&url),
                HttpMethod::Put => Request::put(&url),
                HttpMethod::Patch => Request::patch(&url),
                HttpMethod::Delete => Request::delete(&url),
                HttpMethod::Options => Request::options(&url),
            };

            // add user specified headers
            for (header, value) in &inputs.headers {
                builder = builder.header(header, value);
            }

            // set ACCEPT header if not user specified
            if !inputs.headers.contains_key(http::header::ACCEPT.as_str()) {
                builder = builder.header(http::header::ACCEPT, &inputs.content_type);
            }

            // Get the request body from the context (if any)
            let body = match context.get_request_body() {
                Some(body_str) => {
                    // Set Content-Type header if not already set
                    if !inputs
                        .headers
                        .contains_key(http::header::CONTENT_TYPE.as_str())
                    {
                        builder = builder.header(http::header::CONTENT_TYPE, "application/json");
                    }
                    Body::from(body_str)
                }
                None => Body::empty(),
            };

            // building the request should be infallible
            let mut request = builder.body(body).expect("error creating request");

            if let Some(auth) = &inputs.auth {
                auth.apply(&mut request);
            }

            fetch_response(client, request, inputs.timeout, inputs.max_response_bytes)
                .map(move |response| {
                    if let Ok((_, body)) = &response {
                        emit!(EndpointBytesReceived {
                            byte_size: body.len(),
                            protocol: "http",
                            endpoint: endpoint.as_str(),
                        });
                    }
                    response
                })
                .into_stream()
                .filter_map(move |response| {
                    ready(match response {
                        Ok((header, body)) if header.status == hyper::StatusCode::OK => {
                            context.on_response(&url, &header, &body).map(|mut events| {
                                let byte_size = if events.is_empty() {
                                    // We need to explicitly set the byte size
                                    // to 0 since
                                    // `estimated_json_encoded_size_of` returns
                                    // at least 1 for an empty collection. For
                                    // the purposes of the
                                    // HttpClientEventsReceived event, we should
                                    // emit 0 when there aren't any usable
                                    // metrics.
                                    JsonSize::zero()
                                } else {
                                    events.estimated_json_encoded_size_of()
                                };

                                emit!(HttpClientEventsReceived {
                                    byte_size,
                                    count: events.len(),
                                    url: url.to_string()
                                });

                                // We'll enrich after receiving the events so
                                // that the byte sizes are accurate.
                                context.enrich_events(&mut events);

                                stream::iter(events)
                            })
                        }
                        Ok((header, _)) => {
                            context.on_http_response_error(&url, &header);
                            emit!(HttpClientHttpResponseError {
                                code: header.status,
                                url: url.to_string(),
                            });
                            None
                        }
                        Err(error) => {
                            emit!(HttpClientHttpError {
                                error,
                                url: url.to_string()
                            });
                            None
                        }
                    })
                })
                .flatten()
                .boxed()
        })
        .flatten_unordered(Some(1))
        .boxed();

    match out.send_event_stream(&mut stream).await {
        Ok(()) => {
            debug!("Finished sending.");
            Ok(())
        }
        Err(_) => {
            let (count, _) = stream.size_hint();
            emit!(StreamClosedError { count });
            Err(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn missed_scrapes_do_not_burst() {
        let mut interval = scrape_interval(Duration::from_secs(10));
        interval.tick().await;
        tokio::time::advance(Duration::from_secs(95)).await;
        interval.tick().await;
        assert!(interval.tick().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(interval.tick().now_or_never().is_some());
    }

    #[tokio::test]
    async fn response_body_limit_accepts_exact_size_and_rejects_larger() {
        assert_eq!(
            collect_response(Body::from("abcd"), 4).await.unwrap(),
            "abcd"
        );
        assert!(collect_response(Body::from("abcde"), 4).await.is_err());
        let (mut sender, body) = Body::channel();
        tokio::spawn(async move {
            sender.send_data(Bytes::from_static(b"abc")).await.unwrap();
            drop(sender.send_data(Bytes::from_static(b"de")).await);
        });
        assert!(collect_response(body, 4).await.is_err());
    }

    fn test_server(
        handler: impl Fn(Request<Body>) -> hyper::Response<Body> + Send + Sync + 'static,
    ) -> (Uri, tokio::task::JoinHandle<()>) {
        use hyper::service::{make_service_fn, service_fn};
        use std::{convert::Infallible, sync::Arc};
        let handler = Arc::new(handler);
        let server = hyper::Server::bind(&"127.0.0.1:0".parse().unwrap()).serve(make_service_fn(
            move |_| {
                let handler = Arc::clone(&handler);
                async move {
                    Ok::<_, Infallible>(service_fn(move |request| {
                        ready(Ok::<_, Infallible>(handler(request)))
                    }))
                }
            },
        ));
        let url = format!("http://{}/", server.local_addr()).parse().unwrap();
        (url, tokio::spawn(async move { server.await.unwrap() }))
    }

    #[tokio::test]
    async fn timeout_covers_stalled_response_body() {
        let senders = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let retained = std::sync::Arc::clone(&senders);
        let (url, server) = test_server(move |_| {
            let (sender, body) = Body::channel();
            retained.lock().unwrap().push(sender);
            hyper::Response::new(body)
        });
        let client = HttpClient::new(TlsSettings::default(), &ProxyConfig::default()).unwrap();
        let error = fetch_response(
            client,
            Request::get(url).body(Body::empty()).unwrap(),
            Duration::from_millis(100),
            1024,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("including response body"));
        assert_eq!(senders.lock().unwrap().len(), 1);
        server.abort();
    }

    #[derive(Clone)]
    struct EmptyContext;
    impl HttpClientBuilder for EmptyContext {
        type Context = Self;
        fn build(&self, _: &Uri) -> Self {
            self.clone()
        }
    }
    impl HttpClientContext for EmptyContext {
        fn on_response(&mut self, _: &Uri, _: &Parts, _: &Bytes) -> Option<Vec<Event>> {
            Some(Vec::new())
        }
    }

    #[tokio::test]
    async fn source_allows_only_one_in_flight_request_across_endpoints_and_ticks() {
        let senders = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let retained = std::sync::Arc::clone(&senders);
        let (url, server) = test_server(move |_| {
            let (sender, body) = Body::channel();
            retained.lock().unwrap().push(sender);
            hyper::Response::new(body)
        });
        let (out, _rx) = SourceSender::new_test();
        let cx = crate::config::SourceContext::new_test(out.clone(), None);
        let inputs = GenericHttpClientInputs {
            urls: vec![url.clone(), url],
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(5),
            max_response_bytes: 1024,
            headers: HeaderMap::new(),
            content_type: "text/plain".into(),
            auth: None,
            tls: TlsSettings::default(),
            proxy: ProxyConfig::default(),
            shutdown: cx.shutdown,
        };
        let task = tokio::spawn(call(inputs, EmptyContext, out, HttpMethod::Get));
        tokio::time::timeout(Duration::from_secs(2), async {
            while senders.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(senders.lock().unwrap().len(), 1);
        senders.lock().unwrap().clear(); // Complete the first body, permitting the second endpoint.
        tokio::time::timeout(Duration::from_secs(2), async {
            while senders.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(senders.lock().unwrap().len(), 1);
        task.abort();
        server.abort();
    }

    #[test]
    fn headers_are_normalized_and_sensitive() {
        let headers = build_headers(&HashMap::from([(
            "X-Private-Token".to_string(),
            vec!["secret".to_string(), "another-secret".to_string()],
        )]))
        .unwrap();

        let values = headers
            .get_all("x-private-token")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|value| value.is_sensitive()));
    }

    #[test]
    fn invalid_header_name_is_rejected() {
        let error = build_headers(&HashMap::from([(
            "invalid header".to_string(),
            vec!["value".to_string()],
        )]))
        .unwrap_err();

        assert!(error.to_string().contains("Invalid HTTP header name"));
    }

    #[test]
    fn invalid_header_value_is_rejected() {
        let error = build_headers(&HashMap::from([(
            "x-header".to_string(),
            vec!["invalid\nvalue".to_string()],
        )]))
        .unwrap_err();

        assert!(error.to_string().contains("Invalid value for HTTP header"));
    }
}
