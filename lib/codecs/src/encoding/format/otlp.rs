use crate::encoding::ProtobufSerializer;
use bytes::BytesMut;
use opentelemetry_proto::metrics::metric_event_to_export_request;
use opentelemetry_proto::proto::{
    DESCRIPTOR_BYTES, LOGS_REQUEST_MESSAGE_TYPE, METRICS_REQUEST_MESSAGE_TYPE,
    RESOURCE_LOGS_JSON_FIELD, RESOURCE_METRICS_JSON_FIELD, RESOURCE_SPANS_JSON_FIELD,
    TRACES_REQUEST_MESSAGE_TYPE,
};
use prost::Message;
use tokio_util::codec::Encoder;
use vector_config_macros::configurable_component;
use vector_core::{config::DataType, event::Event, schema};
use vrl::{event_path, protobuf::encode::Options, value::Value};

/// Config used to build an `OtlpSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct OtlpSerializerConfig {
    // No configuration options needed - OTLP serialization is opinionated
}

impl OtlpSerializerConfig {
    /// Build the `OtlpSerializer` from this configuration.
    pub fn build(&self) -> Result<OtlpSerializer, crate::encoding::BuildError> {
        OtlpSerializer::new()
    }

    /// The data type of events that are accepted by `OtlpSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::all_bits()
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        schema::Requirement::empty()
    }
}

/// Serializer that converts an `Event` to bytes using the OTLP (OpenTelemetry Protocol) protobuf format.
///
/// This serializer encodes events using the OTLP protobuf specification, which is the recommended
/// encoding format for OpenTelemetry data. The output is suitable for sending to OTLP-compatible
/// endpoints with `content-type: application/x-protobuf`.
///
/// # Implementation approach
///
/// This serializer converts Vector's internal event representation to the appropriate OTLP message type
/// based on the top-level field in the event:
/// - `resourceLogs` → `ExportLogsServiceRequest`
/// - `resourceMetrics` → `ExportMetricsServiceRequest`
/// - `resourceSpans` → `ExportTraceServiceRequest`
///
/// The implementation is the inverse of what the `opentelemetry` source does when decoding,
/// ensuring round-trip compatibility.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields will be used once encoding is implemented
pub struct OtlpSerializer {
    logs_descriptor: ProtobufSerializer,
    metrics_descriptor: ProtobufSerializer,
    traces_descriptor: ProtobufSerializer,
    options: Options,
}

impl OtlpSerializer {
    /// Creates a new OTLP serializer with the appropriate message descriptors.
    pub fn new() -> vector_common::Result<Self> {
        let options = Options {
            use_json_names: true,
            allow_lossy_string_coercion: true,
        };

        let logs_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            LOGS_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        let metrics_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            METRICS_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        let traces_descriptor = ProtobufSerializer::new_from_bytes(
            DESCRIPTOR_BYTES,
            TRACES_REQUEST_MESSAGE_TYPE,
            &options,
        )?;

        Ok(Self {
            logs_descriptor,
            metrics_descriptor,
            traces_descriptor,
            options,
        })
    }
}

impl Encoder<Event> for OtlpSerializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            Event::Log(mut log) => {
                if log.contains(event_path!(RESOURCE_LOGS_JSON_FIELD)) {
                    self.logs_descriptor.encode(Event::Log(log), buffer)
                } else if log.contains(event_path!(RESOURCE_METRICS_JSON_FIELD)) {
                    // Currently the OTLP metrics are Vector logs (not metrics).
                    self.metrics_descriptor.encode(Event::Log(log), buffer)
                } else if let Some(spans) = log.get_mut(event_path!(RESOURCE_SPANS_JSON_FIELD)) {
                    // trace_to_log preserves raw IDs; JSON producers use OTLP's
                    // hexadecimal representation. Both must retain their wire IDs.
                    normalize_log_trace_ids(spans)?;
                    self.traces_descriptor.encode(Event::Log(log), buffer)
                } else {
                    Err(format!(
                        "Log event does not contain OTLP top-level fields ({RESOURCE_LOGS_JSON_FIELD}, {RESOURCE_METRICS_JSON_FIELD} or {RESOURCE_SPANS_JSON_FIELD})",
                    )
                        .into())
                }
            }
            Event::Trace(trace) => {
                if trace.contains(event_path!(RESOURCE_SPANS_JSON_FIELD)) {
                    self.traces_descriptor.encode(Event::Trace(trace), buffer)
                } else {
                    Err(format!(
                        "Trace event does not contain OTLP top-level field ({RESOURCE_SPANS_JSON_FIELD})",
                    )
                        .into())
                }
            }
            Event::Metric(metric) => {
                let request = metric_event_to_export_request(metric)?;
                request.encode(buffer).map_err(Into::into)
            }
        }
    }
}

fn normalize_hex_id(value: Option<&mut Value>, wire_bytes: usize) -> vector_common::Result<()> {
    let Some(Value::Bytes(id)) = value else {
        return Ok(());
    };
    if id.len() != wire_bytes * 2 {
        // Already-decoded IDs, including an absent parent, pass through unchanged.
        return Ok(());
    }
    let decoded: Option<Vec<u8>> = id
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            Some((high * 16 + low) as u8)
        })
        .collect();
    *id = decoded
        .ok_or("OTLP hexadecimal span identifier is invalid")?
        .into();
    Ok(())
}

fn normalize_log_trace_ids(value: &mut Value) -> vector_common::Result<()> {
    let Value::Array(resources) = value else {
        return Ok(());
    };
    for resource in resources {
        let Value::Object(resource) = resource else {
            continue;
        };
        let Some(Value::Array(scopes)) = resource.get_mut("scopeSpans") else {
            continue;
        };
        for scope in scopes {
            let Value::Object(scope) = scope else {
                continue;
            };
            let Some(Value::Array(spans)) = scope.get_mut("spans") else {
                continue;
            };
            for span in spans {
                let Value::Object(span) = span else {
                    continue;
                };
                normalize_hex_id(span.get_mut("traceId"), 16)?;
                normalize_hex_id(span.get_mut("spanId"), 8)?;
                normalize_hex_id(span.get_mut("parentSpanId"), 8)?;
                if let Some(Value::Array(links)) = span.get_mut("links") {
                    for link in links {
                        if let Value::Object(link) = link {
                            normalize_hex_id(link.get_mut("traceId"), 16)?;
                            normalize_hex_id(link.get_mut("spanId"), 8)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use opentelemetry_proto::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::proto::collector::trace::v1::ExportTraceServiceRequest;
    use vector_core::event::{
        LogEvent, Metric, MetricKind, MetricTags, MetricValue, metric::Bucket,
    };

    fn trace_log() -> LogEvent {
        LogEvent::try_from(serde_json::json!({"resourceSpans": [{
            "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "nano-lake-query"}}]},
            "scopeSpans": [{"spans": [{
                "traceId": "00112233445566778899aabbccddeeff",
                "spanId": "0123456789abcdef",
                "parentSpanId": "fedcba9876543210",
                "name": "query", "kind": 1,
                "startTimeUnixNano": "1000000000", "endTimeUnixNano": "1000001000",
                "links": [{"traceId": "ffeeddccbbaa99887766554433221100", "spanId": "fedcba9876543210"}]
            }]}]
        }]})).unwrap()
    }

    fn encode_log_trace(log: LogEvent) -> ExportTraceServiceRequest {
        let mut serializer = OtlpSerializer::new().unwrap();
        let mut buffer = BytesMut::new();
        serializer.encode(Event::Log(log), &mut buffer).unwrap();
        ExportTraceServiceRequest::decode(buffer.freeze()).unwrap()
    }

    #[test]
    fn round_trip_log_traces_with_hex_ids() {
        let request = encode_log_trace(trace_log());
        let span = &request.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(
            span.trace_id,
            [
                0, 17, 34, 51, 68, 85, 102, 119, 136, 153, 170, 187, 204, 221, 238, 255
            ]
        );
        assert_eq!(span.span_id, [1, 35, 69, 103, 137, 171, 205, 239]);
        assert_eq!(span.parent_span_id, [254, 220, 186, 152, 118, 84, 50, 16]);
        assert_eq!(span.name, "query");
        assert_eq!(span.start_time_unix_nano, 1_000_000_000);
        assert_eq!(span.end_time_unix_nano, 1_000_001_000);
        assert_eq!(
            span.links[0].trace_id,
            [
                255, 238, 221, 204, 187, 170, 153, 136, 119, 102, 85, 68, 51, 34, 17, 0
            ]
        );
        assert_eq!(span.links[0].span_id, [254, 220, 186, 152, 118, 84, 50, 16]);
    }

    #[test]
    fn round_trip_log_traces_preserves_raw_ids() {
        let mut log = trace_log();
        log.insert(
            event_path!(
                "resourceSpans",
                0isize,
                "scopeSpans",
                0isize,
                "spans",
                0isize,
                "traceId"
            ),
            bytes::Bytes::from(vec![255; 16]),
        );
        log.insert(
            event_path!(
                "resourceSpans",
                0isize,
                "scopeSpans",
                0isize,
                "spans",
                0isize,
                "spanId"
            ),
            bytes::Bytes::from(vec![128; 8]),
        );
        log.insert(
            event_path!(
                "resourceSpans",
                0isize,
                "scopeSpans",
                0isize,
                "spans",
                0isize,
                "parentSpanId"
            ),
            bytes::Bytes::new(),
        );
        let request = encode_log_trace(log);
        let span = &request.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.trace_id, vec![255; 16]);
        assert_eq!(span.span_id, vec![128; 8]);
        assert!(span.parent_span_id.is_empty());
    }

    #[test]
    fn malformed_hex_log_trace_is_not_encoded() {
        let mut log = trace_log();
        log.insert(
            event_path!(
                "resourceSpans",
                0isize,
                "scopeSpans",
                0isize,
                "spans",
                0isize,
                "traceId"
            ),
            "z".repeat(32),
        );
        let mut serializer = OtlpSerializer::new().unwrap();
        let mut buffer = BytesMut::new();
        assert!(serializer.encode(Event::Log(log), &mut buffer).is_err());
        assert!(buffer.is_empty());
    }

    // `into_event_iter` always wraps attributes in `Some(MetricTags)` (via `build_metric_tags`),
    // even when there are none, so a tag-less input compares unequal to its round-tripped output
    // unless we give it the same empty-but-present tag set up front.
    fn with_empty_tags(metric: Metric) -> Metric {
        metric.with_tags(Some(MetricTags::default()))
    }

    fn round_trip_metric(metric: Metric) -> Metric {
        let mut serializer = OtlpSerializer::new().unwrap();
        let mut buffer = BytesMut::new();
        serializer
            .encode(Event::Metric(metric), &mut buffer)
            .expect("encode should succeed");

        let request =
            ExportMetricsServiceRequest::decode(buffer.freeze()).expect("decode should succeed");
        let mut events: Vec<Event> = request
            .resource_metrics
            .into_iter()
            .flat_map(|rm| rm.into_event_iter())
            .collect();

        assert_eq!(events.len(), 1);
        match events.remove(0) {
            Event::Metric(metric) => metric,
            other => panic!("expected a metric event, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_counter() {
        let metric = with_empty_tags(
            Metric::new(
                "requests",
                MetricKind::Incremental,
                MetricValue::Counter { value: 42.0 },
            )
            .with_timestamp(Some(Utc.timestamp_nanos(1_000_000_000))),
        );

        assert_eq!(metric.clone(), round_trip_metric(metric));
    }

    #[test]
    fn round_trip_gauge() {
        let metric = with_empty_tags(
            Metric::new(
                "cpu_usage",
                MetricKind::Absolute,
                MetricValue::Gauge { value: 12.5 },
            )
            .with_timestamp(Some(Utc.timestamp_nanos(1_000_000_000))),
        );

        assert_eq!(metric.clone(), round_trip_metric(metric));
    }

    #[test]
    fn round_trip_aggregated_histogram() {
        let metric = with_empty_tags(
            Metric::new(
                "latency",
                MetricKind::Absolute,
                MetricValue::AggregatedHistogram {
                    buckets: vec![
                        Bucket {
                            upper_limit: 1.0,
                            count: 1,
                        },
                        Bucket {
                            upper_limit: 2.0,
                            count: 2,
                        },
                        Bucket {
                            upper_limit: f64::INFINITY,
                            count: 3,
                        },
                    ],
                    count: 6,
                    sum: 10.0,
                },
            )
            .with_timestamp(Some(Utc.timestamp_nanos(1_000_000_000))),
        );

        assert_eq!(metric.clone(), round_trip_metric(metric));
    }

    #[test]
    fn round_trip_aggregated_summary() {
        let metric = with_empty_tags(
            Metric::new(
                "response_time",
                MetricKind::Absolute,
                MetricValue::AggregatedSummary {
                    quantiles: vec![
                        vector_core::event::metric::Quantile {
                            quantile: 0.5,
                            value: 10.0,
                        },
                        vector_core::event::metric::Quantile {
                            quantile: 0.99,
                            value: 20.0,
                        },
                    ],
                    count: 100,
                    sum: 1000.0,
                },
            )
            .with_timestamp(Some(Utc.timestamp_nanos(1_000_000_000))),
        );

        assert_eq!(metric.clone(), round_trip_metric(metric));
    }

    #[test]
    fn unsupported_metric_values_return_err() {
        let mut serializer = OtlpSerializer::new().unwrap();
        let mut buffer = BytesMut::new();

        let set_metric = Metric::new(
            "unique_users",
            MetricKind::Incremental,
            MetricValue::Set {
                values: std::iter::once("a".to_string()).collect(),
            },
        );
        assert!(
            serializer
                .encode(Event::Metric(set_metric), &mut buffer)
                .is_err()
        );

        let distribution_metric = Metric::new(
            "latencies",
            MetricKind::Incremental,
            MetricValue::Distribution {
                samples: Vec::new(),
                statistic: vector_core::event::metric::StatisticKind::Histogram,
            },
        );
        assert!(
            serializer
                .encode(Event::Metric(distribution_metric), &mut buffer)
                .is_err()
        );
    }
}
