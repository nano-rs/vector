//! Internal events of the `crowdstrike_fdr` source.

use std::time::Duration;

use vector_lib::{
    NamedInternalEvent, counter, gauge, histogram,
    internal_event::{CounterName, GaugeName, HistogramName, InternalEvent},
};

/// What happened to one SQS manifest message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdrManifestStatus {
    /// Received from the queue.
    Received,
    /// Every event was delivered (or dispatched, without acknowledgements) and the message deleted.
    Acked,
    /// Processing failed or the hold ended; the message is left to re-deliver.
    Released,
    /// Older than the `start_from` cutoff; deleted without downloading.
    Skipped,
    /// Received more than `max_tries` times; left alone for the queue's dead-letter policy.
    Parked,
}

impl FdrManifestStatus {
    /// Every status, for registering the counters up front.
    pub const ALL: [Self; 5] = [
        Self::Received,
        Self::Acked,
        Self::Released,
        Self::Skipped,
        Self::Parked,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Acked => "acked",
            Self::Released => "released",
            Self::Skipped => "skipped",
            Self::Parked => "parked",
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrManifest<'a> {
    pub status: FdrManifestStatus,
    pub message_id: &'a str,
}

impl InternalEvent for CrowdstrikeFdrManifest<'_> {
    fn emit(self) {
        match self.status {
            FdrManifestStatus::Parked => warn!(
                message = "FDR manifest exceeded max_tries; leaving it for the queue's dead-letter policy.",
                message_id = %self.message_id,
            ),
            FdrManifestStatus::Released => debug!(
                message = "Released FDR manifest for re-delivery.",
                message_id = %self.message_id,
            ),
            _ => trace!(
                message = "FDR manifest.",
                status = self.status.as_str(),
                message_id = %self.message_id,
            ),
        }
        counter!(
            CounterName::CrowdstrikeFdrManifestsTotal,
            "status" => self.status.as_str(),
        )
        .increment(1);
    }
}

/// The source's own metrics, registered (at zero) when the source starts and kept for its
/// lifetime. Vector expires internal metrics that are idle for `expire_metrics_secs` (default
/// 300 s) unless a handle to them is still held; a batch source can be idle far longer than
/// that, and these series are the record that a batch was received, deleted or released.
pub struct CrowdstrikeFdrMetrics {
    _counters: Vec<metrics::Counter>,
    _gauges: Vec<metrics::Gauge>,
    _histograms: Vec<metrics::Histogram>,
}

impl CrowdstrikeFdrMetrics {
    /// Registers every series. Call inside the component's span, so the series carry its tags.
    pub fn register() -> Self {
        let mut counters = Vec::new();
        let mut gauges = Vec::new();
        for status in FdrManifestStatus::ALL {
            let counter = counter!(
                CounterName::CrowdstrikeFdrManifestsTotal,
                "status" => status.as_str(),
            );
            counter.increment(0);
            counters.push(counter);
        }
        for name in [
            CounterName::CrowdstrikeFdrFilesFetchedTotal,
            CounterName::CrowdstrikeFdrBytesFetchedTotal,
            CounterName::CrowdstrikeFdrFilesSkippedTotal,
        ] {
            let counter = counter!(name);
            counter.increment(0);
            counters.push(counter);
        }
        for name in [
            GaugeName::CrowdstrikeFdrInflightManifests,
            GaugeName::CrowdstrikeFdrInflightBytes,
        ] {
            let gauge = gauge!(name);
            gauge.increment(0.0);
            gauges.push(gauge);
        }
        // Registered without a sample (a zero would skew the distribution); the handle keeps
        // it from expiring once the first batch records one.
        let histograms = vec![histogram!(HistogramName::CrowdstrikeFdrBatchLagSeconds)];
        Self {
            _counters: counters,
            _gauges: gauges,
            _histograms: histograms,
        }
    }
}

/// Files of a re-delivered batch skipped because an earlier delivery already landed them.
#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrFilesSkipped<'a> {
    pub message_id: &'a str,
    pub skipped: usize,
    pub remaining: usize,
}

impl InternalEvent for CrowdstrikeFdrFilesSkipped<'_> {
    fn emit(self) {
        info!(
            message = "Resuming a re-delivered FDR batch: skipping files already delivered.",
            message_id = %self.message_id,
            skipped = self.skipped,
            remaining = self.remaining,
        );
        counter!(CounterName::CrowdstrikeFdrFilesSkippedTotal).increment(self.skipped as u64);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrFileFetched<'a> {
    pub key: &'a str,
    pub bytes: u64,
}

impl InternalEvent for CrowdstrikeFdrFileFetched<'_> {
    fn emit(self) {
        trace!(message = "Fetched FDR file.", key = %self.key, bytes = self.bytes);
        counter!(CounterName::CrowdstrikeFdrFilesFetchedTotal).increment(1);
        counter!(CounterName::CrowdstrikeFdrBytesFetchedTotal).increment(self.bytes);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrBatchLag {
    pub lag: Duration,
}

impl InternalEvent for CrowdstrikeFdrBatchLag {
    fn emit(self) {
        histogram!(HistogramName::CrowdstrikeFdrBatchLagSeconds).record(self.lag.as_secs_f64());
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrInflight {
    pub manifests: usize,
    pub bytes: u64,
}

impl InternalEvent for CrowdstrikeFdrInflight {
    #[allow(clippy::cast_precision_loss, reason = "gauge values are f64")]
    fn emit(self) {
        gauge!(GaugeName::CrowdstrikeFdrInflightManifests).set(self.manifests as f64);
        gauge!(GaugeName::CrowdstrikeFdrInflightBytes).set(self.bytes as f64);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrQueueDepth {
    pub visible: Option<u64>,
    pub not_visible: Option<u64>,
}

impl InternalEvent for CrowdstrikeFdrQueueDepth {
    #[allow(clippy::cast_precision_loss, reason = "gauge values are f64")]
    fn emit(self) {
        if let Some(visible) = self.visible {
            gauge!(GaugeName::CrowdstrikeFdrQueueDepth, "state" => "visible").set(visible as f64);
        }
        if let Some(not_visible) = self.not_visible {
            gauge!(GaugeName::CrowdstrikeFdrQueueDepth, "state" => "in_flight")
                .set(not_visible as f64);
        }
    }
}

/// Any error of the source. `error_code` names the operation; the standard
/// `component_errors_total` counter carries it.
#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrError<'a> {
    pub error_code: &'static str,
    pub error_type: &'static str,
    pub stage: &'static str,
    pub message_id: &'a str,
    pub error: String,
}

impl InternalEvent for CrowdstrikeFdrError<'_> {
    fn emit(self) {
        error!(
            message = "CrowdStrike FDR source error.",
            message_id = %self.message_id,
            error = %self.error,
            error_code = self.error_code,
            error_type = self.error_type,
            stage = self.stage,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => self.error_code,
            "error_type" => self.error_type,
            "stage" => self.stage,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrChecksumWarning<'a> {
    pub key: &'a str,
    pub detail: String,
}

impl InternalEvent for CrowdstrikeFdrChecksumWarning<'_> {
    fn emit(self) {
        warn!(
            message = "FDR file checksum did not verify; continuing (checksum = warn).",
            key = %self.key,
            detail = %self.detail,
        );
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct CrowdstrikeFdrHoldExpired<'a> {
    pub message_id: &'a str,
    pub held: Duration,
}

impl InternalEvent for CrowdstrikeFdrHoldExpired<'_> {
    fn emit(self) {
        warn!(
            message = "FDR manifest reached max_hold_secs before every event was acknowledged; it will re-deliver.",
            message_id = %self.message_id,
            held_secs = self.held.as_secs(),
        );
    }
}
