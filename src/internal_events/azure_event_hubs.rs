use vector_lib::{
    NamedInternalEvent, counter,
    internal_event::{CounterName, InternalEvent, error_stage, error_type},
};

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsConnectError<'a> {
    pub partition_id: &'a str,
    pub error: azure_messaging_eventhubs::EventHubsError,
}

impl InternalEvent for AzureEventHubsConnectError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to open Event Hubs partition receiver.",
            partition_id = %self.partition_id,
            error = %self.error,
            error_code = "failed_opening_receiver",
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_opening_receiver",
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsReceiveError<'a> {
    pub partition_id: &'a str,
    pub error: azure_messaging_eventhubs::EventHubsError,
}

impl InternalEvent for AzureEventHubsReceiveError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to receive events from Event Hubs.",
            partition_id = %self.partition_id,
            error = %self.error,
            error_code = "failed_receiving_events",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_receiving_events",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsUnsupportedBody<'a> {
    pub partition_id: &'a str,
    pub kind: &'static str,
}

impl InternalEvent for AzureEventHubsUnsupportedBody<'_> {
    fn emit(self) {
        error!(
            message = "Dropped an Event Hubs event with an unsupported AMQP body.",
            partition_id = %self.partition_id,
            body_kind = self.kind,
            error_code = "unsupported_body",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "unsupported_body",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsCheckpointError<'a> {
    pub partition_id: &'a str,
    pub error: azure_core::Error,
}

impl InternalEvent for AzureEventHubsCheckpointError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to write Event Hubs checkpoint. Will retry.",
            partition_id = %self.partition_id,
            error = %self.error,
            error_code = "failed_writing_checkpoint",
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_writing_checkpoint",
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsDeliveryFailed<'a> {
    pub partition_id: &'a str,
}

impl InternalEvent for AzureEventHubsDeliveryFailed<'_> {
    fn emit(self) {
        warn!(
            message = "Downstream delivery failed; replaying the partition from the last acknowledged event.",
            partition_id = %self.partition_id,
        );
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsCheckpointCommitted<'a> {
    pub partition_id: &'a str,
    pub sequence_number: Option<i64>,
}

impl InternalEvent for AzureEventHubsCheckpointCommitted<'_> {
    fn emit(self) {
        debug!(
            message = "Committed Event Hubs checkpoint.",
            partition_id = %self.partition_id,
            sequence_number = ?self.sequence_number,
        );
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsPartitionPropertiesError<'a> {
    pub partition_id: &'a str,
    pub error: azure_messaging_eventhubs::EventHubsError,
}

impl InternalEvent for AzureEventHubsPartitionPropertiesError<'_> {
    fn emit(self) {
        error!(
            message = "Failed to read Event Hubs partition properties. Will retry.",
            partition_id = %self.partition_id,
            error = %self.error,
            error_code = "failed_reading_partition_properties",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_reading_partition_properties",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsInitialCheckpoint<'a> {
    pub partition_id: &'a str,
    pub outcome: &'a crate::sources::azure_event_hubs::InitialCheckpoint,
}

impl InternalEvent for AzureEventHubsInitialCheckpoint<'_> {
    fn emit(self) {
        use crate::sources::azure_event_hubs::InitialCheckpoint;
        match self.outcome {
            InitialCheckpoint::Created(position) => info!(
                message = "Wrote initial Event Hubs checkpoint at the partition's last enqueued event.",
                partition_id = %self.partition_id,
                offset = ?position.offset,
                sequence_number = ?position.sequence_number,
            ),
            InitialCheckpoint::Existing(position) => info!(
                message = "Event Hubs partition got a checkpoint from another writer; resuming from it.",
                partition_id = %self.partition_id,
                offset = ?position.offset,
                sequence_number = ?position.sequence_number,
            ),
            InitialCheckpoint::Unreadable(position) => warn!(
                message = "Event Hubs partition has a checkpoint without a position; kept it and starting after the last enqueued event.",
                partition_id = %self.partition_id,
                offset = ?position.offset,
                sequence_number = ?position.sequence_number,
            ),
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsStartupError<'a> {
    pub stage: &'static str,
    pub error: &'a str,
    pub retry_in: std::time::Duration,
}

impl InternalEvent for AzureEventHubsStartupError<'_> {
    fn emit(self) {
        error!(
            message = "Event Hubs source could not start yet. Will retry.",
            stage_detail = %self.stage,
            error = %self.error,
            retry_in_secs = self.retry_in.as_secs(),
            error_code = "failed_starting",
            error_type = error_type::REQUEST_FAILED,
            stage = error_stage::RECEIVING,
        );
        counter!(
            CounterName::ComponentErrorsTotal,
            "error_code" => "failed_starting",
            "error_type" => error_type::REQUEST_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub struct AzureEventHubsStarted {
    pub failures: usize,
}

impl InternalEvent for AzureEventHubsStarted {
    fn emit(self) {
        info!(
            message = "Event Hubs source started after failed attempts.",
            failed_attempts = self.failures,
        );
    }
}
