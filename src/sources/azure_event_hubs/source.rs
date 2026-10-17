//! The per-partition receive loop of the `azure_event_hubs` source.

use std::{sync::Arc, time::Duration};

use azure_core_amqp::{AmqpSimpleValue, AmqpValue, message::AmqpMessageBody};
use azure_messaging_eventhubs::{
    ConsumerClient, OpenReceiverOptions, StartPosition, models::ReceivedEventData,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::{Decoder, DecoderFramedRead, StreamDecodingError},
    config::{LegacyKey, LogNamespace},
    event::{BatchNotifier, BatchStatus, Event},
    finalization::AddBatchNotifier,
    finalizer::OrderedFinalizer,
    internal_event::{
        ByteSize, BytesReceived, CountByteSize, EventsReceived, InternalEventHandle as _, Protocol,
        Registered,
    },
    shutdown::ShutdownSignal,
};
use vrl::{
    path,
    value::{KeyString, ObjectMap, Value},
};

use super::{
    AzureEventHubsSourceConfig,
    checkpoint::{Checkpointer, InitialCheckpoint, Position},
};
use crate::{
    SourceSender,
    internal_events::{
        AzureEventHubsCheckpointCommitted, AzureEventHubsCheckpointError,
        AzureEventHubsConnectError, AzureEventHubsDeliveryFailed, AzureEventHubsInitialCheckpoint,
        AzureEventHubsPartitionPropertiesError, AzureEventHubsReceiveError,
        AzureEventHubsUnsupportedBody, StreamClosedError,
    },
};

pub(super) const BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How long a partition waits on shutdown for in-flight events to be acknowledged.
const SHUTDOWN_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a receiver close may take before it is abandoned.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reads one partition and checkpoints it.
pub(super) struct PartitionReader {
    pub client: Arc<ConsumerClient>,
    pub partition_id: String,
    pub event_hub_name: String,
    pub checkpointer: Option<Checkpointer>,
    /// The checkpointed position at startup.
    pub initial_position: Option<Position>,
    /// Where to start when there is no checkpoint.
    pub default_start: StartPosition,
    /// When the partition has no checkpoint, resolve what `latest` means now and write it as the
    /// first checkpoint before reading (`start_position = "latest"` with a checkpoint store).
    /// Without it, a restart before the partition's first event would start at `latest` again
    /// and skip everything enqueued while the source was down.
    pub initial_checkpoint: bool,
    pub prefetch: Option<u32>,
    pub owner_level: Option<i64>,
    pub checkpoint_interval: Duration,
    pub decoder: Decoder,
    pub out: SourceSender,
    pub acknowledgements: bool,
    pub log_namespace: LogNamespace,
    pub shutdown: ShutdownSignal,
}

/// Why the receive loop of one connection ended.
enum LoopExit {
    Shutdown,
    /// The receiver failed or ended; reconnect after the last received event.
    Reconnect,
    /// A downstream delivery failed; replay from the last acknowledged event.
    Replay,
    /// The pipeline is gone.
    Closed,
}

/// Tracks the positions of one partition.
#[derive(Debug, Default)]
struct PositionState {
    /// The last position written to the checkpoint store.
    committed: Option<Position>,
    /// The highest position up to which every event has been acknowledged.
    acknowledged: Option<Position>,
    /// The last position handed to the pipeline. Reconnects resume after it, since the events up
    /// to it are already in flight.
    received: Option<Position>,
    /// The first position received while nothing was acknowledged yet. A replay with no
    /// acknowledged position restarts here, so a failure before the first acknowledgement
    /// cannot skip events by falling back to `latest`.
    first_unacknowledged: Option<Position>,
}

impl PositionState {
    fn new(initial: Option<Position>) -> Self {
        Self {
            committed: initial.clone(),
            acknowledged: initial,
            ..Default::default()
        }
    }

    /// The state after [`Checkpointer::initialize`]: the partition resumes after the position it
    /// returned, which is what the store now holds (or keeps, see [`InitialCheckpoint`]).
    fn initialized(outcome: &InitialCheckpoint) -> Self {
        Self::new(Some(outcome.position().clone()))
    }

    fn on_received(&mut self, position: Position, acknowledged_on_send: bool) {
        if self.acknowledged.is_none() && self.first_unacknowledged.is_none() {
            self.first_unacknowledged = Some(position.clone());
        }
        if acknowledged_on_send {
            self.acknowledged = Some(position.clone());
        }
        self.received = Some(position);
    }

    fn on_acknowledged(&mut self, position: Position) {
        self.acknowledged = Some(position);
    }

    /// The start position after a reconnect: right after the last event already in flight.
    fn reconnect_start(&self, default: &StartPosition) -> StartPosition {
        match &self.received {
            Some(received) => received.after(),
            None => self.replay_start(default),
        }
    }

    /// The start position for a replay: right after the last acknowledged event.
    fn replay_start(&self, default: &StartPosition) -> StartPosition {
        match (&self.acknowledged, &self.first_unacknowledged) {
            (Some(acknowledged), _) => acknowledged.after(),
            (None, Some(first)) => first.at(),
            (None, None) => default.clone(),
        }
    }

    /// Forgets the in-flight positions, so the next connection replays from `replay_start`.
    fn reset_to_acknowledged(&mut self) {
        self.received = None;
    }

    /// The acknowledged position, if it has not been committed yet.
    fn pending_commit(&self) -> Option<&Position> {
        self.acknowledged
            .as_ref()
            .filter(|acknowledged| self.committed.as_ref() != Some(*acknowledged))
    }
}

impl PartitionReader {
    pub async fn run(mut self) -> Result<(), ()> {
        let bytes_received = register!(BytesReceived::from(Protocol::from("amqp")));
        let events_received = register!(EventsReceived);
        let mut positions = match (self.initial_position.take(), &self.checkpointer) {
            (None, Some(checkpointer)) if self.initial_checkpoint => {
                match self.initialize_checkpoint(checkpointer).await {
                    Some(positions) => positions,
                    // Shut down before the partition was opened: nothing to drain or commit.
                    None => return Ok(()),
                }
            }
            (initial, _) => PositionState::new(initial),
        };
        let (mut finalizer, mut acks) =
            OrderedFinalizer::<Position>::maybe_new(self.acknowledgements, None);
        let mut checkpoint_tick = tokio::time::interval(self.checkpoint_interval);
        checkpoint_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut backoff = BACKOFF_INITIAL;
        let mut start = positions.reconnect_start(&self.default_start);
        let mut shutdown = self.shutdown.clone();

        let result = loop {
            let options = OpenReceiverOptions {
                start_position: Some(start.clone()),
                prefetch: self.prefetch,
                owner_level: self.owner_level,
                ..Default::default()
            };
            let opened = tokio::select! {
                biased;
                _ = &mut shutdown => break Ok(()),
                opened = self
                    .client
                    .open_receiver_on_partition(self.partition_id.clone(), Some(options)) => opened,
            };
            let receiver = match opened {
                Ok(receiver) => receiver,
                Err(error) => {
                    emit!(AzureEventHubsConnectError {
                        partition_id: &self.partition_id,
                        error,
                    });
                    if !sleep_or_shutdown(&mut backoff, &mut shutdown).await {
                        break Ok(());
                    }
                    continue;
                }
            };
            debug!(
                message = "Opened Event Hubs partition receiver.",
                partition_id = %self.partition_id,
                start = ?start,
            );

            let exit = {
                let stream = receiver.stream_events();
                tokio::pin!(stream);
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => break LoopExit::Shutdown,
                        Some((status, position)) = acks.next() => match status {
                            BatchStatus::Delivered | BatchStatus::Rejected => {
                                positions.on_acknowledged(position);
                            }
                            BatchStatus::Errored => break LoopExit::Replay,
                        },
                        _ = checkpoint_tick.tick() => self.commit(&mut positions).await,
                        next = stream.next() => match next {
                            Some(Ok(event)) => {
                                backoff = BACKOFF_INITIAL;
                                match self
                                    .handle_event(
                                        event,
                                        finalizer.as_ref(),
                                        &bytes_received,
                                        &events_received,
                                    )
                                    .await
                                {
                                    Ok(Some(position)) => {
                                        positions.on_received(position, finalizer.is_none())
                                    }
                                    Ok(None) => {}
                                    Err(()) => break LoopExit::Closed,
                                }
                            }
                            Some(Err(error)) => {
                                emit!(AzureEventHubsReceiveError {
                                    partition_id: &self.partition_id,
                                    error,
                                });
                                break LoopExit::Reconnect;
                            }
                            None => {
                                debug!(
                                    message = "Event Hubs partition stream ended.",
                                    partition_id = %self.partition_id,
                                );
                                break LoopExit::Reconnect;
                            }
                        },
                    }
                }
            };
            if tokio::time::timeout(CLOSE_TIMEOUT, receiver.close())
                .await
                .is_err()
            {
                debug!(
                    message = "Timed out closing the Event Hubs receiver.",
                    partition_id = %self.partition_id,
                );
            }

            match exit {
                LoopExit::Shutdown => break Ok(()),
                LoopExit::Closed => break Err(()),
                LoopExit::Reconnect => {
                    if !sleep_or_shutdown(&mut backoff, &mut shutdown).await {
                        break Ok(());
                    }
                    start = positions.reconnect_start(&self.default_start);
                }
                LoopExit::Replay => {
                    emit!(AzureEventHubsDeliveryFailed {
                        partition_id: &self.partition_id,
                    });
                    // Drop the acknowledgements still pending: everything after the failed event
                    // is read again.
                    (finalizer, acks) =
                        OrderedFinalizer::<Position>::maybe_new(self.acknowledgements, None);
                    positions.reset_to_acknowledged();
                    self.commit(&mut positions).await;
                    start = positions.replay_start(&self.default_start);
                }
            }
        };

        // Wait for the events already sent downstream, then write the final checkpoint.
        if let Some(finalizer) = finalizer.take() {
            drop(finalizer);
            let drain = async {
                while let Some((status, position)) = acks.next().await {
                    match status {
                        BatchStatus::Delivered | BatchStatus::Rejected => {
                            positions.on_acknowledged(position)
                        }
                        BatchStatus::Errored => break,
                    }
                }
            };
            if tokio::time::timeout(SHUTDOWN_ACK_TIMEOUT, drain)
                .await
                .is_err()
            {
                warn!(
                    message = "Timed out waiting for acknowledgements at shutdown; unacknowledged events will be read again.",
                    partition_id = %self.partition_id,
                );
            }
        }
        self.commit(&mut positions).await;
        result
    }

    /// Resolves the position `latest` means right now (the partition's last enqueued event) and
    /// writes it as the partition's first checkpoint, create-only. Reading then starts right
    /// after that position rather than at `latest`, so nothing enqueued between the two is
    /// missed, and a restart before the first event resumes after it too.
    ///
    /// Reading the partition properties and writing the checkpoint are each retried until they
    /// succeed, as opening a receiver is; `latest` is resolved once, so a retried write never
    /// moves it forward.
    ///
    /// Returns `None` on shutdown.
    async fn initialize_checkpoint(&self, checkpointer: &Checkpointer) -> Option<PositionState> {
        let mut shutdown = self.shutdown.clone();
        let mut backoff = BACKOFF_INITIAL;
        let latest = loop {
            let properties = tokio::select! {
                biased;
                _ = &mut shutdown => return None,
                properties = self.client.get_partition_properties(&self.partition_id) => properties,
            };
            match properties {
                Ok(properties) => break Position::last_enqueued(&properties),
                Err(error) => {
                    emit!(AzureEventHubsPartitionPropertiesError {
                        partition_id: &self.partition_id,
                        error,
                    });
                    if !sleep_or_shutdown(&mut backoff, &mut shutdown).await {
                        return None;
                    }
                }
            }
        };
        // The write is retried with the same `latest` until it succeeds (a refused or missing
        // checkpoint store, say), and the partition is not read meanwhile: reading first and
        // committing later would let a restart before that commit resolve `latest` again and
        // skip what was enqueued in between. The events wait in the partition.
        backoff = BACKOFF_INITIAL;
        loop {
            let result = tokio::select! {
                biased;
                _ = &mut shutdown => return None,
                result = checkpointer.initialize(&self.partition_id, latest.clone()) => result,
            };
            match result {
                Ok(outcome) => {
                    emit!(AzureEventHubsInitialCheckpoint {
                        partition_id: &self.partition_id,
                        outcome: &outcome,
                    });
                    return Some(PositionState::initialized(&outcome));
                }
                Err(error) => {
                    emit!(AzureEventHubsCheckpointError {
                        partition_id: &self.partition_id,
                        error,
                    });
                    if !sleep_or_shutdown(&mut backoff, &mut shutdown).await {
                        return None;
                    }
                }
            }
        }
    }

    /// Writes the acknowledged position, if it changed. Failures are retried on the next tick.
    async fn commit(&self, positions: &mut PositionState) {
        let (Some(checkpointer), Some(position)) = (&self.checkpointer, positions.pending_commit())
        else {
            return;
        };
        let position = position.clone();
        match checkpointer.commit(&self.partition_id, &position).await {
            Ok(()) => {
                emit!(AzureEventHubsCheckpointCommitted {
                    partition_id: &self.partition_id,
                    sequence_number: position.sequence_number,
                });
                positions.committed = Some(position);
            }
            Err(error) => emit!(AzureEventHubsCheckpointError {
                partition_id: &self.partition_id,
                error,
            }),
        }
    }

    /// Decodes one Event Hubs event and sends the result downstream.
    ///
    /// Returns the event's position (if it has one), or `Err` when the pipeline is closed.
    async fn handle_event(
        &mut self,
        event: ReceivedEventData,
        finalizer: Option<&OrderedFinalizer<Position>>,
        bytes_received: &Registered<BytesReceived>,
        events_received: &Registered<EventsReceived>,
    ) -> Result<Option<Position>, ()> {
        let position = Position::new(event.offset().clone(), event.sequence_number());
        let metadata = EventMetadata::from_event(&event, &self.partition_id, &self.event_hub_name);

        let body = match body_bytes(&event.raw_amqp_message().body) {
            Ok(body) => body,
            Err(kind) => {
                emit!(AzureEventHubsUnsupportedBody {
                    partition_id: &self.partition_id,
                    kind,
                });
                // The event cannot be decoded; it is skipped like any undecodable payload.
                None
            }
        };

        let mut events = Vec::new();
        if let Some(body) = body {
            bytes_received.emit(ByteSize(body.len()));
            let mut stream = DecoderFramedRead::new(body.as_ref(), self.decoder.clone());
            while let Some(next) = stream.next().await {
                match next {
                    Ok((decoded, _byte_size)) => events.extend(decoded),
                    Err(error) => {
                        // Decoding errors are emitted by the decoder; stop at fatal ones.
                        if !error.can_continue() {
                            break;
                        }
                    }
                }
            }
        }

        if !events.is_empty() {
            events_received.emit(CountByteSize(
                events.len(),
                events.estimated_json_encoded_size_of(),
            ));
            let now = Utc::now();
            for event in &mut events {
                if let Event::Log(log) = event {
                    metadata.insert(self.log_namespace, log, now);
                }
            }
        }

        // Every event, decodable or not, gets an entry in the finalizer: the ordered finalizer only
        // releases a position once all earlier ones are acknowledged.
        let receiver = finalizer.map(|_| {
            let (batch, receiver) = BatchNotifier::new_with_receiver();
            for event in &mut events {
                event.add_batch_notifier(batch.clone());
            }
            receiver
        });

        if !events.is_empty() {
            let count = events.len();
            if self.out.send_batch(events).await.is_err() {
                emit!(StreamClosedError { count });
                return Err(());
            }
        }

        if let (Some(finalizer), Some(receiver), Some(position)) =
            (finalizer, receiver, position.clone())
        {
            finalizer.add(position, receiver);
        }
        Ok(position)
    }
}

/// Sleeps for the current backoff, doubling it, unless shutdown comes first. Returns `false` on
/// shutdown.
pub(super) async fn sleep_or_shutdown(
    backoff: &mut Duration,
    shutdown: &mut ShutdownSignal,
) -> bool {
    let delay = *backoff;
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
    tokio::select! {
        biased;
        _ = shutdown => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Extracts the payload of an AMQP message body.
///
/// Event Hubs producers send a single `data` section; multiple sections are concatenated, and
/// string or binary `amqp-value` bodies are accepted too. Returns `Ok(None)` for an empty body.
fn body_bytes(body: &AmqpMessageBody) -> Result<Option<Bytes>, &'static str> {
    match body {
        AmqpMessageBody::Binary(sections) => Ok(Some(match sections.as_slice() {
            [single] => Bytes::copy_from_slice(single),
            sections => Bytes::from(sections.concat()),
        })),
        AmqpMessageBody::Value(AmqpValue::String(text)) => {
            Ok(Some(Bytes::copy_from_slice(text.as_bytes())))
        }
        AmqpMessageBody::Value(AmqpValue::Binary(data)) => Ok(Some(Bytes::copy_from_slice(data))),
        AmqpMessageBody::Empty => Ok(None),
        AmqpMessageBody::Value(_) => Err("amqp-value"),
        AmqpMessageBody::Sequence(_) => Err("amqp-sequence"),
    }
}

/// The source metadata of one Event Hubs event.
struct EventMetadata {
    event_hub_name: String,
    partition_id: String,
    offset: Option<String>,
    sequence_number: Option<i64>,
    enqueued_time: Option<DateTime<Utc>>,
    partition_key: Option<String>,
    properties: Option<ObjectMap>,
}

impl EventMetadata {
    fn from_event(event: &ReceivedEventData, partition_id: &str, event_hub_name: &str) -> Self {
        let properties = event
            .raw_amqp_message()
            .application_properties
            .as_ref()
            .filter(|properties| !properties.0.is_empty())
            .map(|properties| {
                properties
                    .0
                    .iter()
                    .map(|(key, value)| (KeyString::from(key.as_str()), simple_value(value)))
                    .collect()
            });
        Self {
            event_hub_name: event_hub_name.to_string(),
            partition_id: partition_id.to_string(),
            offset: event.offset().clone(),
            sequence_number: event.sequence_number(),
            enqueued_time: event.enqueued_time().map(DateTime::<Utc>::from),
            partition_key: event.partition_key().clone(),
            properties,
        }
    }

    fn insert(
        &self,
        log_namespace: LogNamespace,
        log: &mut vector_lib::event::LogEvent,
        now: DateTime<Utc>,
    ) {
        let name = AzureEventHubsSourceConfig::NAME;
        log_namespace.insert_standard_vector_source_metadata(log, name, now);
        log_namespace.insert_source_metadata(
            name,
            log,
            Some(LegacyKey::InsertIfEmpty(path!("event_hub_name"))),
            path!("event_hub_name"),
            self.event_hub_name.clone(),
        );
        log_namespace.insert_source_metadata(
            name,
            log,
            Some(LegacyKey::InsertIfEmpty(path!("partition_id"))),
            path!("partition_id"),
            self.partition_id.clone(),
        );
        if let Some(offset) = &self.offset {
            log_namespace.insert_source_metadata(
                name,
                log,
                Some(LegacyKey::InsertIfEmpty(path!("offset"))),
                path!("offset"),
                offset.clone(),
            );
        }
        if let Some(sequence_number) = self.sequence_number {
            log_namespace.insert_source_metadata(
                name,
                log,
                Some(LegacyKey::InsertIfEmpty(path!("sequence_number"))),
                path!("sequence_number"),
                sequence_number,
            );
        }
        if let Some(enqueued_time) = self.enqueued_time {
            log_namespace.insert_source_metadata(
                name,
                log,
                Some(LegacyKey::InsertIfEmpty(path!("enqueued_time"))),
                path!("enqueued_time"),
                enqueued_time,
            );
        }
        if let Some(partition_key) = &self.partition_key {
            log_namespace.insert_source_metadata(
                name,
                log,
                Some(LegacyKey::InsertIfEmpty(path!("partition_key"))),
                path!("partition_key"),
                partition_key.clone(),
            );
        }
        if let Some(properties) = &self.properties {
            log_namespace.insert_source_metadata(
                name,
                log,
                Some(LegacyKey::InsertIfEmpty(path!("properties"))),
                path!("properties"),
                Value::Object(properties.clone()),
            );
        }
    }
}

/// Converts an AMQP application property value.
fn simple_value(value: &AmqpSimpleValue) -> Value {
    match value {
        AmqpSimpleValue::Null => Value::Null,
        AmqpSimpleValue::Boolean(b) => Value::Boolean(*b),
        AmqpSimpleValue::UByte(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::Byte(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::UShort(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::Short(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::UInt(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::Int(n) => Value::Integer(i64::from(*n)),
        AmqpSimpleValue::ULong(n) => {
            i64::try_from(*n).map_or_else(|_| Value::from(n.to_string()), Value::Integer)
        }
        AmqpSimpleValue::Long(n) => Value::Integer(*n),
        AmqpSimpleValue::Float(f) => Value::from_f64_or_zero(f64::from(*f)),
        AmqpSimpleValue::Double(f) => Value::from_f64_or_zero(*f),
        AmqpSimpleValue::Char(c) => Value::from(c.to_string()),
        AmqpSimpleValue::TimeStamp(timestamp) => timestamp
            .0
            .map_or(Value::Null, |t| Value::Timestamp(DateTime::<Utc>::from(t))),
        AmqpSimpleValue::Uuid(uuid) => Value::from(uuid.to_string()),
        AmqpSimpleValue::String(s) => Value::from(s.as_str()),
        AmqpSimpleValue::Symbol(symbol) => Value::from(symbol.0.as_str()),
        AmqpSimpleValue::Binary(data) => Value::Bytes(Bytes::copy_from_slice(data)),
        // Decimals and described values have no lossless VRL representation.
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(seq: i64) -> Position {
        Position::new(Some(format!("{}", seq * 100)), Some(seq)).unwrap()
    }

    fn describe(start: &StartPosition) -> String {
        format!("{:?}/{}", start.location, start.inclusive)
    }

    #[test]
    fn positions_without_checkpoint() {
        let default = StartPosition::default();
        let mut state = PositionState::new(None);
        assert_eq!(
            describe(&state.reconnect_start(&default)),
            describe(&default)
        );
        assert!(state.pending_commit().is_none());

        state.on_received(pos(1), false);
        state.on_received(pos(2), false);
        // Reconnects continue after the events in flight.
        assert_eq!(
            describe(&state.reconnect_start(&default)),
            describe(&pos(2).after())
        );
        // Replays before any acknowledgement start at the first received event, inclusive.
        state.reset_to_acknowledged();
        assert_eq!(
            describe(&state.replay_start(&default)),
            describe(&pos(1).at())
        );

        state.on_acknowledged(pos(1));
        assert_eq!(state.pending_commit(), Some(&pos(1)));
        assert_eq!(
            describe(&state.replay_start(&default)),
            describe(&pos(1).after())
        );
    }

    #[test]
    fn positions_with_checkpoint() {
        let default = StartPosition::default();
        let mut state = PositionState::new(Some(pos(5)));
        assert_eq!(
            describe(&state.reconnect_start(&default)),
            describe(&pos(5).after())
        );
        assert!(state.pending_commit().is_none());
        state.on_received(pos(6), true);
        assert_eq!(state.pending_commit(), Some(&pos(6)));
        state.committed = Some(pos(6));
        assert!(state.pending_commit().is_none());
    }

    #[test]
    fn positions_after_initial_checkpoint() {
        let latest = StartPosition::default();
        let state = PositionState::initialized(&InitialCheckpoint::Created(pos(9)));
        assert_eq!(
            describe(&state.reconnect_start(&latest)),
            describe(&pos(9).after())
        );
        assert_eq!(
            describe(&state.replay_start(&latest)),
            describe(&pos(9).after())
        );
        // The store already holds it.
        assert!(state.pending_commit().is_none());

        let state = PositionState::initialized(&InitialCheckpoint::Existing(pos(5)));
        assert_eq!(
            describe(&state.reconnect_start(&latest)),
            describe(&pos(5).after())
        );
        assert!(state.pending_commit().is_none());
    }

    /// One partition, filtered the way the service applies a receiver's start position. Event
    /// `n` has sequence number `n` and offset `n * 100`, like [`pos`].
    #[derive(Default)]
    struct FakePartition {
        len: i64,
    }

    impl FakePartition {
        fn enqueue(&mut self, count: i64) {
            self.len += count;
        }

        fn properties(&self) -> azure_messaging_eventhubs::models::EventHubPartitionProperties {
            let last = self.len - 1;
            let offset = if last < 0 {
                "-1".to_string()
            } else {
                (last * 100).to_string()
            };
            super::super::checkpoint::tests::partition_properties(last, &offset)
        }

        /// The sequence number of the first event a receiver opened now at `start` reads.
        fn open(&self, start: &StartPosition) -> i64 {
            use azure_messaging_eventhubs::StartLocation;
            let skip = i64::from(!start.inclusive);
            match &start.location {
                StartLocation::Earliest => 0,
                StartLocation::Latest => self.len,
                StartLocation::SequenceNumber(n) => n + skip,
                StartLocation::Offset(offset) => {
                    let offset: i64 = offset.parse().unwrap();
                    if offset < 0 { 0 } else { offset / 100 + skip }
                }
                StartLocation::EnqueuedTime(_) => unimplemented!(),
            }
        }

        /// The events a receiver opened at sequence number `first` has read by now.
        fn read(&self, first: i64) -> Vec<i64> {
            (first..self.len).collect()
        }
    }

    /// The partition is idle when the source starts, the source stops before any event arrives,
    /// events are enqueued while it is down, and it starts again: every one of them is read.
    #[tokio::test]
    async fn restart_while_idle_reads_everything_enqueued_while_down() {
        use super::super::checkpoint::MemoryCheckpointStore;

        let latest = StartPosition::default();
        let store = Arc::new(MemoryCheckpointStore::default());
        let checkpointer = Checkpointer::new(
            Arc::<MemoryCheckpointStore>::clone(&store),
            "ns",
            "hub",
            "$Default",
        );
        let mut partition = FakePartition::default();
        partition.enqueue(10); // History from before the source existed: never read.

        // First start: no checkpoint, so the initial one is written at the last enqueued event.
        assert!(checkpointer.load().await.unwrap().is_empty());
        let outcome = checkpointer
            .initialize("0", Position::last_enqueued(&partition.properties()))
            .await
            .unwrap();
        assert_eq!(outcome, InitialCheckpoint::Created(pos(9)));
        let state = PositionState::initialized(&outcome);
        let first = partition.open(&state.reconnect_start(&latest));
        assert_eq!(partition.read(first), Vec::<i64>::new());
        // Stopped while idle; nothing else was committed.

        partition.enqueue(5); // Enqueued while the source is down.

        // Restart: the initial checkpoint is loaded like any other.
        let loaded = checkpointer.load().await.unwrap();
        assert_eq!(loaded.get("0"), Some(&pos(9)));
        let state = PositionState::new(loaded.get("0").cloned());
        let first = partition.open(&state.reconnect_start(&latest));
        assert_eq!(partition.read(first), vec![10, 11, 12, 13, 14]);

        // Without the initial checkpoint the restart starts at `latest` and skips all five.
        let first = partition.open(&PositionState::new(None).reconnect_start(&latest));
        assert_eq!(partition.read(first), Vec::<i64>::new());

        // A third start finds the checkpoint and never re-initializes or overwrites it.
        let again = checkpointer
            .initialize("0", Position::last_enqueued(&partition.properties()))
            .await
            .unwrap();
        assert_eq!(again, InitialCheckpoint::Existing(pos(9)));
    }

    /// A partition that never had an event: the initial checkpoint is "before the first event",
    /// so the first events, even those enqueued while the source was down, are read.
    #[tokio::test]
    async fn restart_of_a_new_empty_partition_reads_its_first_events() {
        use super::super::checkpoint::MemoryCheckpointStore;

        let latest = StartPosition::default();
        let store = Arc::new(MemoryCheckpointStore::default());
        let checkpointer = Checkpointer::new(store, "ns", "hub", "$Default");
        let mut partition = FakePartition::default();

        let outcome = checkpointer
            .initialize("0", Position::last_enqueued(&partition.properties()))
            .await
            .unwrap();
        assert!(matches!(outcome, InitialCheckpoint::Created(_)));
        partition.enqueue(3);

        let loaded = checkpointer.load().await.unwrap();
        let state = PositionState::new(loaded.get("0").cloned());
        let first = partition.open(&state.reconnect_start(&latest));
        assert_eq!(partition.read(first), vec![0, 1, 2]);
    }

    #[test]
    fn body_variants() {
        assert_eq!(
            body_bytes(&AmqpMessageBody::Binary(vec![
                b"ab".to_vec(),
                b"cd".to_vec()
            ])),
            Ok(Some(Bytes::from_static(b"abcd")))
        );
        assert_eq!(
            body_bytes(&AmqpMessageBody::Value(AmqpValue::String("hi".into()))),
            Ok(Some(Bytes::from_static(b"hi")))
        );
        assert_eq!(body_bytes(&AmqpMessageBody::Empty), Ok(None));
        assert_eq!(
            body_bytes(&AmqpMessageBody::Value(AmqpValue::Long(1))),
            Err("amqp-value")
        );
    }

    #[test]
    fn property_values() {
        assert_eq!(simple_value(&AmqpSimpleValue::Int(-3)), Value::Integer(-3));
        assert_eq!(
            simple_value(&AmqpSimpleValue::ULong(u64::MAX)),
            Value::from(u64::MAX.to_string())
        );
        assert_eq!(
            simple_value(&AmqpSimpleValue::String("x".into())),
            Value::from("x")
        );
        assert_eq!(
            simple_value(&AmqpSimpleValue::Boolean(true)),
            Value::Boolean(true)
        );
    }
}
