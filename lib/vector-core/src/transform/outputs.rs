use std::{collections::HashMap, error, sync::Arc, time::Instant};

use vector_common::{
    EventDataEq,
    byte_size_of::ByteSizeOf,
    internal_event::{
        self, CountByteSize, DEFAULT_OUTPUT, EventsSent, InternalEventHandle as _, Registered,
        register,
    },
    json_size::JsonSize,
};

use crate::{
    config,
    config::{ComponentKey, OutputId},
    event::{EstimatedJsonEncodedSizeOf, Event, EventArray, EventContainer, EventMutRef, EventRef},
    fanout::{self, Fanout},
    schema,
};

struct TransformOutput {
    /// `None` for the default output.
    port: Option<String>,
    fanout: Fanout,
    events_sent: Registered<EventsSent>,
    log_schema_definitions: HashMap<OutputId, Arc<schema::Definition>>,
    output_id: Arc<OutputId>,
}

pub struct TransformOutputs {
    outputs_spec: Vec<config::TransformOutput>,
    /// Every output, the default one (if any) first. One `Vec` so a send can borrow the port it
    /// sends to and its siblings at once without allocating (NAN-5874).
    outputs: Vec<TransformOutput>,
}

impl TransformOutputs {
    pub fn new(
        outputs_in: Vec<config::TransformOutput>,
        component_key: &ComponentKey,
    ) -> (Self, HashMap<Option<String>, fanout::ControlChannel>) {
        let outputs_spec = outputs_in.clone();
        let mut outputs = Vec::with_capacity(outputs_in.len());
        let mut controls = HashMap::new();

        for output in outputs_in {
            let (fanout, control) = Fanout::new(component_key.clone());

            let log_schema_definitions = output
                .log_schema_definitions
                .into_iter()
                .map(|(id, definition)| (id, Arc::new(definition)))
                .collect();

            let events_sent = match &output.port {
                None => register(EventsSent::from(internal_event::Output(Some(
                    DEFAULT_OUTPUT.into(),
                )))),
                Some(name) => register(EventsSent::from(internal_event::Output(Some(
                    name.clone().into(),
                )))),
            };
            let transform_output = TransformOutput {
                port: output.port.clone(),
                fanout,
                events_sent,
                log_schema_definitions,
                output_id: Arc::new(OutputId {
                    component: component_key.clone(),
                    port: output.port.clone(),
                }),
            };
            if output.port.is_none() {
                outputs.insert(0, transform_output);
            } else {
                outputs.push(transform_output);
            }
            controls.insert(output.port, control);
        }

        let me = Self {
            outputs_spec,
            outputs,
        };

        (me, controls)
    }

    pub fn new_buf_with_capacity(&self, capacity: usize) -> TransformOutputsBuf {
        TransformOutputsBuf::new_with_capacity(self.outputs_spec.clone(), capacity)
    }

    /// Sends the events in the buffer to their respective outputs.
    ///
    /// # Panics
    ///
    /// Panics if `buf` was not created by this `TransformOutputs` (mismatched outputs).
    ///
    /// # Errors
    ///
    /// If an error occurs while sending events to their respective output, an error variant will be
    /// returned detailing the cause.
    pub async fn send(
        &mut self,
        buf: &mut TransformOutputsBuf,
    ) -> Result<(), Box<dyn error::Error + Send + Sync>> {
        // NAN-5874: the ports are sent one after the other, and a send to a port with a paused
        // sink waits until the reload replaces or removes it. Meanwhile every other port keeps
        // applying its control messages: a reload that pauses one of THEIR sinks waits
        // (`wait_for_sinks`) for the sink's sender to be dropped, and it must not wait on this
        // send, which only that reload's completion unblocks. No allocation: the siblings are
        // the two halves of `outputs` around the port being sent.
        for i in 0..self.outputs.len() {
            let (before, rest) = self.outputs.split_at_mut(i);
            let (current, after) = rest.split_first_mut().expect("index in range");
            let out_buf = match &current.port {
                None => buf.primary_buffer.as_mut().expect("mismatched outputs"),
                Some(name) => buf.named_buffers.get_mut(name).expect("unknown output"),
            };
            if out_buf.is_empty() || (before.is_empty() && after.is_empty()) {
                // Nothing to wait for on this port, or no sibling to serve.
                Self::send_single_buffer(out_buf, current).await?;
                continue;
            }
            let send = Self::send_single_buffer(out_buf, current);
            tokio::pin!(send);
            loop {
                tokio::select! {
                    biased;
                    result = &mut send => {
                        result?;
                        break;
                    }
                    () = serve_any(before, after) => {}
                }
            }
        }
        Ok(())
    }

    /// Waits for a control message on any port and applies it (for an idle transform; see
    /// `Fanout::serve_control_message`). Never resolves once every port's channel is closed.
    /// Allocation-free, so a runner can poll it on every iteration.
    pub async fn serve_control_messages(&mut self) {
        serve_any(&mut self.outputs, &mut []).await;
    }

    async fn send_single_buffer(
        buf: &mut OutputBuffer,
        output: &mut TransformOutput,
    ) -> Result<(), Box<dyn error::Error + Send + Sync>> {
        for event in buf.events_mut() {
            super::update_runtime_schema_definition(
                event,
                &output.output_id,
                &output.log_schema_definitions,
            );
        }
        let count = buf.len();
        let byte_size = buf.estimated_json_encoded_size_of();
        buf.send(&mut output.fanout).await?;
        output.events_sent.emit(CountByteSize(count, byte_size));
        Ok(())
    }
}

/// Applies the first control message any output in `a` or `b` receives. Pending forever when
/// there are none, or once every channel is closed. Allocation-free.
async fn serve_any(a: &mut [TransformOutput], b: &mut [TransformOutput]) {
    std::future::poll_fn(|cx| {
        for output in a.iter_mut().chain(b.iter_mut()) {
            if output.fanout.poll_serve_control_message(cx).is_ready() {
                return std::task::Poll::Ready(());
            }
        }
        std::task::Poll::Pending
    })
    .await;
}

#[derive(Debug, Clone)]
pub struct TransformOutputsBuf {
    pub(super) primary_buffer: Option<OutputBuffer>,
    pub(super) named_buffers: HashMap<String, OutputBuffer>,
}

impl TransformOutputsBuf {
    pub fn new_with_capacity(outputs_in: Vec<config::TransformOutput>, capacity: usize) -> Self {
        let mut primary_buffer = None;
        let mut named_buffers = HashMap::new();

        for output in outputs_in {
            match output.port {
                None => {
                    primary_buffer = Some(OutputBuffer::with_capacity(capacity));
                }
                Some(name) => {
                    named_buffers.insert(name.clone(), OutputBuffer::default());
                }
            }
        }

        Self {
            primary_buffer,
            named_buffers,
        }
    }

    /// Adds a new event to the named output buffer.
    ///
    /// # Panics
    ///
    /// Panics if there is no output with the given name.
    pub fn push(&mut self, name: Option<&str>, event: Event) {
        match name {
            Some(name) => self.named_buffers.get_mut(name),
            None => self.primary_buffer.as_mut(),
        }
        .expect("unknown output")
        .push(event);
    }

    /// Drains the default output buffer.
    ///
    /// # Panics
    ///
    /// Panics if there is no default output.
    pub fn drain(&mut self) -> impl Iterator<Item = Event> + '_ {
        self.primary_buffer
            .as_mut()
            .expect("no default output")
            .drain()
    }

    /// Drains the named output buffer.
    ///
    /// # Panics
    ///
    /// Panics if there is no output with the given name.
    pub fn drain_named(&mut self, name: &str) -> impl Iterator<Item = Event> + '_ {
        self.named_buffers
            .get_mut(name)
            .expect("unknown output")
            .drain()
    }

    /// Takes the default output buffer.
    ///
    /// # Panics
    ///
    /// Panics if there is no default output.
    pub fn take_primary(&mut self) -> OutputBuffer {
        std::mem::take(self.primary_buffer.as_mut().expect("no default output"))
    }

    pub fn take_all_named(&mut self) -> HashMap<String, OutputBuffer> {
        std::mem::take(&mut self.named_buffers)
    }

    /// Applies `f` to each [`EventArray`] currently buffered in this outputs buffer.
    ///
    /// This is useful for cross-cutting instrumentation (e.g. latency timestamp propagation)
    /// that needs mutable access to the buffered arrays before they are sent.
    pub fn for_each_array_mut(&mut self, mut f: impl FnMut(&mut EventArray)) {
        if let Some(primary) = self.primary_buffer.as_mut() {
            primary.for_each_array_mut(&mut f);
        }
        for buf in self.named_buffers.values_mut() {
            buf.for_each_array_mut(&mut f);
        }
    }
}

impl ByteSizeOf for TransformOutputsBuf {
    fn allocated_bytes(&self) -> usize {
        self.primary_buffer.size_of()
            + self
                .named_buffers
                .values()
                .map(ByteSizeOf::size_of)
                .sum::<usize>()
    }
}

#[derive(Debug, Default, Clone)]
pub struct OutputBuffer(pub(super) Vec<EventArray>);

impl OutputBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, event: Event) {
        // Coalesce multiple pushes of the same type into one array.
        match (event, self.0.last_mut()) {
            (Event::Log(log), Some(EventArray::Logs(logs))) => {
                logs.push(log);
            }
            (Event::Metric(metric), Some(EventArray::Metrics(metrics))) => {
                metrics.push(metric);
            }
            (Event::Trace(trace), Some(EventArray::Traces(traces))) => {
                traces.push(trace);
            }
            (event, _) => {
                self.0.push(event.into());
            }
        }
    }

    pub fn append(&mut self, events: &mut Vec<Event>) {
        for event in events.drain(..) {
            self.push(event);
        }
    }

    pub fn extend(&mut self, events: impl Iterator<Item = Event>) {
        for event in events {
            self.push(event);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(EventArray::len).sum()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn first(&self) -> Option<EventRef<'_>> {
        self.0.first().and_then(|first| match first {
            EventArray::Logs(l) => l.first().map(Into::into),
            EventArray::Metrics(m) => m.first().map(Into::into),
            EventArray::Traces(t) => t.first().map(Into::into),
        })
    }

    pub fn drain(&mut self) -> impl Iterator<Item = Event> + '_ {
        self.0.drain(..).flat_map(EventArray::into_events)
    }

    /// Applies `f` to each [`EventArray`] currently held by this buffer.
    pub fn for_each_array_mut(&mut self, mut f: impl FnMut(&mut EventArray)) {
        for array in &mut self.0 {
            f(array);
        }
    }

    async fn send(
        &mut self,
        output: &mut Fanout,
    ) -> Result<(), Box<dyn error::Error + Send + Sync>> {
        let send_start = Some(Instant::now());
        for array in std::mem::take(&mut self.0) {
            output.send(array, send_start).await?;
        }

        Ok(())
    }

    fn iter_events(&self) -> impl Iterator<Item = EventRef<'_>> {
        self.0.iter().flat_map(EventArray::iter_events)
    }

    fn events_mut(&mut self) -> impl Iterator<Item = EventMutRef<'_>> {
        self.0.iter_mut().flat_map(EventArray::iter_events_mut)
    }

    pub fn into_events(self) -> impl Iterator<Item = Event> {
        self.0.into_iter().flat_map(EventArray::into_events)
    }
}

impl ByteSizeOf for OutputBuffer {
    fn allocated_bytes(&self) -> usize {
        self.0.iter().map(ByteSizeOf::size_of).sum()
    }
}

impl EventDataEq<Vec<Event>> for OutputBuffer {
    fn event_data_eq(&self, other: &Vec<Event>) -> bool {
        struct Comparator<'a>(EventRef<'a>);

        impl PartialEq<&Event> for Comparator<'_> {
            fn eq(&self, that: &&Event) -> bool {
                self.0.event_data_eq(that)
            }
        }

        self.iter_events().map(Comparator).eq(other.iter())
    }
}

impl EstimatedJsonEncodedSizeOf for OutputBuffer {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.0
            .iter()
            .map(EstimatedJsonEncodedSizeOf::estimated_json_encoded_size_of)
            .sum()
    }
}

impl From<Vec<Event>> for OutputBuffer {
    fn from(events: Vec<Event>) -> Self {
        let mut result = Self::default();
        result.extend(events.into_iter());
        result
    }
}

#[cfg(test)]
mod nan5874_tests {
    use std::{collections::HashMap, num::NonZeroUsize, time::Duration};

    use futures::StreamExt;
    use tracing::Span;
    use vector_buffers::{
        WhenFull,
        topology::{
            builder::TopologyBuilder,
            channel::{BufferReceiver, BufferSender},
        },
    };

    use super::TransformOutputs;
    use crate::{
        config::{self, ComponentKey, DataType},
        event::{Event, EventArray, LogEvent},
        fanout::ControlMessage,
    };

    fn pair() -> (BufferSender<EventArray>, BufferReceiver<EventArray>) {
        TopologyBuilder::standalone_memory(
            NonZeroUsize::new(4).unwrap(),
            WhenFull::Block,
            &Span::current(),
            None,
            None,
        )
    }

    // NAN-5874: a send blocked on one port's paused sink keeps applying the other ports' control
    // messages. A reload that pauses or removes a sink of another port waits for that sink's
    // sender to be dropped, and only the reload's completion unblocks the paused port.
    #[tokio::test]
    async fn a_paused_port_does_not_block_its_siblings_control_messages() {
        let specs = vec![
            config::TransformOutput::new(DataType::all_bits(), HashMap::new()).with_port("a"),
            config::TransformOutput::new(DataType::all_bits(), HashMap::new()).with_port("b"),
        ];
        let (mut outputs, controls) = TransformOutputs::new(specs, &ComponentKey::from("route"));
        let (a_tx, _a_rx) = pair();
        let (b_tx, b_rx) = pair();
        let control_a = &controls[&Some("a".to_string())];
        let control_b = &controls[&Some("b".to_string())];
        control_a
            .send(ControlMessage::Add(ComponentKey::from("sink_a"), a_tx))
            .unwrap();
        control_b
            .send(ControlMessage::Add(ComponentKey::from("sink_b"), b_tx))
            .unwrap();
        control_a
            .send(ControlMessage::Pause(ComponentKey::from("sink_a")))
            .unwrap();

        let mut buf = outputs.new_buf_with_capacity(1);
        buf.push(Some("a"), Event::Log(LogEvent::from("to a")));
        buf.push(Some("b"), Event::Log(LogEvent::from("to b")));
        let send = tokio::spawn(async move {
            // Port order is not fixed: whichever port sends first, port `a` blocks.
            let _sent = outputs.send(&mut buf).await;
            outputs
        });

        control_b
            .send(ControlMessage::Remove(ComponentKey::from("sink_b")))
            .unwrap();
        let mut b_stream = b_rx.into_stream();
        let drained = tokio::time::timeout(Duration::from_secs(2), async {
            while b_stream.next().await.is_some() {}
        })
        .await;
        assert!(
            drained.is_ok(),
            "port b applied Remove(sink_b) while port a waited"
        );
        assert!(
            !send.is_finished(),
            "port a is still waiting for its paused sink"
        );
        send.abort();
    }

    // NAN-5874: an idle transform applies its ports' control messages.
    #[tokio::test]
    async fn serve_control_messages_applies_any_ports_messages() {
        let specs = vec![
            config::TransformOutput::new(DataType::all_bits(), HashMap::new()),
            config::TransformOutput::new(DataType::all_bits(), HashMap::new()).with_port("b"),
        ];
        let (mut outputs, controls) = TransformOutputs::new(specs, &ComponentKey::from("t"));
        let (b_tx, b_rx) = pair();
        let control_b = &controls[&Some("b".to_string())];
        control_b
            .send(ControlMessage::Add(ComponentKey::from("sink_b"), b_tx))
            .unwrap();
        control_b
            .send(ControlMessage::Remove(ComponentKey::from("sink_b")))
            .unwrap();
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(1), outputs.serve_control_messages())
                .await
                .expect("a message was applied");
        }
        let next = tokio::time::timeout(Duration::from_secs(1), b_rx.into_stream().next())
            .await
            .expect("the removed sink's sender was dropped");
        assert!(next.is_none());
    }

    /// NAN-5874 review: per-batch cost of `TransformOutputs::send` on a 1-port transform and a
    /// 50-port route (events on 5 ports), sinks drained. Run with
    /// `cargo test -p vector-core --release --lib -- --ignored --nocapture bench_send`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "benchmark"]
    #[allow(clippy::print_stderr, reason = "a benchmark reports its timings")]
    async fn bench_send() {
        const ROUNDS: u32 = 20_000;
        for (ports, used) in [(1usize, 1usize), (50, 5)] {
            let specs: Vec<_> = (0..ports)
                .map(|i| {
                    let spec = config::TransformOutput::new(DataType::all_bits(), HashMap::new());
                    if ports == 1 {
                        spec
                    } else {
                        spec.with_port(format!("p{i}"))
                    }
                })
                .collect();
            let (mut outputs, controls) = TransformOutputs::new(specs, &ComponentKey::from("t"));
            for (name, control) in &controls {
                let (tx, rx) = TopologyBuilder::standalone_memory(
                    NonZeroUsize::new(1024).unwrap(),
                    WhenFull::Block,
                    &Span::current(),
                    None,
                    None,
                );
                control
                    .send(ControlMessage::Add(
                        ComponentKey::from(format!("sink_{name:?}")),
                        tx,
                    ))
                    .unwrap();
                tokio::spawn(async move {
                    let mut stream = rx.into_stream();
                    while stream.next().await.is_some() {}
                });
            }
            let mut buf = outputs.new_buf_with_capacity(1);
            let names: Vec<Option<String>> = (0..used)
                .map(|i| (ports > 1).then(|| format!("p{i}")))
                .collect();
            let event = Event::Log(LogEvent::from("x"));
            let started = std::time::Instant::now();
            for _ in 0..ROUNDS {
                for name in &names {
                    buf.push(name.as_deref(), event.clone());
                }
                outputs.send(&mut buf).await.unwrap();
            }
            let per = started.elapsed() / ROUNDS;
            eprintln!("bench_send: {ports} ports, {used} used: {per:?} per batch");
        }
    }
}
