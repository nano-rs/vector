//! Mock transforms that probe the topology reload state machine (NAN-5874).
//!
//! - `test_slow_process`: a `test_basic` transform that takes `per_event_ms` to process each event,
//!   so its input buffer holds a backlog when a reload starts.
//! - `test_flaky_build`: a `test_basic` transform whose build succeeds `ok_builds` times and fails
//!   afterwards, so a reload's rollback can be made to fail (`ReloadError::FailedToRestore`).

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use vector_lib::{
    config::{Input, TransformOutput},
    configurable::configurable_component,
    event::Event,
    schema,
    transform::{FunctionTransform, OutputBuffer, Transform},
};
use vrl::value::Value;

use super::BasicTransformConfig;
use crate::config::{OutputId, TransformConfig, TransformContext};

/// Configuration for the `test_slow_process` transform.
#[configurable_component(transform("test_slow_process", "Test (slow process)"))]
#[derive(Clone, Debug, Default)]
pub struct SlowProcessTransformConfig {
    /// Suffix to add to the message of any log event.
    suffix: String,

    /// How long processing each event takes, in milliseconds.
    per_event_ms: u64,
}

impl_generate_config_from_default!(SlowProcessTransformConfig);

impl SlowProcessTransformConfig {
    #[must_use]
    pub const fn new(suffix: String, per_event_ms: u64) -> Self {
        Self {
            suffix,
            per_event_ms,
        }
    }
}

#[async_trait]
#[typetag::serde(name = "test_slow_process")]
impl TransformConfig for SlowProcessTransformConfig {
    async fn build(&self, _globals: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::function(SlowProcessTransform {
            suffix: self.suffix.clone(),
            per_event: Duration::from_millis(self.per_event_ms),
        }))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        context: &TransformContext,
        definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        BasicTransformConfig::default().outputs(context, definitions)
    }
}

#[derive(Clone, Debug)]
struct SlowProcessTransform {
    suffix: String,
    per_event: Duration,
}

impl FunctionTransform for SlowProcessTransform {
    fn transform(&mut self, output: &mut OutputBuffer, mut event: Event) {
        // Blocking on purpose: it stands in for CPU-heavy work (a big VRL program).
        std::thread::sleep(self.per_event);
        if let Event::Log(log) = &mut event
            && let Some(key) = crate::config::log_schema().message_key_target_path()
        {
            let mut v = log.get(key).unwrap().to_string_lossy().into_owned();
            v.push_str(&self.suffix);
            log.insert(key, Value::from(v));
        }
        output.push(event);
    }
}

/// Configuration for the `test_flaky_build` transform.
#[configurable_component(transform("test_flaky_build", "Test (flaky build)"))]
#[derive(Clone, Debug, Default)]
pub struct FlakyBuildTransformConfig {
    /// Suffix to add to the message of any log event.
    suffix: String,

    /// Builds left that succeed; every build after that fails.
    #[serde(skip)]
    ok_builds: Arc<AtomicUsize>,
}

impl_generate_config_from_default!(FlakyBuildTransformConfig);

impl FlakyBuildTransformConfig {
    #[must_use]
    pub fn new(suffix: String, ok_builds: usize) -> Self {
        Self {
            suffix,
            ok_builds: Arc::new(AtomicUsize::new(ok_builds)),
        }
    }
}

#[async_trait]
#[typetag::serde(name = "test_flaky_build")]
impl TransformConfig for FlakyBuildTransformConfig {
    async fn build(&self, globals: &TransformContext) -> crate::Result<Transform> {
        let left = self
            .ok_builds
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if left.is_err() {
            return Err("test_flaky_build: no successful builds left".into());
        }
        BasicTransformConfig::new(self.suffix.clone(), 0.0)
            .build(globals)
            .await
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        context: &TransformContext,
        definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        BasicTransformConfig::default().outputs(context, definitions)
    }
}
