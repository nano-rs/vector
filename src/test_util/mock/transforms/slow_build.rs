use std::time::Duration;

use async_trait::async_trait;
use vector_lib::{
    config::{Input, TransformOutput},
    configurable::configurable_component,
    schema,
    transform::Transform,
};

use super::BasicTransformConfig;
use crate::config::{OutputId, TransformConfig, TransformContext};

/// Configuration for the `test_slow_build` transform: a `test_basic` transform whose build takes
/// `build_ms` milliseconds, standing in for a large remap that compiles for seconds (NAN-5874).
#[configurable_component(transform("test_slow_build", "Test (slow build)"))]
#[derive(Clone, Debug, Default)]
pub struct SlowBuildTransformConfig {
    /// Suffix to add to the message of any log event.
    suffix: String,

    /// How long the build takes, in milliseconds.
    build_ms: u64,
}

impl_generate_config_from_default!(SlowBuildTransformConfig);

impl SlowBuildTransformConfig {
    #[must_use]
    pub const fn new(suffix: String, build_ms: u64) -> Self {
        Self { suffix, build_ms }
    }
}

#[async_trait]
#[typetag::serde(name = "test_slow_build")]
impl TransformConfig for SlowBuildTransformConfig {
    async fn build(&self, globals: &TransformContext) -> crate::Result<Transform> {
        tokio::time::sleep(Duration::from_millis(self.build_ms)).await;
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
