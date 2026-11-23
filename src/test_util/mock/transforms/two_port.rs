use async_trait::async_trait;
use vector_lib::{
    config::{DataType, Input, TransformOutput},
    configurable::configurable_component,
    event::Event,
    schema,
    transform::{SyncTransform, Transform, TransformOutputsBuf},
};

use crate::config::{OutputId, TransformConfig, TransformContext};

/// Configuration for the `test_two_port` transform: a concurrent (`run_concurrently`) transform
/// with outputs `a` and `b` that sends every event to `a` only, so port `b` never sends
/// (NAN-5874: a reload waiting on a sink of `b` must not be starved by traffic on `a`).
#[configurable_component(transform("test_two_port", "Test (two ports)"))]
#[derive(Clone, Debug, Default)]
pub struct TwoPortTransformConfig {}

impl_generate_config_from_default!(TwoPortTransformConfig);

#[derive(Clone, Debug)]
struct TwoPort;

impl SyncTransform for TwoPort {
    fn transform(&mut self, event: Event, output: &mut TransformOutputsBuf) {
        output.push(Some("a"), event);
    }
}

#[async_trait]
#[typetag::serde(name = "test_two_port")]
impl TransformConfig for TwoPortTransformConfig {
    async fn build(&self, _globals: &TransformContext) -> crate::Result<Transform> {
        Ok(Transform::synchronous(TwoPort))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        let definitions = || {
            definitions
                .iter()
                .map(|(output, definition)| (output.clone(), definition.clone()))
                .collect()
        };
        vec![
            TransformOutput::new(DataType::all_bits(), definitions()).with_port("a"),
            TransformOutput::new(DataType::all_bits(), definitions()).with_port("b"),
        ]
    }

    fn enable_concurrency(&self) -> bool {
        true
    }
}
