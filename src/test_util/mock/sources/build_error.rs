use async_trait::async_trait;
use vector_lib::{
    config::{DataType, LogNamespace, SourceOutput},
    configurable::configurable_component,
    schema::Definition,
    source::Source,
};

use crate::config::{SourceConfig, SourceContext};

/// Configuration for the `test_build_error` source.
///
/// Its `build` always fails, the way a source with a credential its auth layer
/// refuses does. Used to drive a reload through the rollback path.
#[configurable_component(source("test_build_error", "Test (build error)."))]
#[derive(Clone, Debug, Default)]
pub struct BuildErrorSourceConfig {
    /// Meaningless field that only exists for triggering config diffs during topology reloading.
    data: Option<String>,
}

impl_generate_config_from_default!(BuildErrorSourceConfig);

#[async_trait]
#[typetag::serde(name = "test_build_error")]
impl SourceConfig for BuildErrorSourceConfig {
    async fn build(&self, _cx: SourceContext) -> crate::Result<Source> {
        Err("test_build_error: this source never builds".into())
    }

    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_maybe_logs(
            DataType::Log,
            Definition::default_legacy_namespace(),
        )]
    }

    fn can_acknowledge(&self) -> bool {
        false
    }
}
