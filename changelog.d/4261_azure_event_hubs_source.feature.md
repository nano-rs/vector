Added a new `azure_event_hubs` source that consumes every partition of an Azure Event Hub over AMQP 1.0 with Microsoft's official `azure_messaging_eventhubs` SDK.

It authenticates with an Event Hubs connection string (Shared Access Signature) or with Microsoft Entra ID through the same `auth` options as the Azure sinks (client secret, client certificate, managed identity, or workload identity with a federated token file). Positions are checkpointed per partition in an Azure Blob Storage container (`checkpoint_store`, compatible with the Azure SDK blob checkpoint layout) only after events are acknowledged end to end, so delivery is at-least-once across restarts. Each event carries its `partition_id`, `offset`, `sequence_number`, `enqueued_time`, `partition_key`, and application `properties` as source metadata, and supports the standard `framing` and `decoding` options.

authors: the2dl
