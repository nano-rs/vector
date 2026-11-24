package metadata

components: sources: azure_event_hubs: {
	title: "Azure Event Hubs"

	features: {
		auto_generated:   true
		acknowledgements: true
		collect: {
			checkpoint: enabled: true
			proxy: enabled:      false
			tls: enabled:        false
			from: service:       services.azure_event_hubs
		}
		multiline: enabled: false
		codecs: {
			enabled:         true
			default_framing: "bytes"
		}
	}

	classes: {
		deployment_roles: ["aggregator"]
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "stream"
		stateful:      true
	}

	support: {
		requirements: [
			"""
				The identity used by the source needs the `Listen` right on the Event Hub (a SAS policy
				with `Listen`, or the *Azure Event Hubs Data Receiver* role), and the checkpoint
				container must exist and be writable (for example with the *Storage Blob Data
				Contributor* role).
				""",
			"""
				The source connects over AMQP on TCP port 5671 to `*.servicebus.windows.net`. Microsoft
				Entra ID authentication also needs HTTPS (443) to the Entra token endpoint, and the
				blob checkpoint store needs HTTPS (443) to the storage account.
				""",
		]
		warnings: []
	}

	installation: {
		platform_name: null
	}

	configuration: generated.components.sources.azure_event_hubs.configuration

	output: logs: record: {
		description: "An event read from an Event Hub partition."
		fields: {
			message: {
				description: "The event body, decoded according to `framing` and `decoding`."
				required:    true
				type: string: {
					examples: ["{\"records\":[{\"category\":\"SignInLogs\"}]}"]
					syntax: "literal"
				}
			}
			event_hub_name: {
				description: "The name of the Event Hub the event was read from."
				required:    true
				type: string: examples: ["my-event-hub"]
			}
			partition_id: {
				description: "The ID of the partition the event was read from."
				required:    true
				type: string: examples: ["0"]
			}
			offset: {
				description: "The offset of the event in its partition."
				required:    false
				type: string: examples: ["4294967296"]
			}
			sequence_number: {
				description: "The sequence number of the event in its partition."
				required:    false
				type: uint: {
					examples: [42]
					unit: null
				}
			}
			enqueued_time: {
				description: "The time at which the Event Hubs service accepted the event."
				required:    false
				type: timestamp: {}
			}
			partition_key: {
				description: "The partition key the event was published with, if any."
				required:    false
				type: string: examples: ["device-1"]
			}
			properties: {
				description: "The application properties of the event, if any."
				required:    false
				type: object: examples: [{"run": "a"}]
			}
			source_type: {
				description: "The name of the source type."
				required:    true
				type: string: examples: ["azure_event_hubs"]
			}
			timestamp: fields._current_timestamp
		}
	}

	how_it_works: {
		checkpointing: {
			title: "Checkpointing and delivery guarantees"
			body: """
				The source reads every partition of the Event Hub (or those listed in `partition_ids`)
				with its own receiver. With a `checkpoint_store`, the position of each partition is
				written every `checkpoint_interval_secs` and on shutdown, but only up to the last event
				whose logs were all acknowledged by the sinks when end-to-end acknowledgements are
				enabled. After a restart each partition resumes right after its checkpoint, and a
				partition without one starts at `start_position`. If a sink reports a delivery
				failure, the partition is read again from the last acknowledged event. Delivery is
				therefore at-least-once: a crash can re-deliver the events read since the last
				checkpoint, but cannot skip any.

				With `start_position` set to `latest`, a partition without a checkpoint gets one
				before it is read: the position of its last enqueued event at that moment. A restart
				then resumes after it, even if the partition received no event in between, so events
				enqueued while Vector was down are never skipped. This first checkpoint is written
				create-only (`If-None-Match: *` on the blob) and never replaces an existing one.

				The checkpoint blobs use the layout of the Azure SDK blob checkpoint stores
				(`<namespace>/<event hub>/<consumer group>/checkpoint/<partition id>`).
				"""
		}
		authentication: {
			title: "Authentication"
			body: """
				Set `connection_string` to authenticate with a Shared Access Signature. A connection
				string with `SharedAccessKeyName` and `SharedAccessKey` signs short-lived tokens and
				renews them; one with a pre-formed `SharedAccessSignature` is used until it expires.

				Alternatively, set `namespace` and `auth` to authenticate with Microsoft Entra ID. The
				`workload_identity` credential reads a federated token from `token_file_path` and
				re-reads it as it rotates, which suits workload identity federation with an external
				token issuer.
				"""
		}
		consumer_groups: {
			title: "Consumer groups and scaling"
			body: """
				Give each Vector deployment its own `consumer_group`. Event Hubs allows at most five
				concurrent readers per partition and consumer group. Set `owner_level` to make the
				service disconnect older readers of the same partitions, so that only one instance
				reads at a time.
				"""
		}
	}
}
