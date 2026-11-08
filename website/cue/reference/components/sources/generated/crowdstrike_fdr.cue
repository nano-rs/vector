package metadata

generated: components: sources: crowdstrike_fdr: configuration: {
	acknowledgements: {
		deprecated: true
		description: """
			Controls how acknowledgements are handled by this source.

			This setting is **deprecated** in favor of enabling `acknowledgements` at the [global][global_acks] or sink level.

			Enabling or disabling acknowledgements at the source level has **no effect** on acknowledgement behavior.

			See [End-to-end Acknowledgements][e2e_acks] for more information on how event acknowledgement is handled.

			[global_acks]: https://vector.dev/docs/reference/configuration/global-options/#acknowledgements
			[e2e_acks]: https://vector.dev/docs/architecture/end-to-end-acknowledgements/
			"""
		required: false
		type: object: options: enabled: {
			description: "Whether or not end-to-end acknowledgements are enabled for this source."
			required:    false
			type: bool: {}
		}
	}
	auth: {
		description: "Configuration of the authentication strategy for interacting with AWS services."
		required:    false
		type:        _schemaDefinitions["vector::aws::auth::AwsAuthentication"]
	}
	checksum: {
		description: "How to verify the per-object checksum in the manifest (32 hex digits = MD5, 64 = SHA-256)."
		required:    false
		type: string: {
			default: "verify"
			enum: {
				off: "Do not hash files."
				verify: """
					Verify every file. A missing checksum, one of unknown shape, a mismatch or a size
					mismatch fails the file, which releases the manifest for re-delivery.
					"""
				warn: "Verify every file, but only log a warning (and count an error) on failure."
			}
		}
	}
	connect_timeout_secs: {
		description: "Timeout for establishing a connection to SQS or S3, in seconds."
		required:    false
		type: uint: {
			default: 20
			unit:    "seconds"
		}
	}
	cursor_grace_secs: {
		description: """
			With `start_from = cursor`, how far before the stored cursor still counts as new, in
			seconds. Covers batches that arrive out of order or were released before a restart.
			"""
		required: false
		type: uint: {
			default: 3600
			unit:    "seconds"
		}
	}
	data_dir: {
		description: """
			The directory for the `start_from = cursor` state.

			Defaults to the global `data_dir` option.
			"""
		required: false
		type: string: examples: ["/var/lib/vector"]
	}
	decoding: {
		description: """
			Configures how events are decoded from raw bytes. Note some decoders can also determine the event output
			type (log, metric, trace).
			"""
		required: false
		type:     _schemaDefinitions["derived::codecs::decoding::DeserializerConfig::2c0db0ef1f05c78303bd4391"]
	}
	delete_failed_message: {
		description: """
			Whether to delete a message whose events a sink rejected permanently.

			When `false` (the default), the message re-delivers until `max_tries`.
			"""
		required: false
		type: bool: default: false
	}
	delete_message: {
		description: """
			Whether to delete a manifest's message once it is delivered.

			Set to `false` only for debugging: every message re-delivers.
			"""
		required: false
		type: bool: default: true
	}
	endpoint: {
		description: "Custom endpoint for use with AWS-compatible services."
		required:    false
		type: string: examples: ["http://127.0.0.0:5000/path/to/service"]
	}
	file_concurrency: {
		description: """
			Number of objects downloaded at once, across all manifests.

			Each download is buffered in memory while it is verified (FDR objects are about 30-35 MB).
			"""
		required: false
		type: uint: default: 4
	}
	force_path_style: {
		description: "Specifies which addressing style to use for S3."
		required:    false
		type: bool: default: true
	}
	heartbeat_interval_secs: {
		description: """
			How often to extend a held message's visibility, in seconds.

			Defaults to half of `visibility_timeout_secs`, and may not exceed it.
			"""
		required: false
		type: uint: unit: "seconds"
	}
	max_hold_secs: {
		description: """
			The longest a manifest is held, in seconds.

			After this, visibility is no longer extended and the source stops waiting for the
			manifest's acknowledgements, so the message re-delivers.
			"""
		required: false
		type: uint: {
			default: 36000
			unit:    "seconds"
		}
	}
	max_inflight_bytes: {
		description: """
			Maximum total compressed size (per the manifests) of held manifests, in bytes.

			A manifest larger than the whole budget is still processed, alone. Unlimited when unset.
			"""
		required: false
		type: uint: unit: "bytes"
	}
	max_inflight_manifests: {
		description: """
			Maximum number of manifests held (received but not yet deleted or released) at once.

			The source stops polling while this many are in flight.
			"""
		required: false
		type: uint: default: 64
	}
	max_line_bytes: {
		description: "Lines longer than this many bytes are dropped (and counted as errors)."
		required:    false
		type: uint: {
			default: 8388608
			unit:    "bytes"
		}
	}
	max_object_bytes: {
		description: """
			An object larger than this many (compressed) bytes fails instead of being buffered.

			Objects are held in memory while they are verified, so this bounds memory use to about
			`file_concurrency` times this value.
			"""
		required: false
		type: uint: {
			default: 536870912
			unit:    "bytes"
		}
	}
	max_number_of_messages: {
		description: "Maximum number of messages to receive per poll (1-10)."
		required:    false
		type: uint: default: 10
	}
	max_tries: {
		description: """
			How many deliveries of a message to process before leaving it alone.

			A message received more than this many times (per `ApproximateReceiveCount`) is not
			processed, deleted or released; configure a redrive policy (dead-letter queue) on the
			queue to move it aside.
			"""
		required: false
		type: uint: default: 3
	}
	operation_timeout_secs: {
		description: """
			Timeout for one SQS or S3 operation, in seconds.

			For a GET this covers the response headers; the body is bounded by
			`stream_idle_timeout_secs`. Must exceed `poll_secs`.
			"""
		required: false
		type: uint: {
			default: 90
			unit:    "seconds"
		}
	}
	poll_secs: {
		description: "How long to long-poll the queue for messages, in seconds (0-20)."
		required:    false
		type: uint: {
			default: 20
			unit:    "seconds"
		}
	}
	queue_depth_interval_secs: {
		description: """
			How often to read the queue depth with `GetQueueAttributes`, in seconds; `0` disables it.

			If the credentials may not call `GetQueueAttributes`, the gauge is disabled at the first
			denial.
			"""
		required: false
		type: uint: {
			default: 60
			unit:    "seconds"
		}
	}
	queue_url: {
		description: "The URL of the FDR SQS queue that carries batch manifests."
		required:    true
		type: string: examples: ["https://sqs.us-west-2.amazonaws.com/123456789012/cs-mav-cannon-queue"]
	}
	read_timeout_secs: {
		description: "Timeout for the first byte of a response, in seconds. Must exceed `poll_secs`."
		required:    false
		type: uint: {
			default: 60
			unit:    "seconds"
		}
	}
	region: {
		description: """
			The [AWS region][aws_region] of the target service.

			[aws_region]: https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints
			"""
		required: false
		type: string: examples: ["us-east-1"]
	}
	retry_delay_secs: {
		description: "After a failure, how long the message stays invisible before it re-delivers, in seconds."
		required:    false
		type: uint: {
			default: 30
			unit:    "seconds"
		}
	}
	route_lookups: {
		description: """
			Send lookup snapshots (`fdrv2-*` objects such as aidmaster and managedassets) to the
			`lookups` output instead of the default output.
			"""
		required: false
		type: bool: default: true
	}
	start_from: {
		description: """
			Which manifests to process, by batch time.

			- `beginning`: every manifest on the queue.
			- `now`: only batches newer than the time the source started.
			- `cursor`: only batches newer than the newest batch this source fully delivered before
			  (persisted in `data_dir`), minus `cursor_grace_secs`. Every manifest is processed on
			  the first run.
			- An RFC 3339 timestamp: only batches newer than it.

			Older manifests are deleted without downloading and counted as skipped.
			"""
		required: false
		type: string: {
			default: "beginning"
			examples: ["beginning", "cursor", "2026-11-01T00:00:00Z"]
		}
	}
	stream_idle_timeout_secs: {
		description: "A GET whose body sends no data for this many seconds fails (and its manifest is released)."
		required:    false
		type: uint: {
			default: 120
			unit:    "seconds"
		}
	}
	tls: {
		description: "TLS configuration."
		required:    false
		type:        _schemaDefinitions["core::option::Option<vector_core::tls::settings::TlsConfig>"]
	}
	visibility_timeout_secs: {
		description: "The visibility timeout to request when receiving, and to renew while a manifest is held."
		required:    false
		type: uint: {
			default: 300
			unit:    "seconds"
		}
	}
}
