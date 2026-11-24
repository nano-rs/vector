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
			The directory for the `start_from = cursor` and `start_from = now` state, the record of
			which files of each held batch are delivered, and objects spooled to disk.

			Defaults to the global `data_dir` option. Without a usable data directory, delivered
			files are remembered only until restart and objects spool to the system temp directory.

			Skipping already-delivered files on re-delivery works within one instance with a
			persistent data directory. A batch re-delivered to another instance, or to this one after
			its data directory was lost (for example an ephemeral volume), is emitted whole.
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
	fetch_max_tries: {
		description: """
			How many times to try fetching one object (GET, body read and checksum) before giving
			up on its batch.

			Tries are spaced by `fetch_retry_backoff_secs`, doubling, while the message is still
			held. Only when one object has used every try is the batch released (and that object
			logged). A re-delivered batch skips the files already delivered.
			"""
		required: false
		type: uint: default: 4
	}
	fetch_retry_backoff_secs: {
		description: """
			The wait after the first failed try of an object, in seconds. Doubles with every
			further try, up to 30 seconds.
			"""
		required: false
		type: uint: {
			default: 1
			unit:    "seconds"
		}
	}
	file_concurrency: {
		description: """
			Number of objects fetched at once, across all manifests.

			One reader per object: at 1, throughput is bound by one download and decode at a time
			(about one 30 MB FDR object a minute in the field); 16 kept a pipeline busy. Memory stays
			within `max_memory_bytes` whatever this is; objects past it spool to `data_dir`.

			Each object is downloaded completely and verified before any of its events is emitted,
			in its own task, so a backed-up pipeline never leaves a GET half-read. A downloaded
			object keeps its slot until its events are emitted; this also bounds how far a manifest
			prefetches ahead of the file it is emitting. Objects are emitted in manifest order.
			"""
		required: false
		type: uint: default: 16
	}
	force_path_style: {
		description: """
			Whether to address S3 path-style (`https://s3.<region>.amazonaws.com/<bucket>/<key>`).

			Off by default: FDR buckets are read through S3 access-point aliases, which AWS serves
			virtual-hosted (the SDK default). Turn it on for S3-compatible endpoints such as LocalStack.
			"""
		required: false
		type: bool: default: false
	}
	heartbeat_interval_secs: {
		description: """
			How often to extend a held message's visibility, in seconds.

			Defaults to half of `visibility_timeout_secs`, and may not exceed that half.
			"""
		required: false
		type: uint: unit: "seconds"
	}
	max_hold_secs: {
		description: """
			The longest a manifest is held, in seconds.

			After this, visibility is no longer extended and the source stops waiting for the
			manifest's acknowledgements, so the message re-delivers. At most 43200 (12 hours), the
			longest SQS lets a received message stay invisible.
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

			Objects are buffered (in memory, or on disk past `spool_threshold_bytes`) while they are
			verified.
			"""
		required: false
		type: uint: {
			default: 536870912
			unit:    "bytes"
		}
	}
	max_memory_bytes: {
		description: """
			Total memory for objects held between download and emission, across all manifests.

			An object is held in memory only if its share fits (its listed size, or
			`spool_threshold_bytes` when the manifest does not list one); otherwise it spools to
			`data_dir`. Memory for objects never exceeds this value, whatever `file_concurrency` is.
			`0` spools every object.
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
	skip_action: {
		description: """
			What to do with a batch older than the `start_from` cutoff: `delete` it (on its first
			delivery only) or `release` it back to the queue, never deleting it.
			"""
		required: false
		type: string: {
			default: "delete"
			enum: {
				delete: """
					Delete the message without downloading, on its first delivery only (a message received
					before was accepted by an earlier run and is processed normally). The batch is gone
					from the queue for every consumer.
					"""
				release: """
					Never delete: hand the message back (visible again after `retry_delay_secs`) without
					downloading, on every delivery, unless this instance already delivered part of the
					batch. The batch stays on the queue for other consumers, or until the queue's retention
					period or dead-letter policy (after `max_tries` receives) removes it.
					"""
			}
		}
	}
	spool_threshold_bytes: {
		description: """
			Objects up to this many (compressed) bytes are held in memory between download and
			emission; larger ones are written to a temporary file in `data_dir`.

			Memory for objects is also capped in total by `max_memory_bytes`; an object that does
			not fit spools even below this threshold. Spooled objects take disk instead: up to
			`file_concurrency` times `max_object_bytes` in the worst case (FDR objects are about
			30-35 MB, so typically `file_concurrency` times that). If the spool directory is not
			writable, the source logs an error at startup and holds objects in memory only: a
			manifest then waits for memory instead of spooling, and only an object that can never
			fit in `max_memory_bytes` (or one of unlisted size that grows past this threshold)
			fails, releasing its batch.
			"""
		required: false
		type: uint: {
			default: 67108864
			unit:    "bytes"
		}
	}
	start_from: {
		description: """
			Which manifests to process, by batch time.

			- `beginning`: every manifest on the queue.
			- `now`: only batches newer than the time the source first started (persisted in
			  `data_dir`, so a restart does not move it).
			- `cursor`: only batches newer than the newest batch this source fully delivered before
			  (persisted in `data_dir`), minus `cursor_grace_secs`. Every manifest is processed on
			  the first run.
			- An RFC 3339 timestamp: only batches newer than it.

			What happens to an older manifest is set by `skip_action`. With the default (`delete`),
			it is **deleted** from the queue without downloading, and counted as skipped, only on
			its first delivery (`ApproximateReceiveCount` 1). A message received before was accepted
			by an earlier run and is processed normally, so a restart never drops undelivered
			batches. Use `skip_action = release` when other consumers share the queue. The cursor
			never moves past a batch that is still held or parked.
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
