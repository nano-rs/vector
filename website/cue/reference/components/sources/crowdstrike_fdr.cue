package metadata

components: sources: crowdstrike_fdr: {
	title: "CrowdStrike FDR"

	features: {
		auto_generated:   true
		acknowledgements: true
		collect: {
			checkpoint: enabled: false
			proxy: enabled:      true
			tls: {
				enabled:                true
				can_verify_certificate: true
				can_verify_hostname:    true
				enabled_default:        true
				enabled_by_scheme:      true
			}
			from: service: services.crowdstrike_fdr
		}
		multiline: enabled: false
		codecs: {
			enabled:         true
			default_framing: "newline_delimited"
		}
	}

	classes: {
		deployment_roles: ["aggregator"]
		delivery:      "at_least_once"
		development:   "beta"
		egress_method: "stream"
		stateful:      false
	}

	support: {
		requirements: [
			"""
				The credentials need `sqs:ReceiveMessage`, `sqs:DeleteMessage` and
				`sqs:ChangeMessageVisibility` on the FDR queue, and `s3:GetObject` on the FDR bucket
				(often an S3 access-point alias, used as the bucket name). `sqs:GetQueueAttributes` is
				optional: without it the queue depth gauge is disabled.
				""",
			"""
				Configure a redrive policy (dead-letter queue) on the queue if it is under your
				control: messages received more than `max_tries` times are left alone.
				""",
		]
		warnings: [
			"""
				Each object is downloaded completely and verified before its events are emitted. Objects
				are held in memory up to `max_memory_bytes` in total (512 MiB; FDR objects are about
				30-35 MB, and those past `spool_threshold_bytes` always spool); the rest spool to
				`data_dir`, using up to `file_concurrency` (16) times `max_object_bytes` of disk in the
				worst case.
				""",
			"""
				Skipping already-delivered files of a re-delivered batch needs a persistent `data_dir`
				and works within one instance only: a batch re-delivered to another instance, or after
				the data directory is lost, is emitted whole. In a container, mount a volume at the data
				directory (`/var/lib/vector` by default); the source warns at startup when it sits on
				the container's writable layer or a tmpfs.
				""",
			"""
				`start_from` (other than `beginning`) **deletes** batches older than its cutoff on their
				first delivery, by default. If other consumers share the queue, set
				`skip_action: release` to hand them back instead.
				""",
		]
	}

	installation: {
		platform_name: null
	}

	configuration: generated.components.sources.crowdstrike_fdr.configuration

	output: logs: record: {
		description: "One line of an FDR object, decoded with `decoding`. With the `json` codec, the FDR event's own fields are at the root."
		fields: {
			message: {
				description: "The line, when `decoding.codec` is `bytes` (the default)."
				required:    false
				type: string: {
					examples: ["{\"event_simpleName\":\"ProcessRollup2\",\"aid\":\"...\"}"]
					syntax: "literal"
				}
			}
			"fdr.file": {
				description: "The S3 key of the object the line came from. With `fdr.line`, it identifies the line, so a sink can drop re-delivered duplicates."
				required:    true
				type: string: examples: ["0123456789abcdef0123456789abcdef/data/7f6c.../part-00000.gz"]
			}
			"fdr.line": {
				description: "The zero-based line number in the decompressed object."
				required:    true
				type: uint: {
					examples: [0, 1532]
					unit: null
				}
			}
			"fdr.cid": {
				description: "The customer ID from the manifest."
				required:    true
				type: string: examples: ["0123456789abcdef0123456789abcdef"]
			}
			"fdr.bucket": {
				description: "The bucket (or access-point alias) from the manifest."
				required:    true
				type: string: examples: ["cs-mav-cannon-example-s3alias"]
			}
			"fdr.kind": {
				description: "`lookup` for lookup snapshots (`fdrv2-*` objects such as aidmaster and managedassets), `data` otherwise."
				required:    true
				type: string: examples: ["data", "lookup"]
			}
			"fdr.batch_timestamp": {
				description: "The batch time from the manifest."
				required:    true
				type: timestamp: {}
			}
			source_type: {
				description: "The name of the source type."
				required:    true
				type: string: examples: ["crowdstrike_fdr"]
			}
			timestamp: fields._current_timestamp
		}
	}

	telemetry: metrics: {
		crowdstrike_fdr_manifests_total:     components.sources.internal_metrics.output.metrics.crowdstrike_fdr_manifests_total
		crowdstrike_fdr_files_fetched_total: components.sources.internal_metrics.output.metrics.crowdstrike_fdr_files_fetched_total
		crowdstrike_fdr_bytes_fetched_total: components.sources.internal_metrics.output.metrics.crowdstrike_fdr_bytes_fetched_total
		crowdstrike_fdr_files_skipped_total: components.sources.internal_metrics.output.metrics.crowdstrike_fdr_files_skipped_total
		crowdstrike_fdr_batch_lag_seconds:   components.sources.internal_metrics.output.metrics.crowdstrike_fdr_batch_lag_seconds
		crowdstrike_fdr_inflight_manifests:  components.sources.internal_metrics.output.metrics.crowdstrike_fdr_inflight_manifests
		crowdstrike_fdr_inflight_bytes:      components.sources.internal_metrics.output.metrics.crowdstrike_fdr_inflight_bytes
		crowdstrike_fdr_queue_depth:         components.sources.internal_metrics.output.metrics.crowdstrike_fdr_queue_depth
	}

	how_it_works: {
		manifests: {
			title: "Manifests, not S3 notifications"
			body: """
				FDR announces each batch with one SQS message whose body is a manifest:
				`cid`, `timestamp` (epoch milliseconds, possibly as a string), `bucket`, `pathPrefix`
				and `files`, each with a `path`, `size` and `checksum`. The source downloads every
				listed object completely (at most `file_concurrency` at a time across all manifests,
				each in its own task so a backed-up pipeline never leaves a download half-read),
				verifies its checksum, and only then decompresses it if it is gzip and emits one event
				per line, file by file in manifest order. Lines longer than `max_line_bytes` are
				dropped and counted as errors.
				"""
		}
		holding: {
			title: "Holding, heartbeats and re-delivery"
			body: """
				With end-to-end acknowledgements enabled, a message is deleted only after every event
				of every object in its manifest is acknowledged by the sinks. Until then, its
				visibility is extended every `heartbeat_interval_secs` (half the visibility timeout by
				default). After `max_hold_secs` the source stops extending it and stops waiting, so
				the message re-delivers.

				A failed or timed-out GET, a body cut short or stalled (no data for
				`stream_idle_timeout_secs`), or a checksum mismatch is tried again, up to
				`fetch_max_tries` times with a doubling `fetch_retry_backoff_secs`, while the message
				stays held. Nothing of an object is emitted before it is complete and verified. When
				one object has used every try, or on an unparseable manifest or a sink error, the
				message is released: it becomes visible again after `retry_delay_secs`. A message
				received more than `max_tries` times is left alone, for the queue's dead-letter
				policy.

				Each file of a manifest is recorded as delivered (in `data_dir`, keyed by the
				manifest) once all of its own events are acknowledged, and a re-delivered manifest
				skips those files; the record is removed once the message is deleted. A release waits
				for the acknowledgements of files already emitted, so a re-delivery repeats only what
				was not delivered.

				`max_inflight_manifests` and `max_inflight_bytes` bound how many manifests are held at
				once; the source stops polling while the limit is reached.

				What counts as delivered follows Vector's acknowledgement rules: a sink error
				(`Errored`) or rejection (`Rejected`) on any one event releases the whole manifest
				(a rejection deletes it only with `delete_failed_message`), and so does a pipeline
				that closes or times out while events are handed to it. An event a transform drops
				on purpose (for example a `filter`) counts as delivered. Without acknowledgements the
				source warns at startup and deletes each message once its events enter the pipeline.
				"""
		}
		restarts: {
			title: "Restarts and duplicates"
			body: """
				Delivery is at least once. On shutdown the source stops receiving, hands every
				unfinished manifest back to the queue at once, and keeps waiting for the
				acknowledgements of files already emitted until Vector's shutdown deadline. A
				re-delivered manifest skips the files recorded as delivered (the record survives a
				restart when `data_dir` is usable), so at most the files that were in flight are
				emitted again, with the same `fdr.file` and `fdr.line`, so a sink keyed on those two
				drops them. The record is per instance and lives in `data_dir`: a manifest
				re-delivered to another instance, or after an ephemeral `data_dir` is lost, is emitted
				whole. Acknowledgements that arrive after `max_hold_secs` are still recorded. A manifest is never deleted before all of its events are
				acknowledged, so a restart does not lose events.
				"""
		}
		observability: {
			title: "Logs and metrics"
			body: """
				Every batch logs at `info`: when it is received (message ID, file count, bytes, batch
				time, receive count), when it is delivered and its message deleted, and when it is
				released (with the reason: an error code such as `failed_fetching_object`,
				`delivery_failed` or `sink_rejected`, or `shutdown`). A resumed batch logs how many
				files it skipped. Like all of Vector's internal logs, each line is rate limited per
				component (one per 10 seconds, then a count of the suppressed ones).

				The source's counters and gauges (`crowdstrike_fdr_manifests_total` for every
				status, files and bytes fetched, files skipped, in-flight manifests and bytes) are
				registered at zero when the source starts and never expire, whatever
				`expire_metrics_secs` is.
				"""
		}
		addressing: {
			title: "Access-point aliases"
			body: """
				FDR manifests name an S3 access-point alias as the bucket. The source reads it like a
				bucket, with virtual-hosted addressing (`force_path_style: false`, the default) in
				the source's `region`. Set `force_path_style: true` only for S3-compatible endpoints
				such as LocalStack.
				"""
		}
		checksums: {
			title: "Checksums"
			body: """
				A checksum of 32 hex digits is MD5, one of 64 is SHA-256. With `checksum: verify`
				(the default), a file whose checksum or size does not match fails, and so does a
				missing checksum or one of any other shape (before downloading). `warn` logs and
				counts the failure but keeps the events; `off` does not hash.
				"""
		}
		start_from: {
			title: "Backlog cutoff"
			body: """
				`start_from` drops old batches without downloading them: `now` skips batches older
				than the first start of the source (stored in `data_dir`), an RFC 3339 time skips
				batches older than it, and `cursor` skips batches older than the newest batch this
				source fully delivered before (stored in `data_dir`), minus `cursor_grace_secs`.
				Skipped messages are deleted and counted as `skipped`.

				Only a first delivery (`ApproximateReceiveCount` 1) is ever skipped. A message
				received before was accepted by an earlier run, or released by this one, and is
				processed normally, so a restart never deletes an undelivered batch. The cursor never
				moves past a batch that is still held or parked, and `now` does not move on restart.
				"""
		}
		lookups: {
			title: "Lookup snapshots"
			body: """
				Lookup snapshots (objects under an `fdrv2*` path segment, such as aidmaster and
				managedassets) arrive on the same queue. With `route_lookups` (the default) their
				events go to the `lookups` output (`<source id>.lookups`); otherwise they go to the
				default output. Every event carries `fdr.kind`.
				"""
		}
	}
}
