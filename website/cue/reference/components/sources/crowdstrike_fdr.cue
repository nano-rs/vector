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
				Each object is held in memory while it is downloaded and verified (FDR objects are
				about 30-35 MB), so memory use grows with `file_concurrency`.
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
				listed object (at most `file_concurrency` at a time across all manifests),
				decompresses it if it is gzip, and emits one event per line. Lines longer than
				`max_line_bytes` are dropped and counted as errors.
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

				A failed or timed-out GET, a stalled body (no data for `stream_idle_timeout_secs`), a
				checksum failure, an unparseable manifest or a sink error releases the message: it
				becomes visible again after `retry_delay_secs`. Re-delivered manifests emit the same
				events again, with the same `fdr.file` and `fdr.line`. A message received more than
				`max_tries` times is left alone, for the queue's dead-letter policy.

				`max_inflight_manifests` and `max_inflight_bytes` bound how many manifests are held at
				once; the source stops polling while the limit is reached.
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
				than the start of the source, an RFC 3339 time skips batches older than it, and
				`cursor` skips batches older than the newest batch this source fully delivered before
				(stored in `data_dir`), minus `cursor_grace_secs`. Skipped messages are deleted and
				counted as `skipped`.
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
