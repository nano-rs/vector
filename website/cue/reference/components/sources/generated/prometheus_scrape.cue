package metadata

generated: components: sources: prometheus_scrape: configuration: {
	auth: {
		description: """
			Configuration of the authentication strategy for HTTP requests.

			HTTP authentication should be used with HTTPS only, as the authentication credentials are passed as an
			HTTP header without any additional encryption beyond what is provided by the transport itself.
			"""
		required: false
		type:     _schemaDefinitions["core::option::Option<vector::http::Auth>"]
	}
	endpoint_tag: {
		description: """
			The tag name added to each event representing the scraped instance's endpoint.

			The tag value is the endpoint of the scraped instance.
			"""
		required: false
		type: string: {}
	}
	endpoints: {
		description: "Endpoints to scrape metrics from."
		required:    true
		type: array: items: type: string: examples: ["http://localhost:9090/metrics"]
	}
	headers: {
		description: """
			Headers to apply to the scrape requests.

			One or more values for the same header can be provided.
			"""
		required: false
		type: object: {
			examples: [{
				"X-My-Header": ["value1", "value2"]
			}]
			options: "*": {
				description: "An HTTP request header and its value(s)."
				required:    true
				type: array: items: type: string: {}
			}
		}
	}
	honor_labels: {
		description: """
			Controls how tag conflicts are handled if the scraped source has tags to be added.

			If `true`, the new tag is not added if the scraped metric has the tag already. If `false`, the conflicting tag
			is renamed by prepending `exported_` to the original name.

			This matches Prometheus’ `honor_labels` configuration.
			"""
		required: false
		type: bool: default: false
	}
	instance_tag: {
		description: """
			The tag name added to each event representing the scraped instance's `host:port`.

			The tag value is the host and port of the scraped instance.
			"""
		required: false
		type: string: {}
	}
	max_response_bytes: {
		description: "Maximum response body size in bytes. Oversize responses are refused and counted as errors."
		required:    false
		type: uint: default: 33554432
	}
	query: {
		description: """
			Custom parameters for the scrape request query string.

			One or more values for the same parameter key can be provided. The parameters provided in this option are
			appended to any parameters manually provided in the `endpoints` option. This option is especially useful when
			scraping the `/federate` endpoint.
			"""
		required: false
		type: object: {
			examples: [{
				"match[]": ["{job=\"somejob\"}", "{__name__=~\"job:.*\"}"]
			}]
			options: "*": {
				description: "Configuration of the query parameter value for HTTP requests."
				required:    true
				type:        _schemaDefinitions["vector::http::ParameterValue"]
			}
		}
	}
	scrape_interval_secs: {
		description: """
			The interval between scrapes. Only one request runs at a time per source.
			Missed ticks are skipped after a stall.
			"""
		required: false
		type: uint: {
			default: 15
			unit:    "seconds"
		}
	}
	scrape_timeout_secs: {
		description: "The timeout for each scrape request, including reading the response body."
		required:    false
		type: float: {
			default: 5.0
			unit:    "seconds"
		}
	}
	tls: {
		description: "TLS configuration."
		required:    false
		type:        _schemaDefinitions["core::option::Option<vector_core::tls::settings::TlsConfig>"]
	}
}
