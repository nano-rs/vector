HTTP client and Prometheus scrape sources now skip missed ticks and run at most one request at a time per source. Scrape timeouts include the response body. The new `max_response_bytes` option defaults to 32 MiB; larger responses are refused and counted as component errors.

authors: the2dl
