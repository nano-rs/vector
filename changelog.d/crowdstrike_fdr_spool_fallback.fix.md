The `crowdstrike_fdr` source no longer fails batches when its spool directory is not writable (for example a read-only `/tmp` fallback): objects are then held in memory only, and a manifest waits for room within `max_memory_bytes` instead of spooling. Only an object that can never fit in `max_memory_bytes` fails. Batches that `start_from` skips with `skip_action = release` are no longer counted or logged as parked once they pass `max_tries`, and the batch-lag histogram no longer expires when idle or records deliveries of parked batches (NAN-5005).

authors: nano-rs
