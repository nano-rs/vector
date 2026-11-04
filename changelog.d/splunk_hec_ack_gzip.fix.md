The `splunk_hec` source now decodes `Content-Encoding: gzip` on `/services/collector/ack`. Vector's own `splunk_hec_logs` sink compresses its indexer-ack poll with the sink's `compression`, so with gzip every poll was answered 400 and the sender fell back to treating the plain 200 as delivery (NAN-4976).

authors: nano-rs
