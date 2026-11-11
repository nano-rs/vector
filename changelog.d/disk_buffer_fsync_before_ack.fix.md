A disk buffer now `fsync`s its data file before it acknowledges a record that a source with end-to-end acknowledgements is waiting on, so an HTTP 2xx, OTLP success or HEC ackId of true survives a host crash (power loss, kernel panic, hard reset), not only a process kill. A background group syncer runs the sync while the writer keeps writing, so records from concurrent requests share one sync. Records no source waits on keep the periodic sync. A new data file's directory entry is synced too (NAN-5285).

authors: nano-rs
