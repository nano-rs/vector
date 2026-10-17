Small `--chunk-size-events` values now also bound transform input queues, preventing whole HTTP responses from accumulating behind a blocked sink. The default 100-event transform queue ceiling is unchanged.

authors: the2dl
