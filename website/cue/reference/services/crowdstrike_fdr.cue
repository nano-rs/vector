package metadata

services: crowdstrike_fdr: {
	name:     "CrowdStrike Falcon Data Replicator"
	thing:    "a \(name) feed"
	url:      urls.crowdstrike_fdr
	versions: null

	description: "[CrowdStrike Falcon Data Replicator (FDR)](\(urls.crowdstrike_fdr)) replicates raw Falcon endpoint telemetry to an S3 bucket and announces each batch on an SQS queue with a manifest of the objects it wrote."
}
