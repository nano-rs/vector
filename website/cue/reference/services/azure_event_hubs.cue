package metadata

services: azure_event_hubs: {
	name:     "Azure Event Hubs"
	thing:    "an \(name) event hub"
	url:      urls.azure_event_hubs
	versions: null

	description: "[Azure Event Hubs](\(urls.azure_event_hubs)) is a fully managed, real-time data ingestion service on Microsoft Azure. It is the standard way to stream Azure platform logs, such as diagnostic settings and Microsoft Entra ID logs, to external systems."
}
