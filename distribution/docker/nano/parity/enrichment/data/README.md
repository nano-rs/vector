Fixture data for the `enrichment/` parity unit (NAN-3604, enrichment kept for NAN-3489).

- `custom-type.mmdb`, `GeoLite2-ASN-Test.mmdb`: byte copies of this repository's
  `tests/data/` fixtures (upstream Vector test data; the ASN file is MaxMind's public
  MaxMind-DB test-data, dual Apache-2.0/MIT). A few KiB each, no real geolocation data.
- `ioc.csv`: two documentation-range IPs (RFC 5737), hand-written.
