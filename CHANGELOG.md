# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.9]
### Added

### Dependencies

### Changed
<<<<<<< HEAD
=======
- [CCR] Add getHistoryOperationsFromTranslog method to fetch the history snapshot from translogs ([#3948](https://github.com/opensearch-project/OpenSearch/pull/3948))
- Relax visibility of the HTTP_CHANNEL_KEY and HTTP_SERVER_CHANNEL_KEY to make it possible for the plugins to access associated Netty4HttpChannel / Netty4HttpServerChannel instance ([#4638](https://github.com/opensearch-project/OpenSearch/pull/4638))
- Migrate client transports to Apache HttpClient / Core 5.x ([#4459](https://github.com/opensearch-project/OpenSearch/pull/4459))
- Change http code on create index API with bad input raising NotXContentException from 500 to 400 ([#4773](https://github.com/opensearch-project/OpenSearch/pull/4773))
- Improve summary error message for invalid setting updates ([#4792](https://github.com/opensearch-project/OpenSearch/pull/4792))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))

### Deprecated

### Removed
- Remove deprecated code to add node name into log pattern of log4j property file ([#4568](https://github.com/opensearch-project/OpenSearch/pull/4568))
- Unused object and import within TransportClusterAllocationExplainAction ([#4639](https://github.com/opensearch-project/OpenSearch/pull/4639))
- Remove LegacyESVersion.V_7_0_* and V_7_1_* Constants ([#2768](https://https://github.com/opensearch-project/OpenSearch/pull/2768))
- Remove LegacyESVersion.V_7_2_ and V_7_3_ Constants ([#4702](https://github.com/opensearch-project/OpenSearch/pull/4702))
- Always auto release the flood stage block ([#4703](https://github.com/opensearch-project/OpenSearch/pull/4703))
- Remove LegacyESVersion.V_7_4_ and V_7_5_ Constants ([#4704](https://github.com/opensearch-project/OpenSearch/pull/4704))
- Remove Legacy Version support from Snapshot/Restore Service ([#4728](https://github.com/opensearch-project/OpenSearch/pull/4728))
- Remove deprecated serialization logic from pipeline aggs ([#4847](https://github.com/opensearch-project/OpenSearch/pull/4847))
- Remove unused private methods ([#4926](https://github.com/opensearch-project/OpenSearch/pull/4926))
- Remove LegacyESVersion.V_7_8_ and V_7_9_ Constants ([#4855](https://github.com/opensearch-project/OpenSearch/pull/4855))
- Remove LegacyESVersion.V_7_6_ and V_7_7_ Constants ([#4837](https://github.com/opensearch-project/OpenSearch/pull/4837))
- Remove LegacyESVersion.V_7_10_ Constants ([#5018](https://github.com/opensearch-project/OpenSearch/pull/5018))
- Remove Version.V_1_ Constants ([#5021](https://github.com/opensearch-project/OpenSearch/pull/5021))
- Remove custom Map, List and Set collection classes ([#6871](https://github.com/opensearch-project/OpenSearch/pull/6871))

### Fixed
- Fix 'org.apache.hc.core5.http.ParseException: Invalid protocol version' under JDK 16+ ([#4827](https://github.com/opensearch-project/OpenSearch/pull/4827))
- Fix compression support for h2c protocol ([#4944](https://github.com/opensearch-project/OpenSearch/pull/4944))
- Don't over-allocate in HeapBufferedAsyncEntityConsumer in order to consume the response ([#9993](https://github.com/opensearch-project/OpenSearch/pull/9993))

### Security

## [Unreleased 2.x]
### Added
- Add coordinator level stats for search latency ([#8386](https://github.com/opensearch-project/OpenSearch/issues/8386))
- Add metrics for thread_pool task wait time ([#9681](https://github.com/opensearch-project/OpenSearch/pull/9681))
- Async blob read support for S3 plugin ([#9694](https://github.com/opensearch-project/OpenSearch/pull/9694))
- [Telemetry-Otel] Added support for OtlpGrpcSpanExporter exporter ([#9666](https://github.com/opensearch-project/OpenSearch/pull/9666))
- Async blob read support for encrypted containers ([#10131](https://github.com/opensearch-project/OpenSearch/pull/10131))
- Add capability to restrict async durability mode for remote indexes ([#10189](https://github.com/opensearch-project/OpenSearch/pull/10189))
- Add Doc Status Counter for Indexing Engine ([#4562](https://github.com/opensearch-project/OpenSearch/issues/4562))

### Dependencies
- Bump `peter-evans/create-or-update-comment` from 2 to 3 ([#9575](https://github.com/opensearch-project/OpenSearch/pull/9575))
- Bump `actions/checkout` from 2 to 4 ([#9968](https://github.com/opensearch-project/OpenSearch/pull/9968))
- Bump OpenTelemetry from 1.26.0 to 1.30.1 ([#9950](https://github.com/opensearch-project/OpenSearch/pull/9950))
- Bump `org.apache.commons:commons-compress` from 1.23.0 to 1.24.0 ([#9973, #9972](https://github.com/opensearch-project/OpenSearch/pull/9973, https://github.com/opensearch-project/OpenSearch/pull/9972))
- Bump `com.google.cloud:google-cloud-core-http` from 2.21.1 to 2.23.0 ([#9971](https://github.com/opensearch-project/OpenSearch/pull/9971))
- Bump `mockito` from 5.4.0 to 5.5.0 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `bytebuddy` from 1.14.3 to 1.14.7 ([#10022](https://github.com/opensearch-project/OpenSearch/pull/10022))
- Bump `com.zaxxer:SparseBitSet` from 1.2 to 1.3 ([#10098](https://github.com/opensearch-project/OpenSearch/pull/10098))
- Bump `tibdex/github-app-token` from 1.5.0 to 2.1.0 ([#10125](https://github.com/opensearch-project/OpenSearch/pull/10125))
- Bump `org.wiremock:wiremock-standalone` from 2.35.0 to 3.1.0 ([#9752](https://github.com/opensearch-project/OpenSearch/pull/9752))
- Bump `com.google.http-client:google-http-client-jackson2` from 1.43.2 to 1.43.3 ([#10126](https://github.com/opensearch-project/OpenSearch/pull/10126))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.3 to 1.1.10.4 ([#10206](https://github.com/opensearch-project/OpenSearch/pull/10206))
- Bump `com.google.api.grpc:proto-google-common-protos` from 2.10.0 to 2.25.0 ([#10208](https://github.com/opensearch-project/OpenSearch/pull/10208))
- Bump `codecov/codecov-action` from 2 to 3 ([#10209](https://github.com/opensearch-project/OpenSearch/pull/10209))
- Bump `org.bouncycastle:bcpkix-jdk15to18` from 1.75 to 1.76 ([10219](https://github.com/opensearch-project/OpenSearch/pull/10219))`

### Changed
- Add instrumentation in rest and network layer. ([#9415](https://github.com/opensearch-project/OpenSearch/pull/9415))
- Allow parameterization of tests with OpenSearchIntegTestCase.SuiteScopeTestCase annotation ([#9916](https://github.com/opensearch-project/OpenSearch/pull/9916))
- Mute the query profile IT with concurrent execution ([#9840](https://github.com/opensearch-project/OpenSearch/pull/9840))
- Force merge with `only_expunge_deletes` honors max segment size ([#10036](https://github.com/opensearch-project/OpenSearch/pull/10036))
- Add instrumentation in transport service. ([#10042](https://github.com/opensearch-project/OpenSearch/pull/10042))
- [Tracing Framework] Add support for SpanKind. ([#10122](https://github.com/opensearch-project/OpenSearch/pull/10122))
>>>>>>> d656e3db592 (Indexing: add Doc Status Counter (#8716))

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 2.9]: https://github.com/opensearch-project/OpenSearch/compare/2.9...2.x
