# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Added
- [Offline Nodes] Adds offline-tasks library containing various interfaces to be used for Offline Background Tasks. ([#13574](https://github.com/opensearch-project/OpenSearch/pull/13574))
- Fix for hasInitiatedFetching to fix allocation explain and manual reroute APIs (([#14972](https://github.com/opensearch-project/OpenSearch/pull/14972))
- [Workload Management] Add queryGroupId to Task ([14708](https://github.com/opensearch-project/OpenSearch/pull/14708))
- Add setting to ignore throttling nodes for allocation of unassigned primaries in remote restore ([#14991](https://github.com/opensearch-project/OpenSearch/pull/14991))
- [Streaming Indexing] Enhance RestClient with a new streaming API support ([#14437](https://github.com/opensearch-project/OpenSearch/pull/14437))
- Add basic aggregation support for derived fields ([#14618](https://github.com/opensearch-project/OpenSearch/pull/14618))
- [Workload Management] Add Create QueryGroup API Logic ([#14680](https://github.com/opensearch-project/OpenSearch/pull/14680))- [Workload Management] Add Create QueryGroup API Logic ([#14680](https://github.com/opensearch-project/OpenSearch/pull/14680))
- Add ThreadContextPermission for markAsSystemContext and allow core to perform the method ([#15016](https://github.com/opensearch-project/OpenSearch/pull/15016))
- Add ThreadContextPermission for stashAndMergeHeaders and stashWithOrigin ([#15039](https://github.com/opensearch-project/OpenSearch/pull/15039))
- [Concurrent Segment Search] Support composite aggregations with scripting ([#15072](https://github.com/opensearch-project/OpenSearch/pull/15072))
- Add `rangeQuery` and `regexpQuery` for `constant_keyword` field type ([#14711](https://github.com/opensearch-project/OpenSearch/pull/14711))
- Add took time to request nodes stats ([#15054](https://github.com/opensearch-project/OpenSearch/pull/15054))
- [Workload Management] Add Get QueryGroup API Logic ([14709](https://github.com/opensearch-project/OpenSearch/pull/14709))
- [Workload Management] Add Settings for Workload Management feature ([#15028](https://github.com/opensearch-project/OpenSearch/pull/15028))
- [Workload Management] QueryGroup resource tracking framework changes ([#13897](https://github.com/opensearch-project/OpenSearch/pull/13897))
- Support filtering on a large list encoded by bitmap ([#14774](https://github.com/opensearch-project/OpenSearch/pull/14774))
- Add slice execution listeners to SearchOperationListener interface ([#15153](https://github.com/opensearch-project/OpenSearch/pull/15153))
- Adding access to noSubMatches and noOverlappingMatches in Hyphenation ([#13895](https://github.com/opensearch-project/OpenSearch/pull/13895))
- Add index creation using the context field ([#15290](https://github.com/opensearch-project/OpenSearch/pull/15290))

### Dependencies
- Bump `netty` from 4.1.111.Final to 4.1.112.Final ([#15081](https://github.com/opensearch-project/OpenSearch/pull/15081))
- Bump `org.apache.commons:commons-lang3` from 3.14.0 to 3.16.0 ([#14861](https://github.com/opensearch-project/OpenSearch/pull/14861), [#15205](https://github.com/opensearch-project/OpenSearch/pull/15205))
- OpenJDK Update (July 2024 Patch releases) ([#14998](https://github.com/opensearch-project/OpenSearch/pull/14998))
- Bump `com.microsoft.azure:msal4j` from 1.16.1 to 1.16.2 ([#14995](https://github.com/opensearch-project/OpenSearch/pull/14995))
- Bump `actions/github-script` from 6 to 7 ([#14997](https://github.com/opensearch-project/OpenSearch/pull/14997))
- Bump `org.tukaani:xz` from 1.9 to 1.10 ([#15110](https://github.com/opensearch-project/OpenSearch/pull/15110))
- Bump `actions/setup-java` from 1 to 4 ([#15104](https://github.com/opensearch-project/OpenSearch/pull/15104))
- Bump `org.apache.avro:avro` from 1.11.3 to 1.12.0 in /plugins/repository-hdfs ([#15119](https://github.com/opensearch-project/OpenSearch/pull/15119))
- Bump `org.bouncycastle:bcpg-fips` from 1.0.7.1 to 2.0.9 ([#15103](https://github.com/opensearch-project/OpenSearch/pull/15103), [#15299](https://github.com/opensearch-project/OpenSearch/pull/15299))
- Bump `com.azure:azure-core` from 1.49.1 to 1.51.0 ([#15111](https://github.com/opensearch-project/OpenSearch/pull/15111))
- Bump `org.xerial.snappy:snappy-java` from 1.1.10.5 to 1.1.10.6 ([#15207](https://github.com/opensearch-project/OpenSearch/pull/15207))
- Bump `com.azure:azure-xml` from 1.0.0 to 1.1.0 ([#15206](https://github.com/opensearch-project/OpenSearch/pull/15206))
- Bump `reactor` from 3.5.19 to 3.5.20 ([#15262](https://github.com/opensearch-project/OpenSearch/pull/15262))
- Bump `reactor-netty` from 1.1.21 to 1.1.22 ([#15262](https://github.com/opensearch-project/OpenSearch/pull/15262))
- Bump `org.apache.kerby:kerb-admin` from 2.0.3 to 2.1.0 ([#15301](https://github.com/opensearch-project/OpenSearch/pull/15301))
- Bump `com.azure:azure-core-http-netty` from 1.15.1 to 1.15.3 ([#15300](https://github.com/opensearch-project/OpenSearch/pull/15300))
- Bump `com.gradle.develocity` from 3.17.6 to 3.18 ([#15297](https://github.com/opensearch-project/OpenSearch/pull/15297))
- Bump `commons-cli:commons-cli` from 1.8.0 to 1.9.0 ([#15298](https://github.com/opensearch-project/OpenSearch/pull/15298))

### Changed
- Add lower limit for primary and replica batch allocators timeout ([#14979](https://github.com/opensearch-project/OpenSearch/pull/14979))
- Optimize regexp-based include/exclude on aggregations when pattern matches prefixes ([#14371](https://github.com/opensearch-project/OpenSearch/pull/14371))
- Replace and block usages of org.apache.logging.log4j.util.Strings ([#15238](https://github.com/opensearch-project/OpenSearch/pull/15238))

### Deprecated

### Removed

### Fixed
- Fix constraint bug which allows more primary shards than average primary shards per index ([#14908](https://github.com/opensearch-project/OpenSearch/pull/14908))
- Fix NPE when bulk ingest with empty pipeline ([#15033](https://github.com/opensearch-project/OpenSearch/pull/15033))
- Fix missing value of FieldSort for unsigned_long ([#14963](https://github.com/opensearch-project/OpenSearch/pull/14963))
- Fix delete index template failed when the index template matches a data stream but is unused ([#15080](https://github.com/opensearch-project/OpenSearch/pull/15080))
- Fix array_index_out_of_bounds_exception when indexing documents with field name containing only dot ([#15126](https://github.com/opensearch-project/OpenSearch/pull/15126))
- Fixed array field name omission in flat_object function for nested JSON ([#13620](https://github.com/opensearch-project/OpenSearch/pull/13620))
- Fix range aggregation optimization ignoring top level queries ([#15194](https://github.com/opensearch-project/OpenSearch/pull/15194))
- Fix incorrect parameter names in MinHash token filter configuration handling ([#15233](https://github.com/opensearch-project/OpenSearch/pull/15233))

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.15...2.x
