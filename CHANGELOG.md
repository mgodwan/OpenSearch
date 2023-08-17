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
- [Remote Store] Add Segment download stats to remotestore stats API ([#8718](https://github.com/opensearch-project/OpenSearch/pull/8718))
- [Remote Store] Add remote segment transfer stats on NodesStats API ([#9168](https://github.com/opensearch-project/OpenSearch/pull/9168))
- Return 409 Conflict HTTP status instead of 503 on failure to concurrently execute snapshots ([#8986](https://github.com/opensearch-project/OpenSearch/pull/5855))
- Fix memory leak when using Zstd Dictionary ([#9403](https://github.com/opensearch-project/OpenSearch/pull/9403))
>>>>>>> 5cc73134321 (Close Zstd Dictionary after execution to avoid any memory leak. (#9403))

### Deprecated

### Removed

### Fixed

### Security

[Unreleased 2.9]: https://github.com/opensearch-project/OpenSearch/compare/2.9...2.x
