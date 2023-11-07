# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## [Unreleased]

### Added

- Also use `DiskFull` custom node condition to consider a node unhealthy.

## [2.0.0] - 2022-05-16

### Changed

- Upgrade dependencies.

### Added

- Add `ResetTickCounters` to  reset counters on all nodes in cluster to zero.

## [1.0.1] - 2020-10-30

## [1.0.0] - 2020-10-26

### Added

- Add github workflows for automatic releases.
- Add bad node detector code.

[Unreleased]: https://github.com/giantswarm/badnodedetector/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/giantswarm/badnodedetector/compare/v1.0.1...v2.0.0
[1.0.1]: https://github.com/giantswarm/badnodedetector/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/giantswarm/badnodedetector/releases/tag/v1.0.0
