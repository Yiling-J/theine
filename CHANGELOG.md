# Changelog
All notable changes to this project will be documented in this file.

## [0.5.1]
- Bumped theine-core from ^0.4.5 to ^0.5.0. This update drops core support for Python 3.8 and adds a prebuilt wheel for Python 3.13.

## [0.5.0]
- Fixed an issue where exceptions thrown in decorated async functions could cause hangs: https://github.com/Yiling-J/theine/issues/34
- The minimum supported Python version is 3.9 now because Python3.8 already reached its EOL on 2024-10-07

## [0.4.4]
- Improve decorator typing and fix core sketch bug

## [0.4.3]
- Add license to pyproject.toml

## [0.4.2]
- Fix mypy no-implicit-reexport

## [0.4.1]
### Added
- Add close/stats API

## [0.4.0]
### Added
- Add policy option to Django settings
### Changed
- Update core API


## [0.3.3]
### Added
- Clock-PRO policy

## [0.3.2]
### Fixed
- Fix lru policy
- Fix decorator type hints

## [0.3.1]
### Fixed
- Fix async decorator
### Changed
- Optimize len() method, get len from core directly

## [0.3.0]
### Changed
- Optimize theine core, 50% throughput improve and save 30% metadata memory overhead

## [0.2.0]
### Changed
- Optimize theine core, 50% throughput improve on set and 20% throughput improve on get


## [0.1.4]
### Changed
- Cache Key can be hashable now
- Enable auto expire for auto-key decorator

## [0.1.3]
### Added
- Add decorator

### Changed
- Bump Theine-Core version
