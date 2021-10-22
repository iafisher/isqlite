# Changelog
All notable changes to isqlite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## Unreleased
### Added
- The `Database` constructor now accepts a parameter `enforce_foreign_keys` which can be set to false to turn off foreign-key constraint enforcement.

### Changed
- `Database.update` now returns the count of rows updated, and `Database.update_by_pk` returns a boolean indicating whether or not the row was updated.


## [1.2.0] - 2021-10-19
### Fixed
- The PyPI package now depends on the correct version of `sqliteparser`.


## [1.1.0] - 2021-10-19
### Fixed
- `Database.update` now reads from `Database.update_auto_timestamp_columns`. It previously incorrectly read from `Database.insert_auto_timestamp_columns`.
- The auto-timestamp behavior of `isqlite create` and `isqlite update` can now be disabled with the `--no-auto-timestamp` flag.


## [1.0.0] - 2021-10-17
First official release.
