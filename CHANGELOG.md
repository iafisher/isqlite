# Changelog
All notable changes to isqlite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## Unreleased
### Fixed
- `Database.update` now reads from `Database.update_auto_timestamp_columns`. It previously incorrectly read from `Database.insert_auto_timestamp_columns`.
- The auto-timestamp behavior of `isqlite create` and `isqlite update` can now be disabled with the `--no-auto-timestamp` flag.


## [1.0.0] - 2021-10-17
First official release.
