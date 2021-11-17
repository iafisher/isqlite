# Changelog
All notable changes to isqlite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Backwards compatibility is maintained under the restrictions of Semantic Versioning with regards to the following interfaces:

- All Python interfaces documented in the [API reference](https://isqlite.readthedocs.io/en/stable/api.html).
- The command-line interface of the `isqlite` tool.

Backwards compatibility is NOT guaranteed to be maintained with regards to:

- Any Python interface not documented in the API reference.
- The output of the `isqlite` command-line tool.

Numbers in parentheses after entries refer to issues in the [GitHub issue tracker](https://github.com/iafisher/isqlite/issues).


## Unreleased
### Added
- `isqlite icreate` and `isqlite iupdate` commands to interactively create and update rows.
- A `detect_renaming` parameter has been added to `Database.diff` and `Database.migrate`, and a `--no-rename` flag has been added to `isqlite diff` and `isqlite migrate`, allowing isqlite's renaming detection to be disabled in case it gives erroneous results.

### Changed
- `isqlite sql` no longer prints anything if the SQL query returned no rows. (#66)

### Fixed
- Previously, under some circumstances `Database.diff` would identify a column as renamed (e.g., `a --> b`) even though the old column (`a`) was still in the table. This has been fixed. (#67)
- `isqlite update` with `--auto-timestamp` turned on (the default) no longer updates the `created_at` timestamp field. (#68)
- `isqlite create` and `isqlite update` no longer crash when a value in a command-line argument contains an equals sign.
- `isqlite get` now prints a helpful error message instead of crashing when the row does not exist.
- `isqlite schema <table>` now prints a helpful error message when the table does not exist.


## [1.3.0] - 2021-11-14
### Added
- `Database.select`, `Database.get`, and related methods now accept a `columns` parameter to specify the set of columns to be returned for each row. (#63)
- The `Database` constructor now accepts a parameter `enforce_foreign_keys` which can be set to false to turn off foreign-key constraint enforcement.
- All `columns` constructors except `boolean` and `primary_key` now accept a `unique` parameter to enforce a SQL `UNIQUE` constraint. (#64)
- `isqlite create` as an alias for `isqlite insert` and `isqlite list` as an alias for `isqlite select`. (#61)

### Changed
- `Database.update` now returns the count of rows updated, and `Database.update_by_pk` returns a boolean indicating whether or not the row was updated.

### Fixed
- Thanks to upstream fixes in the sqliteparser library, isqlite can now handle some SQL syntax it would previously choke on, including `VARCHAR(...)` column types, multi-word column types, and `NULL` column constraints.
- The `isqlite migrate` and `isqlite diff` commands now give better error messages for invalid schema files.
- The `isqlite sql` command now gives a more informative error message when attempting to modify the database without the `--write` flag. (#62)


## [1.2.0] - 2021-10-19
### Fixed
- The PyPI package now depends on the correct version of `sqliteparser`.


## [1.1.0] - 2021-10-19
### Fixed
- `Database.update` now reads from `Database.update_auto_timestamp_columns`. It previously incorrectly read from `Database.insert_auto_timestamp_columns`.
- The auto-timestamp behavior of `isqlite create` and `isqlite update` can now be disabled with the `--no-auto-timestamp` flag.


## [1.0.0] - 2021-10-17
First official release.
