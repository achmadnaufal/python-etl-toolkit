# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.2.0] - 2026-03-04
### Added
- `transform_pipeline()`: configurable step-based transform chain (normalize, deduplicate, hash)
- `deduplicate()`: key-based deduplication with removed record count tracking
- `coerce_types()`: schema-driven type coercion for int, float, str, datetime
- `add_row_hash()`: MD5 row hash for CDC (Change Data Capture) pattern
- `load_summary()`: post-pipeline summary with record count, nulls, dedup stats
- NbS project sample data with intentional duplicate for testing
- 14 unit tests covering normalization, dedup, type coercion, hashing, pipeline
### Fixed
- `extract()` alias added for semantic ETL naming
- `transform_pipeline()` raises clear error for unknown step names
## [1.1.0] - 2026-03-02
### Added
- Add async pipeline support and data lineage tracking
- Improved unit test coverage
- Enhanced documentation with realistic examples
