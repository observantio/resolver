# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Removed VictoriaMetrics connector support from resolver runtime wiring, including datasource factory/provider paths and related configuration flow.
- Deleted the dedicated VictoriaMetrics connector module (`connectors/victoria.py`) and updated connector exports/import paths accordingly.
- Updated resolver bootstrap and analysis configuration paths to align with the post-Victoria connector set.
- Refreshed and expanded resolver test coverage across API surface, datasource factory/helpers, connector security, health checks, and main readiness for the new connector baseline.

## [v0.0.1] - 2026-03-20

### Added

- Added a tag-driven release workflow so creating a git tag like `vX.Y.Z` publishes a matching versioned image.
- Added GitHub Release creation as part of the same release workflow.
- Added multi-architecture image publishing (`linux/amd64`, `linux/arm64`) for release tags.

### Changed

- Replaced the previous CI-follow publishing logic with a release-tag-first flow.
- Standardized image output tags to semantic release versions (`ghcr.io/<owner>/resolver:vX.Y.Z`).

### Notes

- This service now uses git tags as release boundaries.
- For platform-wide releases, keep this tag aligned with the version used by the main repository manifest.
