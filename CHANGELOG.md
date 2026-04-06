# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Added resolver OpenAPI middleware package initialization to support shared middleware discovery/import paths.

### Changed

- Pinned resolver runtime dependencies in `pyproject.toml` to explicit `==` versions for reproducible installs/builds.
- Applied a clean pylint reformat/refactor pass across resolver with safe line-wrapping/readability updates.
- Enforced strict naming consistency for module state, enum members, and internal variables/constants to align with configured pylint rules.
- Removed legacy uppercase alias usage from tests and aligned analyzer compatibility exports with strict snake_case lint policy.
- Reduced lint noise by aligning resolver code with stricter naming/style policy while preserving intended behavior.
- Updated resolver contract-testing scaffolding to align with middleware-based OpenAPI customization flow used across services.
- Resolved validation gaps identified by Schemathesis and fuzz-style tests; the provided verification scripts now run fully green (100%).

## [v0.0.2] - 2026-03-30

### Changed

- Removed VictoriaMetrics connector support from resolver runtime wiring, including datasource factory/provider paths and related configuration flow.
- Deleted the dedicated VictoriaMetrics connector module (`connectors/victoria.py`) and updated connector exports/import paths accordingly.
- Updated resolver bootstrap and analysis configuration paths to align with the post-Victoria connector set.
- Refreshed and expanded resolver test coverage across API surface, datasource factory/helpers, connector security, health checks, and main readiness for the new connector baseline.
- Updated pre-commit type/lint hooks to use `pyproject.toml` for mypy and pylint configuration.
- Switched resolver DB session management to a session-factory pattern with stricter initialization validation and disposal cleanup.
- Added connector header compatibility support by preferring `request_headers()` while keeping `_headers()` as legacy fallback.
- Improved engine utility robustness in weight coercion and latency timestamp normalization paths.

### Fixed

- Fixed connector/query helper compatibility for connectors exposing only `request_headers()`.
- Fixed DB-session error-path handling when `_session_factory` is non-callable or missing.

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
