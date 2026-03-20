# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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

