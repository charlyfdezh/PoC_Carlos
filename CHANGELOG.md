# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial setup of the `CHANGELOG.md` file to track future changes.

---

## [1.0.0] - 2025-03-21

### Added

- Initial implementation of the Contoso PoC project.
- `validate_data` method in `validator.py` to validate data with `reject_all`  strategy.


### Changed

- Optimized `validate_data` to read the metadata file only once and propagate it to other methods.
- Updated `validate_data_types` to handle custom validations.

### Fixed

- Fixed errors in data type validation in `validator_test.py`.

---

## [0.1.0] - 2025-03-18

### Added

- Initial setup of the project with PySpark and Delta Lake.
- Basic methods for reading and writing data in `reader_writer_config.py`.
