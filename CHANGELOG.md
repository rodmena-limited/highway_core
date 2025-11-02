# Changelog

All notable changes to Highway Core will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.6] - 2025-02-02
### Added
- Tier 4: Persistence and Resumability - Workflow state is saved after each task execution
- Pydantic models for robust data validation and serialization
- Support for workflow resumption with run IDs
- Bulkhead pattern for operation isolation
- Comprehensive operator support: task, condition, parallel, wait, while, foreach
- State management with variables, results and memory

### Changed
- Refactored WorkflowState to be a Pydantic model for better serialization
- Improved orchestrator to handle completed task tracking
- Enhanced persistence manager interface
- Updated execution flow to properly support workflow resumability

### Fixed
- Type hinting issues resolved for mypy compatibility
- Orchestrator initialization logic corrected
- Memory tool access patterns updated

## [0.0.5] - 2025-01-28
### Added
- Parallel execution support with ThreadPoolExecutor
- Operator handlers for condition, parallel, wait, while, foreach operations
- Tool registry for dynamic function discovery
- Comprehensive test suite for all operators

### Changed
- Improved error handling and logging
- Optimized task execution flow

## [0.0.4] - 2025-01-25
### Added
- Initial task execution functionality
- Basic workflow definition and execution engine
- YAML parsing and validation
- Basic tool implementations (log, memory, etc.)

### Changed
- Initial implementation of workflow execution engine