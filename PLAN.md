# Highway Core Refactoring Plan

## Overview
Refactor the `highway_core` library from a simple, in-memory DAG runner into a durable, multi-process, database-driven workflow engine with hybrid mode support.

## Architecture
- **Local Mode**: Existing in-memory execution (backward compatible)
- **Durable Mode**: New database-driven execution with external services
- **Hybrid Mode**: Seamless switching between modes based on workflow configuration

## Implementation Phases

### Phase 1: Database Evolution
- Modify `Workflow` model with mode column and new statuses
- Enhance `Task` model with durable waiting support (timers, events)
- Add `TaskQueue` model for durable task execution

### Phase 2: Database Manager Methods
- Add durable workflow creation and management methods
- Implement task queue operations with atomic locking
- Add methods for durable task state management

### Phase 3: Durable Services
- Create Scheduler service for workflow orchestration
- Create Worker service for task execution
- Implement service coordination and error handling

### Phase 4: Hybrid Mode Handlers
- Update Orchestrator to support both modes
- Modify executors to handle durable context injection
- Implement hybrid wait and condition handlers

### Phase 5: Human-in-the-Loop Tools
- Create human.request_approval tool for durable pauses
- Add email, database, and API tool stubs
- Implement durable task resumption via tokens

### Phase 6: Flask API
- Create REST API for durable workflow management
- Implement workflow submission and status endpoints
- Add human task resumption endpoints

### Phase 7: Test Workflow
- Create comprehensive expense report workflow example
- Demonstrate human-in-the-loop with timeouts and branching
- Test all durable features in integration

### Phase 8: CLI Updates
- Add durable mode commands to CLI
- Support service management (scheduler, worker, API)
- Maintain backward compatibility for existing commands

## Key Features
- **Durable Timers**: Non-blocking wait operations
- **Human Tasks**: External event-based workflow pauses
- **Database-Driven**: PostgreSQL-based state management
- **Multi-Process**: Independent scheduler and worker services
- **Backward Compatible**: Existing workflows continue to work unchanged

## Testing Strategy
- Verify backward compatibility with existing tests
- Test durable mode with new workflow examples
- Integration testing of all services
- Human task simulation and resumption testing

## Files to Create/Modify
- `highway_core/persistence/models.py` (enhanced)
- `highway_core/persistence/database.py` (new methods)
- `highway_core/services/` (new directory)
- `highway_core/tools/` (new human tools)
- `highway_core/api/` (new Flask API)
- `cli.py` (enhanced with durable commands)
- Test workflows and integration tests

## Success Criteria
- All existing functionality preserved in Local Mode
- Durable Mode supports long-running workflows
- Human tasks can be paused and resumed externally
- Services can be started/stopped independently
- Comprehensive test coverage for new features