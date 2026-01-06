# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-01-06

### Added
- **Bidirectional WebSocket Support** - Full duplex communication over WebSocket transport
  - Chat demo example showcasing real-time bidirectional RPC
- **`.map()` Implementation** - Server-side collection transformation with promise pipelining
  - `WireRemap` expressions for mapping operations
  - `MapApplicator` for executing map instructions on collections
  - TypeScript interoperability for map operations

### Changed
- **Test Reorganization** - Restructured tests into test pyramid pattern
  - `tests/a_unit/` - Unit tests for individual components
  - `tests/b_integration/` - Integration tests for component interactions
  - `tests/c_e2e/` - End-to-end tests against examples
- **Code Quality Improvements**
  - Replaced `isinstance()` chains with `match` statements throughout codebase
  - Added class-level type annotations to `RpcStub` and `RpcPromise`
  - Improved type safety with explicit casts in wire protocol handling
- **Package Structure** - Reorganized source code into logical packages

### Fixed
- Resolved all typing errors reported by `ty` and `pyrefly` type checkers
- Fixed all ruff linting errors including:
  - Converted f-string logging to lazy `%` formatting (G004)
  - Used `enumerate()` instead of manual counters (SIM113)
- Fixed `call-top-callable` error in `TargetStubHook.dispose()` using `getattr` pattern
- Fixed transport type narrowing in `PipelineBatch._ensure_transport()`

## [0.4.2] - 2025-10-23

### Added
- **WebSocket Server Endpoint** - Server now accepts WebSocket connections at `/rpc/ws`
  - Implements request-response RPC pattern (client→server calls)
  - Maintains session-specific import tables for connection lifetime
  - Automatic resource cleanup on disconnect

### Fixed
- **WebSocket endpoint registration** - Fixed bug where `/rpc/ws` was advertised but not implemented
  - Server was only registering `/rpc/batch`, causing 404 errors for WebSocket clients
  - Chat example and other WebSocket-based code now successfully connects to server

### Changed
- **WebSocket Limitation Documentation** - Clarified current WebSocket capabilities
- **Test Status** - Re-skipped chat example tests with accurate reason
- WebSocket transport supports client→server RPC but not server→client (bidirectional)
- For bidirectional RPC, use HTTP Batch or WebTransport transports
- Chat example remains non-functional pending bidirectional WebSocket implementation

## [0.4.0] - 2025-10-02

### Added
- **WebTransport/HTTP/3 Support** - Full implementation of modern transport protocol
- **Examples**
  - `examples/webtransport/` - Standalone WebTransport echo server/client
  - `examples/webtransport-integrated/` - Integrated WebTransport with full RPC
  - `examples/actor-system/` - Distributed actor system with supervisor/worker pattern
- **Tests**
  - 12 new WebTransport unit tests in `tests/test_webtransport.py`
- **Documentation**
  - Complete READMEs for all new examples
  - WebTransport vs WebSocket comparison
  - Certificate generation guides

### Changed
- **API Improvements**
  - `Client` now supports all three transports: HTTP Batch, WebSocket, and WebTransport
  - Transport auto-detection based on URL scheme and path
  - Server can run multiple transports simultaneously

## [0.3.1] - 2025-10-01

### Added
- **Comprehensive Documentation**
  - Quickstart Guide (`docs/quickstart.md`) - Get started in 5 minutes
  - Architecture Guide (`docs/architecture.md`) - Understand the hook-based design
  - API Reference (`docs/api-reference.md`) - Complete API documentation
  - Documentation Index (`docs/README.md`) - Central documentation hub
- **Comprehensive test coverage improvements across core modules**

### Changed
- Improved code quality and reduced cyclomatic complexity in 8 functions
- Refactored `TestService.call()` with dispatch table pattern
- Extracted helper methods in `PipelineBatch._execute()`
- Simplified `TargetStubHook.call()` with method extraction

### Removed
- **Legacy code cleanup** - removed monolithic evaluator and table abstractions
- Architecture now fully hook-based with no legacy code paths

## [0.3.0] - 2025-10-01

### Added
- Promise pipelining support for batching multiple dependent RPC calls
- `PipelineBatch` and `PipelinePromise` classes for client-side pipelining
- Property access on promises creates pipelined references
- TypeScript interoperability with array escaping
- 179 tests with 67% coverage

### Changed
- Replaced `isinstance()` chains with match statements throughout codebase
- All linting checks now passing (ruff + pyrefly)

## [0.2.1] - 2025-09-30

### Added
- Resume tokens for session restoration
- `ResumeToken` and `ResumeTokenManager` classes
- Bidirectional RPC (peer-to-peer capability passing)
- Server-side resume token management

### Changed
- Improved session lifecycle management

## [0.2.0] - 2025-09-30

### Added
- HTTP batch transport (client & server)
- WebSocket transport
- Transport abstraction layer
- Basic client/server implementation
- Wire protocol serialization/deserialization
- Capability dispatch and method calls
- Expression evaluator with match statements
- Remap expressions (`.map()` operations)
- Release with refcount
- Escaped literal arrays
- Promise resolution
- Error handling with custom data fields
- Security: Stack trace redaction
- 165 tests with 74% coverage

### Changed
- Complete protocol implementation matching TypeScript reference

## [0.1.0] - 2025-09-30

### Added
- Initial project structure
- Core protocol types (IDs, errors, wire format)
- Basic RPC framework
- Documentation and examples
