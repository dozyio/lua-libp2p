# Milestone 0 Plan: Project Skeleton

## Goal
Stand up a runnable Lua project scaffold with clear module boundaries and a basic test loop.

## Tasks
1. Initialize project layout and base modules (`transport`, `security`, `muxer`, `protocol`, `crypto`, `peerstore`).
2. Decide and document runtime/tooling assumptions (Lua version, socket lib, protobuf, crypto bindings).
3. Add shared error types and minimal structured logging.
4. Add test harness and one loopback integration test using a dummy protocol.

## Deliverables
- Directory/module skeleton committed.
- `README` section with runtime assumptions.
- Basic test command and one passing integration test.

## Exit Criteria
- Fresh clone can run tests successfully.
- Dummy loopback protocol passes in CI/local.
