# Milestone 5 Plan: Yamux Multiplexing

## Goal
Support multiple logical streams over one secure connection with `/yamux/1.0.0`.

## Tasks
1. Implement yamux frame parser/serializer.
2. Implement stream lifecycle (open, data, close, reset).
3. Implement minimum viable flow control/window updates.
4. Add scheduler for concurrent stream I/O.
5. Add tests for parallel streams and close/reset edge cases.

## Deliverables
- `muxer/yamux` module.
- Multiplexing integration tests.

## Exit Criteria
- Multiple streams operate concurrently without blocking each other.
- Backpressure and reset behavior are tested.
