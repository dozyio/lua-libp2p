# Milestone 10 Plan: Minimal Public API + Docs

## Goal
Ship a tiny, coherent API with clear docs and a fast-start example.

## Tasks
1. Define public host API (`new_host`, `start`, `dial`, `ping`, `handle`).
2. Hide internal modules behind stable interfaces.
3. Write quickstart docs with one end-to-end example.
4. Document supported protocols and explicit non-goals.
5. Add API smoke tests and example verification script.

## Deliverables
- Public API module.
- `README` quickstart and protocol support matrix.
- Runnable example for connect + identify + ping.

## Exit Criteria
- New user can run the example and interop in under 5 minutes.
- API surface is stable and minimal.
