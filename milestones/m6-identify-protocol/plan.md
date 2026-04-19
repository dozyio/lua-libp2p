# Milestone 6 Plan: Identify Protocol

## Goal
Exchange peer metadata and protocol support with `/ipfs/id/1.0.0`.

## Tasks
1. Implement identify protobuf message encode/decode.
2. Implement identify responder and requester.
3. Include addresses, protocol list, observed address, and identity fields.
4. Apply identify results to peerstore updates.
5. Add Lua<->go-libp2p identify interop tests.

## Deliverables
- `protocol/identify` module.
- Peerstore update path for identify events.
- Interop tests.

## Exit Criteria
- Lua node can request and serve identify successfully.
- Peerstore reflects received identify data.
