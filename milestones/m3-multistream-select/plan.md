# Milestone 3 Plan: multistream-select

## Goal
Negotiate protocols on a connection using `/multistream/1.0.0`.

## Tasks
1. Implement varint length-delimited message framing used by multistream.
2. Implement inbound protocol handler registration and negotiation loop.
3. Implement outbound protocol selection with fallback list support.
4. Handle unsupported protocol responses (`na`) and clean failure paths.
5. Add tests for success, fallback, and unsupported protocol cases.

## Deliverables
- `protocol/mss` module.
- Negotiation tests with toy protocol IDs.

## Exit Criteria
- Peers can negotiate a selected protocol over one TCP connection.
- Failure behavior is predictable and tested.
