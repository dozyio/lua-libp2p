--- Built-in KAD-DHT value record validators/selectors.
-- @module lua_libp2p.kad_dht.record_validators
local error_mod = require("lua_libp2p.error")
local peerid = require("lua_libp2p.peerid")

local M = {}

function M.pk_peer_id_text(key)
  if type(key) ~= "string" or key:sub(1, 4) ~= "/pk/" or #key <= 4 then
    return nil, error_mod.new("input", "/pk record key must be /pk/<peer-id>")
  end
  local peer_id_part = key:sub(5)
  local parsed, parse_err = peerid.parse(peer_id_part)
  if not parsed then
    parsed, parse_err = peerid.from_bytes(peer_id_part)
  end
  if not parsed then
    return nil, parse_err
  end
  return parsed.id
end

--- Validate `/pk/<peer-id>` public key records.
-- The record value is protobuf-encoded libp2p PublicKey bytes.
function M.validate_pk(key, record)
  if type(record) ~= "table" or type(record.value) ~= "string" or record.value == "" then
    return nil, error_mod.new("input", "/pk record value must be public key protobuf bytes")
  end
  local expected_peer_id, key_err = M.pk_peer_id_text(key)
  if not expected_peer_id then
    return nil, key_err
  end
  if record.key ~= nil and record.key ~= key then
    return nil, error_mod.new("input", "/pk record key does not match record key")
  end

  local derived, derive_err = peerid.from_public_key_proto(record.value)
  if not derived then
    return nil, derive_err
  end
  if derived.id ~= expected_peer_id then
    return nil,
      error_mod.new("protocol", "/pk record public key does not match peer id", {
        expected = expected_peer_id,
        got = derived.id,
      })
  end
  return true
end

--- Select between `/pk` records for the same key.
-- Public keys are immutable for a peer id; keep the existing value when it differs.
function M.select_pk(_, existing, incoming)
  if type(existing) == "table" and type(incoming) == "table" and existing.value == incoming.value then
    return "incoming"
  end
  return "existing"
end

function M.default_validators()
  return {
    pk = M.validate_pk,
  }
end

function M.default_selectors()
  return {
    pk = M.select_pk,
  }
end

return M
