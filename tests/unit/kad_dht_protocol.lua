local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local network = require("lua_libp2p.network")
local varint = require("lua_libp2p.multiformats.varint")

local function new_scripted_conn(incoming)
  local conn = {
    _in = incoming or "",
    _out = "",
  }

  function conn:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function run()
  if kad_protocol.MAX_MESSAGE_SIZE ~= network.MESSAGE_SIZE_MAX then
    return nil, "kad-dht should default to network message size max"
  end

  local encoded, enc_err = kad_protocol.encode_message({
    type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
    key = "target-key",
    closer_peers = {
      { id = "peer-a", addrs = { "/ip4/127.0.0.1/tcp/4001" }, connection = 1 },
      { id = "peer-b" },
    },
  })
  if not encoded then
    return nil, enc_err
  end

  local decoded, dec_err = kad_protocol.decode_message(encoded)
  if not decoded then
    return nil, dec_err
  end
  if decoded.type ~= kad_protocol.MESSAGE_TYPE.FIND_NODE then
    return nil, "decoded message type mismatch"
  end
  if decoded.key ~= "target-key" then
    return nil, "decoded key mismatch"
  end
  if #decoded.closer_peers ~= 2 then
    return nil, "decoded closer peers mismatch"
  end

  local value_encoded, value_enc_err = kad_protocol.encode_message({
    type = kad_protocol.MESSAGE_TYPE.GET_VALUE,
    key = "key-a",
    record = {
      key = "key-a",
      value = "value-a",
      time_received = "2026-04-26T00:00:00Z",
    },
  })
  if not value_encoded then
    return nil, value_enc_err
  end
  local value_decoded, value_dec_err = kad_protocol.decode_message(value_encoded)
  if not value_decoded then
    return nil, value_dec_err
  end
  if not value_decoded.record or value_decoded.record.value ~= "value-a" then
    return nil, "decoded GET_VALUE record mismatch"
  end

  local providers_encoded, providers_enc_err = kad_protocol.encode_message({
    type = kad_protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = "cid-key",
    provider_peers = {
      { id = "provider-a", addrs = { "/ip4/127.0.0.1/tcp/4001" } },
    },
  })
  if not providers_encoded then
    return nil, providers_enc_err
  end
  local providers_decoded, providers_dec_err = kad_protocol.decode_message(providers_encoded)
  if not providers_decoded then
    return nil, providers_dec_err
  end
  if #providers_decoded.provider_peers ~= 1 or providers_decoded.provider_peers[1].id ~= "provider-a" then
    return nil, "decoded GET_PROVIDERS provider mismatch"
  end

  local writer = new_scripted_conn("")
  local wrote, write_err = kad_protocol.write(writer, {
    type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
    key = "target-key",
  })
  if not wrote then
    return nil, write_err
  end
  local reader = new_scripted_conn(writer:writes())
  local read_back, read_err = kad_protocol.read(reader)
  if not read_back then
    return nil, read_err
  end
  if read_back.key ~= "target-key" then
    return nil, "framed read/write key mismatch"
  end

  local oversized = string.rep("x", 33)
  local oversized_frame = assert(varint.encode_u64(#oversized)) .. oversized
  local oversized_reader = new_scripted_conn(oversized_frame)
  local oversized_msg, oversized_err = kad_protocol.read(oversized_reader, { max_message_size = 32 })
  if oversized_msg ~= nil then
    return nil, "expected oversized kad-dht frame to fail"
  end
  if not oversized_err or oversized_err.kind ~= "decode" then
    return nil, "expected decode error for oversized kad-dht frame"
  end

  local too_large_writer = new_scripted_conn("")
  local too_large_ok, too_large_err = kad_protocol.write(too_large_writer, {
    type = kad_protocol.MESSAGE_TYPE.FIND_NODE,
    key = string.rep("k", 40),
  }, { max_message_size = 16 })
  if too_large_ok ~= nil then
    return nil, "expected oversized kad-dht write to fail"
  end
  if not too_large_err or too_large_err.kind ~= "input" then
    return nil, "expected input error for oversized kad-dht write"
  end

  return true
end

return {
  name = "kad-dht protocol framing and codec",
  run = run,
}
