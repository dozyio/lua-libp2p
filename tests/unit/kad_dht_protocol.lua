local kad_protocol = require("lua_libp2p.kad_dht.protocol")
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

  local oversized = string.rep("x", kad_protocol.MAX_MESSAGE_SIZE + 1)
  local oversized_frame = assert(varint.encode_u64(#oversized)) .. oversized
  local oversized_reader = new_scripted_conn(oversized_frame)
  local oversized_msg, oversized_err = kad_protocol.read(oversized_reader)
  if oversized_msg ~= nil then
    return nil, "expected oversized kad-dht frame to fail"
  end
  if not oversized_err or oversized_err.kind ~= "decode" then
    return nil, "expected decode error for oversized kad-dht frame"
  end

  return true
end

return {
  name = "kad-dht protocol framing and codec",
  run = run,
}
