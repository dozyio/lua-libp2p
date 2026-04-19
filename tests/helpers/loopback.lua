local error_mod = require("lua_libp2p.error")

local function make_endpoint(inbox, peer_inbox)
  local endpoint = {
    _inbox = inbox,
    _peer_inbox = peer_inbox,
    _closed = false,
  }

  function endpoint:write(payload)
    if self._closed then
      return nil, error_mod.new("closed", "write on closed endpoint")
    end
    self._peer_inbox[#self._peer_inbox + 1] = payload
    return true
  end

  function endpoint:read()
    if self._closed then
      return nil, error_mod.new("closed", "read on closed endpoint")
    end
    if #self._inbox == 0 then
      return nil, error_mod.new("would_block", "no data available")
    end
    local payload = table.remove(self._inbox, 1)
    return payload
  end

  function endpoint:close()
    self._closed = true
    return true
  end

  return endpoint
end

local M = {}

function M.new_pair()
  local a_inbox = {}
  local b_inbox = {}

  local a = make_endpoint(a_inbox, b_inbox)
  local b = make_endpoint(b_inbox, a_inbox)

  return a, b
end

return M
