local autonat_server = require("lua_libp2p.autonat.server")
local autonat_v1 = require("lua_libp2p.protocol.autonat_v1")
local autonat_v2 = require("lua_libp2p.protocol.autonat_v2")
local multiaddr = require("lua_libp2p.multiaddr")

local function stream_from_v1_messages(messages)
  local reads = {}
  for _, message in ipairs(messages or {}) do
    local chunks = {}
    local stream = {
      write = function(_, data)
        chunks[#chunks + 1] = data
        return true
      end,
    }
    assert(autonat_v1.write_message(stream, message))
    reads[#reads + 1] = table.concat(chunks)
  end
  local input = table.concat(reads)
  local pos = 1
  return {
    writes = {},
    read = function(_, n)
      if pos > #input then
        return nil, "eof"
      end
      local chunk = input:sub(pos, pos + n - 1)
      pos = pos + #chunk
      return chunk
    end,
    write = function(self, data)
      self.writes[#self.writes + 1] = data
      return true
    end,
    close = function(self)
      self.closed = true
      return true
    end,
  }
end

local function stream_from_v2_messages(messages)
  local reads = {}
  for _, message in ipairs(messages or {}) do
    local chunks = {}
    local stream = {
      write = function(_, data)
        chunks[#chunks + 1] = data
        return true
      end,
    }
    assert(autonat_v2.write_message(stream, message))
    reads[#reads + 1] = table.concat(chunks)
  end
  local input = table.concat(reads)
  local pos = 1
  return {
    writes = {},
    read = function(_, n)
      if pos > #input then
        return nil, "eof"
      end
      local chunk = input:sub(pos, pos + n - 1)
      pos = pos + #chunk
      return chunk
    end,
    write = function(self, data)
      self.writes[#self.writes + 1] = data
      return true
    end,
    close = function(self)
      self.closed = true
      return true
    end,
  }
end

local function run()
  local host = {
    _handlers = {},
    handle = function(self, protocol_id, handler)
      self._handlers[protocol_id] = handler
      return true
    end,
    unhandle = function(self, protocol_id)
      self._handlers[protocol_id] = nil
      return true
    end,
    is_dialable_addr = function()
      return true
    end,
    dial = function(_, addr)
      return addr == "/ip4/203.0.113.10/tcp/4001"
    end,
    new_stream = function(self, target, protocols)
      self.last_new_stream = { target = target, protocols = protocols }
      local response = {}
      local w = {
        write = function(_, data)
          response[#response + 1] = data
          return true
        end,
      }
      assert(autonat_v2.write_dial_back_response(w, { status = autonat_v2.DIAL_BACK_STATUS.OK }))
      local response_bytes = table.concat(response)
      local read_pos = 1
      local stream = {
        write = function(_, data)
          self.last_dial_back_write = data
          return true
        end,
        read = function(_, n)
          if read_pos > #response_bytes then
            return nil, "eof"
          end
          local chunk = response_bytes:sub(read_pos, read_pos + n - 1)
          read_pos = read_pos + #chunk
          return chunk
        end,
        close = function()
          return true
        end,
      }
      return stream, protocols[1], nil, nil
    end,
  }

  local service = assert(autonat_server.new(host, { enable_v1 = true, enable_v2 = true }))
  assert(service:start())

  local addr_bytes = assert(multiaddr.to_bytes("/ip4/203.0.113.10/tcp/4001"))
  local v1_stream = stream_from_v1_messages({
    {
      type = autonat_v1.MESSAGE_TYPE.DIAL,
      dial = {
        peer = {
          addrs = { addr_bytes },
        },
      },
    },
  })
  local ok, err = host._handlers[autonat_v1.ID](v1_stream, {
    state = {
      remote_peer_id = "12D3KooWTestPeer",
      remote_addr = "/ip4/203.0.113.10/tcp/49152",
    },
  })
  if not ok then
    return nil, err
  end
  local v1_out = table.concat(v1_stream.writes)
  local v1_pos = 1
  local decoded_v1 = assert(autonat_v1.read_message({
    read = function(_, n)
      if v1_pos > #v1_out then
        return nil, "eof"
      end
      local chunk = v1_out:sub(v1_pos, v1_pos + n - 1)
      v1_pos = v1_pos + #chunk
      return chunk
    end,
  }))
  if decoded_v1.dialResponse.status ~= autonat_v1.RESPONSE_STATUS.OK then
    return nil, "autonat server should answer v1 dial request (got " .. tostring(decoded_v1.dialResponse.status) .. ")"
  end

  local v2_stream = stream_from_v2_messages({
    {
      dialRequest = {
        addrs = { addr_bytes },
        nonce = 77,
      },
    },
  })
  ok, err = host._handlers[autonat_v2.DIAL_REQUEST_ID](v2_stream, {
    state = {
      remote_peer_id = "12D3KooWTestPeer",
      remote_addr = "/ip4/203.0.113.10/tcp/49152",
    },
  })
  if not ok then
    return nil, err
  end
  local v2_out = table.concat(v2_stream.writes)
  local v2_pos = 1
  local decoded_v2 = assert(autonat_v2.read_message({
    read = function(_, n)
      if v2_pos > #v2_out then
        return nil, "eof"
      end
      local chunk = v2_out:sub(v2_pos, v2_pos + n - 1)
      v2_pos = v2_pos + #chunk
      return chunk
    end,
  }))
  if not decoded_v2.dialResponse or decoded_v2.dialResponse.status ~= autonat_v2.RESPONSE_STATUS.OK then
    return nil, "autonat server should answer v2 dial request"
  end

  local limited_host = {
    _handlers = {},
    handle = host.handle,
    unhandle = host.unhandle,
    is_dialable_addr = host.is_dialable_addr,
    dial = host.dial,
    new_stream = host.new_stream,
  }
  local limited = assert(autonat_server.new(limited_host, {
    enable_v1 = false,
    enable_v2 = true,
    max_requests_per_peer_per_window = 1,
    max_requests_per_window = 100,
  }))
  assert(limited:start())

  local v2_once = stream_from_v2_messages({
    { dialRequest = { addrs = { addr_bytes }, nonce = 88 } },
  })
  ok, err = limited_host._handlers[autonat_v2.DIAL_REQUEST_ID](v2_once, {
    state = {
      remote_peer_id = "12D3KooWRateLimited",
      remote_addr = "/ip4/203.0.113.10/tcp/49153",
    },
  })
  if not ok then
    return nil, err
  end

  local v2_twice = stream_from_v2_messages({
    { dialRequest = { addrs = { addr_bytes }, nonce = 89 } },
  })
  ok, err = limited_host._handlers[autonat_v2.DIAL_REQUEST_ID](v2_twice, {
    state = {
      remote_peer_id = "12D3KooWRateLimited",
      remote_addr = "/ip4/203.0.113.10/tcp/49154",
    },
  })
  if not ok then
    return nil, err
  end
  local limited_out = table.concat(v2_twice.writes)
  local limited_pos = 1
  local limited_resp = assert(autonat_v2.read_message({
    read = function(_, n)
      if limited_pos > #limited_out then
        return nil, "eof"
      end
      local chunk = limited_out:sub(limited_pos, limited_pos + n - 1)
      limited_pos = limited_pos + #chunk
      return chunk
    end,
  }))
  if
    not limited_resp.dialResponse or limited_resp.dialResponse.status ~= autonat_v2.RESPONSE_STATUS.E_REQUEST_REJECTED
  then
    return nil, "autonat server should rate limit v2 requests per peer"
  end

  return true
end

return {
  name = "autonat combined server service",
  run = run,
}
