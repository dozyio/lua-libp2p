local address_manager = require("lua_libp2p.address_manager")
local autonat_client = require("lua_libp2p.autonat.client")
local autonat_proto = require("lua_libp2p.protocol.autonat_v2")

local function stream_from_messages(messages)
  local reads = {}
  for _, message in ipairs(messages or {}) do
    local chunks = {}
    local stream = {
      writes = chunks,
      write = function(_, data)
        chunks[#chunks + 1] = data
        return true
      end,
    }
    assert(autonat_proto.write_message(stream, message))
    reads[#reads + 1] = table.concat(chunks)
  end
  local input = table.concat(reads)
  local pos = 1
  return {
    writes = {},
    closed = false,
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

local function dial_back_stream(nonce)
  local chunks = {}
  local writer = {
    write = function(_, data)
      chunks[#chunks + 1] = data
      return true
    end,
  }
  assert(autonat_proto.write_dial_back(writer, { nonce = nonce }))
  local input = table.concat(chunks)
  local pos = 1
  return {
    writes = {},
    read = function(_, n)
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

local function fake_host(response_stream)
  local handlers = {}
  return {
    address_manager = address_manager.new({
      observed_addrs = { "/ip4/203.0.113.10/tcp/4001" },
    }),
    _handlers = handlers,
    events = {},
    handle = function(self, protocol_id, handler)
      self._handlers[protocol_id] = handler
      return true
    end,
    emit = function(self, event_name, payload)
      self.events[#self.events + 1] = { name = event_name, payload = payload }
      return true
    end,
    new_stream = function(self, target, protocols)
      self.last_target = target
      self.last_protocols = protocols
      return response_stream, protocols[1], nil, nil
    end,
    spawn_task = function(self, name, fn, opts)
      local task = {
        id = 1,
        name = name,
        service = opts and opts.service,
        status = "completed",
      }
      task.result = fn({
        checkpoint = function()
          return true
        end,
      })
      self.last_task = task
      return task
    end,
  }
end

local function run()
  local request_stream = stream_from_messages({
    {
      dialDataRequest = {
        addrIdx = 0,
        numBytes = 8,
      },
    },
    {
      dialResponse = {
        status = autonat_proto.RESPONSE_STATUS.OK,
        addrIdx = 0,
        dialStatus = autonat_proto.DIAL_STATUS.OK,
      },
    },
  })
  local host = fake_host(request_stream)
  local client = assert(autonat_client.new(host, {
    max_dial_data_bytes = 32,
  }))
  local started, start_err = client:start()
  if not started then
    return nil, start_err
  end
  if type(host._handlers[autonat_proto.DIAL_BACK_ID]) ~= "function" then
    return nil, "autonat client service should register dial-back handler"
  end

  local result, result_err = client:check("server-peer", { nonce = 42 })
  if not result then
    return nil, result_err
  end
  if not result.reachable or result.addr ~= "/ip4/203.0.113.10/tcp/4001" then
    return nil, "autonat client should report reachable selected address"
  end
  if host.last_protocols[1] ~= autonat_proto.DIAL_REQUEST_ID then
    return nil, "autonat client should open dial-request protocol"
  end
  if #request_stream.writes < 2 then
    return nil, "autonat client should write request and dial data response"
  end
  local reachability = host.address_manager:get_reachability("/ip4/203.0.113.10/tcp/4001")
  if not reachability or reachability.status ~= "public" or reachability.type ~= "observed" then
    return nil, "autonat client should store address reachability"
  end
  if host.events[#host.events].name ~= "autonat:address:reachable" then
    return nil, "autonat client should emit reachable event"
  end

  local task_host = fake_host(stream_from_messages({
    {
      dialResponse = {
        status = autonat_proto.RESPONSE_STATUS.OK,
        addrIdx = 0,
        dialStatus = autonat_proto.DIAL_STATUS.OK,
      },
    },
  }))
  local task_client = assert(autonat_client.new(task_host))
  local check_task, check_task_err = task_client:start_check("server-peer", { nonce = 43 })
  if not check_task then
    return nil, check_task_err
  end
  if check_task.name ~= "autonat.check" or check_task.service ~= "autonat" then
    return nil, "autonat start_check should spawn service task"
  end

  client._pending[99] = { addrs = { "/ip4/203.0.113.10/tcp/4001" } }
  local back_stream = dial_back_stream(99)
  local handled, handle_err = host._handlers[autonat_proto.DIAL_BACK_ID](back_stream)
  if not handled then
    return nil, handle_err
  end
  if client._verified[99] ~= true or #back_stream.writes ~= 1 then
    return nil, "autonat client should verify dial-back nonce and respond"
  end

  local relay_only_host = fake_host(stream_from_messages({
    {
      dialResponse = {
        status = autonat_proto.RESPONSE_STATUS.OK,
        addrIdx = 0,
        dialStatus = autonat_proto.DIAL_STATUS.OK,
      },
    },
  }))
  relay_only_host.address_manager = address_manager.new({
    relay_addrs = { "/ip4/203.0.113.11/tcp/4001/p2p/relay/p2p-circuit/p2p/self" },
  })
  local relay_only_client = assert(autonat_client.new(relay_only_host))
  local skipped, skipped_err = relay_only_client:check("server-peer", { nonce = 100 })
  if skipped or not skipped_err then
    return nil, "autonat client should skip relay addrs by default"
  end

  return true
end

return {
  name = "autonat v2 client service",
  run = run,
}
