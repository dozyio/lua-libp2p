local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log")
local multiaddr = require("lua_libp2p.multiaddr")
local autonat_proto = require("lua_libp2p.protocol.autonat_v2")

local M = {}

local Client = {}
Client.__index = Client

local function copy_list(values)
  local out = {}
  for i, value in ipairs(values or {}) do
    out[i] = value
  end
  return out
end

local function nonce()
  local hi = math.random(0, 0x1fffff)
  local lo = math.random(0, 0x7fffffff)
  return hi * 0x80000000 + lo
end

local function emit_event(host, name, payload)
  if not (host and type(host.emit) == "function") then
    return true
  end
  local ok, err = host:emit(name, payload)
  if not ok then
    log.warn("autonat event handler failed", {
      event = name,
      cause = tostring(err),
    })
  end
  return true
end

local function peer_id_from_target(target)
  if type(target) == "string" then
    if target:sub(1, 1) ~= "/" then
      return target
    end
    local parsed = multiaddr.parse(target)
    if parsed then
      for i = #parsed.components, 1, -1 do
        if parsed.components[i].protocol == "p2p" then
          return parsed.components[i].value
        end
      end
    end
  elseif type(target) == "table" then
    return target.peer_id or target.peerId
  end
  return nil
end

local function candidate_addrs(host, opts)
  local options = opts or {}
  local source = options.addrs
  if source == nil and host and host.address_manager and type(host.address_manager.get_observed_addrs) == "function" then
    source = host.address_manager:get_observed_addrs()
  end
  if source == nil and host and type(host.get_multiaddrs_raw) == "function" then
    source = host:get_multiaddrs_raw()
  end

  local out = {}
  for _, addr in ipairs(source or {}) do
    if type(addr) == "string" and addr ~= "" then
      if multiaddr.is_relay_addr(addr) and not options.allow_relay_addrs then
        goto continue_addr
      end
      if options.allow_private_addrs or not multiaddr.is_private_addr(addr) then
        out[#out + 1] = addr
      end
    end
    ::continue_addr::
  end
  return out
end

local function encode_addrs(addrs)
  local out = {}
  for _, addr in ipairs(addrs or {}) do
    local bytes, err = multiaddr.to_bytes(addr)
    if not bytes then
      return nil, err
    end
    out[#out + 1] = bytes
  end
  return out
end

local function response_name(map, code)
  for name, value in pairs(map) do
    if value == code then
      return name
    end
  end
  return tostring(code)
end

function Client:_record_result(result)
  local addr = result.addr
  if addr and self.host and self.host.address_manager and type(self.host.address_manager.set_reachability) == "function" then
    self.host.address_manager:set_reachability(addr, {
      status = result.reachable and "public" or "private",
      source = "autonat_v2",
      type = result.type or "observed",
      verified = result.reachable == true,
      checked_at = result.checked_at,
      server_peer_id = result.server_peer_id,
      response_status = result.response_status,
      dial_status = result.dial_status,
    })
  end
  self.results[addr or tostring(result.nonce)] = result
end

function Client:_handle_dial_data_request(stream, request)
  if self.allow_dial_data == false then
    return nil, error_mod.new("permission", "autonat dial data rejected")
  end
  local total = tonumber(request.numBytes) or 0
  if total > self.max_dial_data_bytes then
    return nil, error_mod.new("permission", "autonat dial data request too large", {
      requested = total,
      max = self.max_dial_data_bytes,
    })
  end
  local chunk_size = math.min(self.dial_data_chunk_size, autonat_proto.DIAL_DATA_CHUNK_SIZE)
  local chunk = string.rep("\0", chunk_size)
  local sent = 0
  while sent < total do
    local n = math.min(chunk_size, total - sent)
    local ok, err = autonat_proto.write_message(stream, {
      dialDataResponse = { data = chunk:sub(1, n) },
    })
    if not ok then
      return nil, err
    end
    sent = sent + n
  end
  return true
end

function Client:check(server, opts)
  local options = opts or {}
  local addrs = candidate_addrs(self.host, options)
  if #addrs == 0 then
    return nil, error_mod.new("state", "autonat check requires at least one public candidate address")
  end
  local encoded_addrs, addrs_err = encode_addrs(addrs)
  if not encoded_addrs then
    return nil, addrs_err
  end
  if not (self.host and type(self.host.new_stream) == "function") then
    return nil, error_mod.new("state", "autonat client requires host:new_stream")
  end

  local request_nonce = options.nonce or nonce()
  self._pending[request_nonce] = {
    server_peer_id = peer_id_from_target(server),
    addrs = copy_list(addrs),
    started_at = os.time(),
  }

  local stream, selected, _, stream_err = self.host:new_stream(server, { autonat_proto.DIAL_REQUEST_ID }, options.stream_opts)
  if not stream then
    self._pending[request_nonce] = nil
    return nil, stream_err
  end

  local wrote, write_err = autonat_proto.write_message(stream, {
    dialRequest = {
      addrs = encoded_addrs,
      nonce = request_nonce,
    },
  })
  if not wrote then
    self._pending[request_nonce] = nil
    pcall(function() stream:close() end)
    return nil, write_err
  end

  local response
  while true do
    local message, read_err = autonat_proto.read_message(stream, {
      max_message_size = self.max_message_size,
    })
    if not message then
      self._pending[request_nonce] = nil
      pcall(function() stream:close() end)
      return nil, read_err
    end
    if message.dialDataRequest then
      local ok, err = self:_handle_dial_data_request(stream, message.dialDataRequest)
      if not ok then
        self._pending[request_nonce] = nil
        pcall(function() stream:close() end)
        return nil, err
      end
    elseif message.dialResponse then
      response = message.dialResponse
      break
    else
      self._pending[request_nonce] = nil
      pcall(function() stream:close() end)
      return nil, error_mod.new("protocol", "unexpected autonat response message")
    end
  end

  pcall(function() stream:close() end)
  self._pending[request_nonce] = nil

  if response.status ~= autonat_proto.RESPONSE_STATUS.OK then
    local failed = {
      server_peer_id = peer_id_from_target(server),
      nonce = request_nonce,
      selected_protocol = selected,
      response_status = response_name(autonat_proto.RESPONSE_STATUS, response.status),
      dial_status = response_name(autonat_proto.DIAL_STATUS, response.dialStatus),
      checked_at = os.time(),
      reachable = false,
    }
    emit_event(self.host, "autonat:request:failed", failed)
    return failed
  end

  local addr_index = (response.addrIdx or 0) + 1
  local result = {
    server_peer_id = peer_id_from_target(server),
    nonce = request_nonce,
    selected_protocol = selected,
    addr = addrs[addr_index],
    addr_index = response.addrIdx,
    response_status = response_name(autonat_proto.RESPONSE_STATUS, response.status),
    dial_status = response_name(autonat_proto.DIAL_STATUS, response.dialStatus),
    checked_at = os.time(),
    reachable = response.dialStatus == autonat_proto.DIAL_STATUS.OK,
    dial_back_verified = self._verified[request_nonce] == true,
    type = options.type or "observed",
  }
  self:_record_result(result)
  emit_event(self.host, "autonat:address:checked", result)
  if result.reachable then
    emit_event(self.host, "autonat:address:reachable", result)
  else
    emit_event(self.host, "autonat:address:unreachable", result)
  end
  self._verified[request_nonce] = nil
  return result
end

function Client:start_check(server, opts)
  if not (self.host and type(self.host.spawn_task) == "function") then
    return nil, error_mod.new("state", "autonat start_check requires host task scheduler")
  end
  local options = opts or {}
  return self.host:spawn_task("autonat.check", function(ctx)
    local check_opts = {}
    for k, v in pairs(options) do
      check_opts[k] = v
    end
    check_opts.stream_opts = check_opts.stream_opts or {}
    check_opts.stream_opts.ctx = check_opts.stream_opts.ctx or ctx
    return self:check(server, check_opts)
  end, {
    service = "autonat",
  })
end

function Client:_handle_dial_back(stream)
  local message, read_err = autonat_proto.read_dial_back(stream, {
    max_message_size = self.max_message_size,
  })
  if not message then
    return nil, read_err
  end
  if not self._pending[message.nonce] then
    return nil, error_mod.new("protocol", "autonat dial-back nonce not pending")
  end
  self._verified[message.nonce] = true
  local ok, write_err = autonat_proto.write_dial_back_response(stream, {
    status = autonat_proto.DIAL_BACK_STATUS.OK,
  })
  if not ok then
    return nil, write_err
  end
  if type(stream.close) == "function" then
    pcall(function() stream:close() end)
  end
  return true
end

function Client:start()
  if self.started then
    return true
  end
  if not (self.host and type(self.host.handle) == "function") then
    return nil, error_mod.new("state", "autonat service requires host:handle")
  end
  local ok, err = self.host:handle(autonat_proto.DIAL_BACK_ID, function(stream)
    return self:_handle_dial_back(stream)
  end)
  if not ok then
    return nil, err
  end
  self.started = true
  return true
end

function Client:stop()
  self.started = false
  return true
end

function Client:status()
  local pending = 0
  for _ in pairs(self._pending) do
    pending = pending + 1
  end
  return {
    started = self.started,
    pending = pending,
    results = self.results,
  }
end

function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "autonat client requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    allow_dial_data = options.allow_dial_data ~= false,
    max_dial_data_bytes = options.max_dial_data_bytes or (100 * 1024),
    dial_data_chunk_size = options.dial_data_chunk_size or autonat_proto.DIAL_DATA_CHUNK_SIZE,
    max_message_size = options.max_message_size or autonat_proto.MAX_MESSAGE_SIZE,
    _pending = {},
    _verified = {},
    results = {},
    started = false,
  }, Client)
end

M.Client = Client
M.DIAL_REQUEST_ID = autonat_proto.DIAL_REQUEST_ID
M.DIAL_BACK_ID = autonat_proto.DIAL_BACK_ID

return M
