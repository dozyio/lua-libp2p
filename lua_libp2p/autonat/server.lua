--- AutoNAT server service supporting v1 and v2.
-- @module lua_libp2p.autonat.server
local autonat_v1 = require("lua_libp2p.protocol.autonat_v1")
local autonat_v2 = require("lua_libp2p.protocol.autonat_v2")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("autonat")
local multiaddr = require("lua_libp2p.multiaddr")
local peerid = require("lua_libp2p.peerid")

local M = {}
M.provides = { "autonat_server" }

local Server = {}
Server.__index = Server

local function now_seconds()
  local ok_socket, socket = pcall(require, "socket")
  if ok_socket and type(socket.gettime) == "function" then
    return socket.gettime()
  end
  return os.time()
end

local function prune_recent(entries, now, window_seconds)
  local out = {}
  for i = 1, #entries do
    if now - entries[i] < window_seconds then
      out[#out + 1] = entries[i]
    end
  end
  return out
end

local function random_between(min_value, max_value)
  local min_n = math.floor(tonumber(min_value) or 0)
  local max_n = math.floor(tonumber(max_value) or 0)
  if min_n < 0 then
    min_n = 0
  end
  if max_n < min_n then
    max_n = min_n
  end
  if max_n == min_n then
    return min_n
  end
  return math.random(min_n, max_n)
end

local function host_part(addr)
  local parsed = multiaddr.parse(addr)
  if not parsed or type(parsed.components) ~= "table" then
    return nil
  end
  for _, component in ipairs(parsed.components) do
    if
      component.protocol == "ip4"
      or component.protocol == "ip6"
      or component.protocol == "dns"
      or component.protocol == "dns4"
      or component.protocol == "dns6"
    then
      return component.protocol, component.value
    end
  end
  return nil
end

local function same_host(a, b)
  local ap, ah = host_part(a)
  local bp, bh = host_part(b)
  if not ap or not bp then
    return false
  end
  return ap == bp and tostring(ah) == tostring(bh)
end

local function ensure_peer_suffix(addr, peer_id)
  local parsed = multiaddr.parse(addr)
  if not parsed then
    return nil
  end
  local last = parsed.components[#parsed.components]
  if last and last.protocol == "p2p" then
    return addr
  end
  return multiaddr.encapsulate(addr, "/p2p/" .. peer_id)
end

local function stream_peer_id(ctx)
  return (ctx and ctx.peer_id)
    or (ctx and ctx.state and ctx.state.remote_peer_id)
    or (ctx and ctx.connection and ctx.connection.remote_peer_id)
end

local function stream_remote_addr(ctx)
  return (ctx and ctx.state and ctx.state.remote_addr) or (ctx and ctx.connection and ctx.connection.remote_addr)
end

local function close_quiet(stream)
  if type(stream.close) == "function" then
    pcall(function()
      stream:close()
    end)
  end
end

function Server:_dial_back_addr(addr, peer_id, opts)
  local target = ensure_peer_suffix(addr, peer_id)
  if not target then
    return nil, error_mod.new("input", "autonat target address is invalid")
  end
  local stream, _, _, conn_or_err = self.host:new_stream(target, { autonat_v2.DIAL_BACK_ID }, opts)
  if not stream then
    return nil, conn_or_err
  end
  local wrote, write_err = autonat_v2.write_dial_back(stream, { nonce = opts and opts.nonce })
  if not wrote then
    close_quiet(stream)
    return nil, write_err
  end
  local back_response, response_err = autonat_v2.read_dial_back_response(stream, {
    max_message_size = self.max_message_size,
  })
  if not back_response then
    close_quiet(stream)
    return nil, response_err
  end
  if back_response.status ~= autonat_v2.DIAL_BACK_STATUS.OK then
    close_quiet(stream)
    return nil, error_mod.new("protocol", "autonat dial-back status not ok")
  end
  close_quiet(stream)
  return true
end

function Server:_limiter_init()
  self._limiter = self._limiter or {
    global = {},
    per_peer = {},
    dial_data = {},
    in_flight = {},
  }
end

function Server:_limiter_allow(peer_id)
  self:_limiter_init()
  local now = now_seconds()
  local window = self.rate_limit_window_seconds
  local limiter = self._limiter
  limiter.global = prune_recent(limiter.global, now, window)
  limiter.dial_data = prune_recent(limiter.dial_data, now, window)
  local peer_events = prune_recent(limiter.per_peer[peer_id] or {}, now, window)
  limiter.per_peer[peer_id] = peer_events

  if self.max_concurrent_per_peer > 0 and (limiter.in_flight[peer_id] or 0) >= self.max_concurrent_per_peer then
    return false
  end
  if self.max_requests_per_window > 0 and #limiter.global >= self.max_requests_per_window then
    return false
  end
  if self.max_requests_per_peer_per_window > 0 and #peer_events >= self.max_requests_per_peer_per_window then
    return false
  end

  limiter.global[#limiter.global + 1] = now
  peer_events[#peer_events + 1] = now
  limiter.per_peer[peer_id] = peer_events
  limiter.in_flight[peer_id] = (limiter.in_flight[peer_id] or 0) + 1
  return true
end

function Server:_limiter_complete(peer_id)
  if not self._limiter then
    return true
  end
  local in_flight = (self._limiter.in_flight[peer_id] or 0) - 1
  if in_flight <= 0 then
    self._limiter.in_flight[peer_id] = nil
  else
    self._limiter.in_flight[peer_id] = in_flight
  end
  return true
end

function Server:_limiter_allow_dial_data()
  self:_limiter_init()
  local now = now_seconds()
  local window = self.rate_limit_window_seconds
  self._limiter.dial_data = prune_recent(self._limiter.dial_data, now, window)
  if
    self.max_dial_data_requests_per_window > 0 and #self._limiter.dial_data >= self.max_dial_data_requests_per_window
  then
    return false
  end
  self._limiter.dial_data[#self._limiter.dial_data + 1] = now
  return true
end

function Server:_serve_v1(stream, ctx)
  local requester_peer = stream_peer_id(ctx)
  log.debug("autonat v1 request received", {
    peer_id = requester_peer,
    remote_addr = stream_remote_addr(ctx),
  })
  if not requester_peer then
    return nil, error_mod.new("state", "missing remote peer id")
  end
  if not self:_limiter_allow(requester_peer) then
    log.debug("autonat v1 request rate limited", { peer_id = requester_peer })
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_DIAL_REFUSED,
        statusText = "rate limited",
      },
    })
    close_quiet(stream)
    return true
  end
  local function done()
    self:_limiter_complete(requester_peer)
  end

  local request, request_err = autonat_v1.read_message(stream, {
    max_message_size = self.max_message_size,
  })
  if not request then
    done()
    return nil, request_err
  end

  if request.type ~= autonat_v1.MESSAGE_TYPE.DIAL or type(request.dial) ~= "table" then
    log.debug("autonat v1 bad request", { peer_id = requester_peer, request_type = request and request.type or nil })
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_BAD_REQUEST,
        statusText = "expected dial request",
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local peer = request.dial.peer or {}
  local remote_addr = stream_remote_addr(ctx)
  if not requester_peer or not remote_addr then
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_INTERNAL_ERROR,
        statusText = "missing stream metadata",
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local requested_peer = nil
  if type(peer.id) == "string" and peer.id ~= "" then
    local parsed_peer = peerid.from_bytes(peer.id)
    if parsed_peer then
      requested_peer = parsed_peer.id
    end
  end
  if requested_peer and requested_peer ~= requester_peer then
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_BAD_REQUEST,
        statusText = "peer id mismatch",
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  if multiaddr.is_relay_addr(remote_addr) then
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_DIAL_REFUSED,
        statusText = "relayed request refused",
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local addrs = {}
  for _, raw in ipairs(peer.addrs or {}) do
    local addr = multiaddr.from_bytes(raw)
    if type(addr) == "table" then
      addr = multiaddr.format(addr)
    end
    if
      addr
      and not multiaddr.is_private_addr(addr)
      and not multiaddr.is_relay_addr(addr)
      and (not remote_addr or same_host(addr, remote_addr))
    then
      addrs[#addrs + 1] = addr
    end
  end

  if #addrs == 0 then
    autonat_v1.write_message(stream, {
      type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
      dialResponse = {
        status = autonat_v1.RESPONSE_STATUS.E_DIAL_REFUSED,
        statusText = "no dialable addresses",
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local last_addr = addrs[1]
  for _, addr in ipairs(addrs) do
    last_addr = addr
    local ok = self.host:dial(addr, { force = true, timeout = self.timeout })
    if ok then
      local addr_bytes = multiaddr.to_bytes(addr)
      autonat_v1.write_message(stream, {
        type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
        dialResponse = {
          status = autonat_v1.RESPONSE_STATUS.OK,
          addr = addr_bytes,
        },
      })
      close_quiet(stream)
      done()
      return true
    end
  end

  local addr_bytes = multiaddr.to_bytes(last_addr)
  autonat_v1.write_message(stream, {
    type = autonat_v1.MESSAGE_TYPE.DIAL_RESPONSE,
    dialResponse = {
      status = autonat_v1.RESPONSE_STATUS.E_DIAL_ERROR,
      addr = addr_bytes,
    },
  })
  close_quiet(stream)
  done()
  return true
end

function Server:_serve_v2(stream, ctx)
  local requester_peer = stream_peer_id(ctx)
  log.debug("autonat v2 request received", {
    peer_id = requester_peer,
    remote_addr = stream_remote_addr(ctx),
  })
  if not requester_peer then
    return nil, error_mod.new("state", "missing remote peer id")
  end
  if not self:_limiter_allow(requester_peer) then
    log.debug("autonat v2 request rate limited", { peer_id = requester_peer })
    autonat_v2.write_message(stream, {
      dialResponse = {
        status = autonat_v2.RESPONSE_STATUS.E_REQUEST_REJECTED,
        addrIdx = 0,
        dialStatus = autonat_v2.DIAL_STATUS.UNUSED,
      },
    })
    close_quiet(stream)
    return true
  end
  local function done()
    self:_limiter_complete(requester_peer)
  end

  local request_message, request_err = autonat_v2.read_message(stream, {
    max_message_size = self.max_message_size,
  })
  if not request_message then
    log.debug("autonat v2 read failed", { peer_id = requester_peer, cause = tostring(request_err) })
    done()
    return nil, request_err
  end
  local request = request_message.dialRequest
  if type(request) ~= "table" then
    log.debug("autonat v2 missing dialRequest", { peer_id = requester_peer })
    local _ = autonat_v2.write_message(stream, {
      dialResponse = {
        status = autonat_v2.RESPONSE_STATUS.E_INTERNAL_ERROR,
        addrIdx = 0,
        dialStatus = autonat_v2.DIAL_STATUS.UNUSED,
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local remote_addr = stream_remote_addr(ctx)
  if not requester_peer then
    log.debug("autonat v2 missing stream metadata", {
      peer_id = requester_peer,
      remote_addr = remote_addr,
    })
    local _ = autonat_v2.write_message(stream, {
      dialResponse = {
        status = autonat_v2.RESPONSE_STATUS.E_INTERNAL_ERROR,
        addrIdx = 0,
        dialStatus = autonat_v2.DIAL_STATUS.UNUSED,
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local selected_idx = nil
  local selected_addr = nil
  for i, raw in ipairs(request.addrs or {}) do
    local addr = multiaddr.from_bytes(raw)
    if type(addr) == "table" then
      addr = multiaddr.format(addr)
    end
    if addr and not multiaddr.is_private_addr(addr) and not multiaddr.is_relay_addr(addr) then
      local dialable = true
      if type(self.host.is_dialable_addr) == "function" then
        dialable = self.host:is_dialable_addr(addr) == true
      end
      if dialable then
        selected_idx = i - 1
        selected_addr = addr
        break
      end
    end
  end

  if not selected_addr then
    log.debug("autonat v2 no dialable addr", { peer_id = requester_peer, addrs = #(request.addrs or {}) })
    autonat_v2.write_message(stream, {
      dialResponse = {
        status = autonat_v2.RESPONSE_STATUS.E_DIAL_REFUSED,
        addrIdx = 0,
        dialStatus = autonat_v2.DIAL_STATUS.UNUSED,
      },
    })
    close_quiet(stream)
    done()
    return true
  end

  local requires_dial_data = (not remote_addr) or (not same_host(selected_addr, remote_addr))
  if requires_dial_data then
    local requested_dial_data_bytes = random_between(self.dial_data_min_bytes, self.dial_data_max_bytes)
    log.debug("autonat v2 dial-data required", {
      peer_id = requester_peer,
      selected_addr = selected_addr,
      remote_addr = remote_addr,
      requested_bytes = requested_dial_data_bytes,
    })
    if not self:_limiter_allow_dial_data() then
      log.debug("autonat v2 dial-data rate limited", { peer_id = requester_peer })
      autonat_v2.write_message(stream, {
        dialResponse = {
          status = autonat_v2.RESPONSE_STATUS.E_REQUEST_REJECTED,
          addrIdx = selected_idx,
          dialStatus = autonat_v2.DIAL_STATUS.UNUSED,
        },
      })
      close_quiet(stream)
      done()
      return true
    end
    local asked, asked_err = autonat_v2.write_message(stream, {
      dialDataRequest = {
        addrIdx = selected_idx,
        numBytes = requested_dial_data_bytes,
      },
    })
    if not asked then
      done()
      return nil, asked_err
    end
    local received = 0
    while received < requested_dial_data_bytes do
      local message, read_err = autonat_v2.read_message(stream, {
        max_message_size = self.max_message_size,
      })
      if not message then
        done()
        return nil, read_err
      end
      if not message.dialDataResponse or type(message.dialDataResponse.data) ~= "string" then
        done()
        return nil, error_mod.new("protocol", "expected dialDataResponse")
      end
      received = received + #message.dialDataResponse.data
    end

    local wait_max = tonumber(self.amplification_dial_wait_max_seconds) or 0
    if wait_max > 0 then
      local sleep_for = math.random() * wait_max
      if ctx and type(ctx.sleep) == "function" then
        ctx:sleep(sleep_for)
      elseif self.host and type(self.host.sleep) == "function" then
        self.host:sleep(sleep_for)
      end
    end
  end

  local dial_status = autonat_v2.DIAL_STATUS.E_DIAL_ERROR
  local dial_ok, dial_err = self:_dial_back_addr(selected_addr, requester_peer, {
    nonce = request.nonce,
    timeout = self.timeout,
  })
  if dial_ok then
    dial_status = autonat_v2.DIAL_STATUS.OK
  elseif dial_err and error_mod.is_error(dial_err) and dial_err.kind == "protocol" then
    dial_status = autonat_v2.DIAL_STATUS.E_DIAL_BACK_ERROR
  end
  log.debug("autonat v2 dial attempt completed", {
    peer_id = requester_peer,
    selected_addr = selected_addr,
    dial_status = dial_status,
    cause = dial_err and tostring(dial_err) or nil,
  })

  autonat_v2.write_message(stream, {
    dialResponse = {
      status = autonat_v2.RESPONSE_STATUS.OK,
      addrIdx = selected_idx,
      dialStatus = dial_status,
    },
  })
  close_quiet(stream)
  done()
  return true
end

function Server:start()
  if self.started then
    return true
  end
  if not (self.host and type(self.host.handle) == "function") then
    return nil, error_mod.new("state", "autonat server requires host:handle")
  end
  if self.enable_v1 then
    local ok, err = self.host:handle(autonat_v1.ID, function(stream, ctx)
      return self:_serve_v1(stream, ctx)
    end)
    if not ok then
      return nil, err
    end
  end
  if self.enable_v2 then
    local ok, err = self.host:handle(autonat_v2.DIAL_REQUEST_ID, function(stream, ctx)
      return self:_serve_v2(stream, ctx)
    end)
    if not ok then
      return nil, err
    end
  end
  self.started = true
  log.debug("autonat server started", {
    enable_v1 = self.enable_v1,
    enable_v2 = self.enable_v2,
  })
  return true
end

function Server:stop()
  if self.enable_v1 and self.host and type(self.host.unhandle) == "function" then
    self.host:unhandle(autonat_v1.ID)
  end
  if self.enable_v2 and self.host and type(self.host.unhandle) == "function" then
    self.host:unhandle(autonat_v2.DIAL_REQUEST_ID)
  end
  self.started = false
  return true
end

--- Construct a new AutoNAT server instance.
-- `opts` supports protocol toggles (`enable_v1`, `enable_v2`),
-- timeout/message sizing (`timeout`, `max_message_size`),
-- rate-limiting controls:
-- `rate_limit_window_seconds`, `max_requests_per_window`,
-- `max_requests_per_peer_per_window`, `max_dial_data_requests_per_window`,
-- and `max_concurrent_per_peer`.
-- Anti-amplification controls:
-- `dial_data_min_bytes`, `dial_data_max_bytes`,
-- optional legacy alias `dial_data_bytes`, and
-- `amplification_dial_wait_max_seconds`.
function M.new(host, opts)
  if type(host) ~= "table" then
    return nil, error_mod.new("input", "autonat server requires host")
  end
  local options = opts or {}
  return setmetatable({
    host = host,
    enable_v1 = options.enable_v1 ~= false,
    enable_v2 = options.enable_v2 ~= false,
    timeout = options.timeout or 10,
    max_message_size = options.max_message_size or autonat_v2.MAX_MESSAGE_SIZE,
    dial_data_min_bytes = options.dial_data_min_bytes or options.dial_data_bytes or 30 * 1024,
    dial_data_max_bytes = options.dial_data_max_bytes or options.dial_data_bytes or 100 * 1024,
    amplification_dial_wait_max_seconds = options.amplification_dial_wait_max_seconds or 0,
    rate_limit_window_seconds = options.rate_limit_window_seconds or 60,
    max_requests_per_window = options.max_requests_per_window or 60,
    max_requests_per_peer_per_window = options.max_requests_per_peer_per_window or 12,
    max_dial_data_requests_per_window = options.max_dial_data_requests_per_window or 12,
    max_concurrent_per_peer = options.max_concurrent_per_peer or 2,
    started = false,
  }, Server)
end

M.Server = Server

return M
