--- Connection upgrader for security + multiplexing.
-- Negotiates security transport then stream muxer.
-- @module lua_libp2p.network.upgrader
local error_mod = require("lua_libp2p.error")
local connection = require("lua_libp2p.network.connection")
local muxer_registry = require("lua_libp2p.muxer")
local mss = require("lua_libp2p.multistream_select.protocol")
local noise = require("lua_libp2p.connection_encrypter_noise.protocol")
local plaintext = require("lua_libp2p.connection_encrypter_plaintext.protocol")
local tls = require("lua_libp2p.connection_encrypter_tls.protocol")

local M = {}

local function default_security_protocols()
  return { noise.PROTOCOL_ID, tls.PROTOCOL_ID }
end

local function default_muxer_protocols()
  return muxer_registry.protocol_ids()
end

local function select_early_muxer(selected_security, muxer_protocols, sec_state, is_outbound)
  if selected_security ~= noise.PROTOCOL_ID then
    if selected_security == tls.PROTOCOL_ID and type(sec_state) == "table" then
      return sec_state.selected_muxer
    end
    return nil
  end
  if type(sec_state) ~= "table" or type(sec_state.noise_extensions) ~= "table" then
    return nil
  end

  local remote = sec_state.noise_extensions.stream_muxers
  if type(remote) ~= "table" or #remote == 0 then
    return nil
  end

  if is_outbound then
    for _, local_id in ipairs(muxer_protocols) do
      for _, remote_id in ipairs(remote) do
        if local_id == remote_id then
          return local_id
        end
      end
    end
    return nil
  end

  for _, remote_id in ipairs(remote) do
    for _, local_id in ipairs(muxer_protocols) do
      if local_id == remote_id then
        return remote_id
      end
    end
  end

  return nil
end

local function pick_protocol_list(value, fallback)
  if value == nil then
    return fallback
  end
  if type(value) == "string" then
    return { value }
  end
  if type(value) == "table" and #value > 0 then
    return value
  end
  return nil
end

local function negotiate_inbound(conn, supported)
  local router = mss.new_router()
  for _, protocol_id in ipairs(supported) do
    local ok, reg_err = router:register(protocol_id, function()
      return true
    end)
    if not ok then
      return nil, reg_err
    end
  end

  local protocol_id, _, _, neg_err = router:negotiate(conn)
  if not protocol_id then
    return nil, neg_err
  end
  return protocol_id
end

local function run_security_handshake(raw_conn, is_outbound, selected_security, opts)
  local options = opts or {}

  if selected_security == plaintext.PROTOCOL_ID then
    local keypair = options.local_keypair
    if type(keypair) ~= "table" or type(keypair.public_key) ~= "string" then
      return nil, nil, error_mod.new("input", "plaintext upgrader requires opts.local_keypair")
    end

    local local_exchange, local_exchange_err = plaintext.make_exchange_from_identity(keypair)
    if not local_exchange then
      return nil, nil, local_exchange_err
    end

    local expected = nil
    if is_outbound then
      expected = options.expected_remote_peer_id
    end

    local verified, handshake_err = plaintext.handshake(raw_conn, local_exchange, expected)
    if not verified then
      return nil, nil, handshake_err
    end

    return raw_conn,
      {
        security = selected_security,
        remote_peer_id = verified.peer_id and verified.peer_id.id or nil,
        remote_peer_id_bytes = verified.peer_id and verified.peer_id.bytes or nil,
        remote_public_key = verified.public_key_data,
        remote_key_type = verified.key_type,
      }
  end

  if selected_security == noise.PROTOCOL_ID then
    local secure_conn, hs_state, hs_err
    if is_outbound then
      secure_conn, hs_state, hs_err = noise.handshake_xx_outbound(raw_conn, {
        identity_keypair = options.local_keypair,
        expected_remote_peer_id = options.expected_remote_peer_id,
        extensions = {
          stream_muxers = options.muxer_protocols,
        },
      })
    else
      secure_conn, hs_state, hs_err = noise.handshake_xx_inbound(raw_conn, {
        identity_keypair = options.local_keypair,
        expected_remote_peer_id = options.expected_remote_peer_id,
        extensions = {
          stream_muxers = options.muxer_protocols,
        },
      })
    end
    if not secure_conn then
      return nil, nil, hs_err
    end

    local remote_peer = hs_state and hs_state.remote_peer

    return secure_conn,
      {
        security = selected_security,
        remote_peer_id = remote_peer and remote_peer.id or nil,
        remote_peer_id_bytes = remote_peer and remote_peer.bytes or nil,
        remote_public_key = remote_peer and remote_peer.public_key,
        remote_key_type = remote_peer and remote_peer.type,
        noise_extensions = hs_state and hs_state.remote_extensions or nil,
      }
  end

  if selected_security == tls.PROTOCOL_ID then
    local secure_conn, hs_state, hs_err
    if is_outbound then
      secure_conn, hs_state, hs_err = tls.handshake_outbound(raw_conn, {
        identity_keypair = options.local_keypair,
        expected_remote_peer_id = options.expected_remote_peer_id,
        muxer_protocols = options.muxer_protocols,
        ctx = options.ctx,
      })
    else
      secure_conn, hs_state, hs_err = tls.handshake_inbound(raw_conn, {
        identity_keypair = options.local_keypair,
        expected_remote_peer_id = options.expected_remote_peer_id,
        muxer_protocols = options.muxer_protocols,
        ctx = options.ctx,
      })
    end
    if not secure_conn then
      return nil, nil, hs_err
    end

    local remote_peer = hs_state and hs_state.remote_peer
    return secure_conn,
      {
        security = selected_security,
        remote_peer_id = remote_peer and remote_peer.id or nil,
        remote_peer_id_bytes = remote_peer and remote_peer.bytes or nil,
        remote_public_key = hs_state and hs_state.remote_public_key or nil,
        remote_key_type = hs_state and hs_state.remote_key_type or nil,
        selected_muxer = hs_state and hs_state.selected_muxer or nil,
      }
  end

  return nil,
    nil,
    error_mod.new("unsupported", "security protocol not supported", {
      protocol = selected_security,
    })
end

--- Upgrade an outbound raw transport connection.
-- @tparam table raw_conn Raw transport connection.
-- @tparam[opt] table opts Upgrader options.
-- `opts.local_keypair` (`table`): local identity keypair.
-- `opts.expected_remote_peer_id` (`string`): optional remote peer id check.
-- `opts.security_protocols` (`table<string>`): security protocol preference list.
-- `opts.muxer_protocols` (`table<string>`): muxer protocol preference list.
-- `opts.initial_stream_window` (`number`): yamux initial stream window.
-- `opts.max_ack_backlog` (`number`): yamux outbound ack backlog limit.
-- `opts.max_accept_backlog` (`number`): yamux inbound accept backlog limit.
-- @treturn table|nil conn Upgraded connection.
-- @treturn[opt] table state Negotiated metadata.
-- @treturn[opt] table err
function M.upgrade_outbound(raw_conn, opts)
  local options = opts or {}

  local security_protocols = pick_protocol_list(options.security_protocols, default_security_protocols())
  if not security_protocols then
    return nil, nil, error_mod.new("input", "invalid security_protocols option")
  end

  local muxer_protocols = pick_protocol_list(options.muxer_protocols, default_muxer_protocols())
  if not muxer_protocols then
    return nil, nil, error_mod.new("input", "invalid muxer_protocols option")
  end

  local selected_security, sec_err = mss.select(raw_conn, security_protocols)
  if not selected_security then
    return nil, nil, sec_err
  end

  local secure_conn, sec_state, handshake_err = run_security_handshake(raw_conn, true, selected_security, options)
  if not secure_conn then
    return nil, nil, handshake_err
  end

  local selected_muxer = select_early_muxer(selected_security, muxer_protocols, sec_state, true)
  if not selected_muxer then
    local mux_err
    selected_muxer, mux_err = mss.select(secure_conn, muxer_protocols)
    if not selected_muxer then
      return nil, nil, mux_err
    end
  end
  local session, session_err = muxer_registry.new_session(selected_muxer, secure_conn, {
    is_client = true,
    initial_stream_window = options.initial_stream_window,
    max_ack_backlog = options.max_ack_backlog,
    max_accept_backlog = options.max_accept_backlog,
  })
  if not session then
    return nil, nil, session_err
  end
  local conn = connection.from_session(secure_conn, session)

  local state = {
    security = sec_state.security,
    muxer = selected_muxer,
    remote_peer_id = sec_state.remote_peer_id,
    remote_peer_id_bytes = sec_state.remote_peer_id_bytes,
    remote_public_key = sec_state.remote_public_key,
    remote_key_type = sec_state.remote_key_type,
  }

  return conn, state
end

--- Upgrade an inbound raw transport connection.
-- @tparam table raw_conn Raw transport connection.
-- @tparam[opt] table opts Upgrader options.
-- Uses the same `opts.<field>` values as @{upgrade_outbound}.
-- @treturn table|nil conn
-- @treturn[opt] table state
-- @treturn[opt] table err
function M.upgrade_inbound(raw_conn, opts)
  local options = opts or {}

  local security_protocols = pick_protocol_list(options.security_protocols, default_security_protocols())
  if not security_protocols then
    return nil, nil, error_mod.new("input", "invalid security_protocols option")
  end

  local muxer_protocols = pick_protocol_list(options.muxer_protocols, default_muxer_protocols())
  if not muxer_protocols then
    return nil, nil, error_mod.new("input", "invalid muxer_protocols option")
  end

  local selected_security, sec_err = negotiate_inbound(raw_conn, security_protocols)
  if not selected_security then
    return nil, nil, sec_err
  end

  local secure_conn, sec_state, handshake_err = run_security_handshake(raw_conn, false, selected_security, options)
  if not secure_conn then
    return nil, nil, handshake_err
  end

  local selected_muxer = select_early_muxer(selected_security, muxer_protocols, sec_state, false)
  if not selected_muxer then
    local mux_err
    selected_muxer, mux_err = negotiate_inbound(secure_conn, muxer_protocols)
    if not selected_muxer then
      return nil, nil, mux_err
    end
  end
  local session, session_err = muxer_registry.new_session(selected_muxer, secure_conn, {
    is_client = false,
    initial_stream_window = options.initial_stream_window,
    max_ack_backlog = options.max_ack_backlog,
    max_accept_backlog = options.max_accept_backlog,
  })
  if not session then
    return nil, nil, session_err
  end
  local conn = connection.from_session(secure_conn, session)

  local state = {
    security = sec_state.security,
    muxer = selected_muxer,
    remote_peer_id = sec_state.remote_peer_id,
    remote_peer_id_bytes = sec_state.remote_peer_id_bytes,
    remote_public_key = sec_state.remote_public_key,
    remote_key_type = sec_state.remote_key_type,
  }

  return conn, state
end

return M
