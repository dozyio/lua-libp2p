local error_mod = require("lua_libp2p.error")
local connection = require("lua_libp2p.network.connection")
local mss = require("lua_libp2p.protocol.mss")
local plaintext = require("lua_libp2p.protocol.plaintext")
local yamux = require("lua_libp2p.muxer.yamux")

local M = {}

local function default_security_protocols()
  return { plaintext.PROTOCOL_ID }
end

local function default_muxer_protocols()
  return { yamux.PROTOCOL_ID }
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
    local ok, reg_err = router:register(protocol_id, function() return true end)
    if not ok then
      return nil, reg_err
    end
  end

  local protocol_id, _, neg_err = router:negotiate(conn)
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

    local local_exchange, local_exchange_err = plaintext.make_exchange_from_ed25519_public_key(keypair.public_key)
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

    return raw_conn, {
      security = selected_security,
      remote_peer_id = verified.peer_id and verified.peer_id.id or nil,
      remote_peer_id_bytes = verified.peer_id and verified.peer_id.bytes or nil,
      remote_public_key = verified.public_key_data,
      remote_key_type = verified.key_type,
    }
  end

  return nil, nil, error_mod.new("unsupported", "security protocol not supported", {
    protocol = selected_security,
  })
end

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

  local selected_muxer, mux_err = mss.select(secure_conn, muxer_protocols)
  if not selected_muxer then
    return nil, nil, mux_err
  end
  if selected_muxer ~= yamux.PROTOCOL_ID then
    return nil, nil, error_mod.new("unsupported", "muxer protocol not supported", { protocol = selected_muxer })
  end

  local conn = connection.with_yamux(secure_conn, {
    is_client = true,
    initial_stream_window = options.initial_stream_window,
    max_ack_backlog = options.max_ack_backlog,
    max_accept_backlog = options.max_accept_backlog,
  })

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

  local selected_muxer, mux_err = negotiate_inbound(secure_conn, muxer_protocols)
  if not selected_muxer then
    return nil, nil, mux_err
  end
  if selected_muxer ~= yamux.PROTOCOL_ID then
    return nil, nil, error_mod.new("unsupported", "muxer protocol not supported", { protocol = selected_muxer })
  end

  local conn = connection.with_yamux(secure_conn, {
    is_client = false,
    initial_stream_window = options.initial_stream_window,
    max_ack_backlog = options.max_ack_backlog,
    max_accept_backlog = options.max_accept_backlog,
  })

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
