--- TLS transport handshake and secure channel.
---@class Libp2pTlsHandshakeOptions
---@field identity_keypair Libp2pIdentityKeypair
---@field expected_remote_peer_id? string
---@field muxer_protocols? string[]
---@field ctx? table

---@class Libp2pTlsHandshakeState
---@field remote_peer_id string
---@field remote_public_key? string
---@field selected_muxer? string
---@field used_early_muxer_negotiation? boolean

local error_mod = require("lua_libp2p.error")
local keys = require("lua_libp2p.crypto.keys")
local log = require("lua_libp2p.log").subsystem("tls")
local tls_verify = require("lua_libp2p.connection_encrypter_tls.verification")

local ok_ssl, ssl = pcall(require, "openssl.ssl")
local ok_ssl_context, ssl_context = pcall(require, "openssl.ssl.context")
local ok_pkey, ossl_pkey = pcall(require, "openssl.pkey")
local ok_x509, ossl_x509 = pcall(require, "openssl.x509")
local ok_x509_name, ossl_x509_name = pcall(require, "openssl.x509.name")
local ok_x509_extension, ossl_x509_extension = pcall(require, "openssl.x509.extension")
local ok_fd_tls, fd_tls = pcall(require, "fd_tls")
local ok_cqueues, cqueues = pcall(require, "cqueues")

local M = {}

M.PROTOCOL_ID = "/tls/1.0.0"
M.ALPN_FALLBACK = "libp2p"
M.CERT_PREFIX = tls_verify.CERT_PREFIX
M.EXTENSION_OID = tls_verify.EXTENSION_OID

local function tls_ctx_new(is_server)
  if not (ok_ssl and ok_ssl_context and ok_pkey and ok_x509 and ok_x509_name and ok_x509_extension) then
    return nil, error_mod.new("unsupported", "luaossl TLS/X509 modules are required for tls support")
  end
  local ctx, ctx_err = ssl_context.new("TLS", is_server == true)
  if not ctx then
    return nil, error_mod.new("io", "failed creating TLS context", { cause = ctx_err })
  end
  ctx:setOptions(ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1 | ssl.OP_NO_TLSv1_2 | ssl.OP_NO_RENEGOTIATION)
  return ctx
end

local function build_certificate(identity_keypair)
  local cert_key, cert_key_err = ossl_pkey.new({ type = "EC", curve = "prime256v1" })
  if not cert_key then
    return nil, nil, nil, nil, error_mod.new("io", "failed generating TLS certificate key", { cause = cert_key_err })
  end

  local pubkey_proto, pubkey_err = keys.public_key_proto(identity_keypair)
  if not pubkey_proto then
    return nil, nil, nil, nil, pubkey_err
  end

  local cert_pub_pem, cert_pub_pem_err = cert_key:toPEM("public")
  if not cert_pub_pem then
    return nil,
      nil,
      nil,
      nil,
      error_mod.new("io", "failed exporting TLS certificate public key", { cause = cert_pub_pem_err })
  end
  local cert_pub_der = tls_verify.pem_body_to_der(cert_pub_pem, "PUBLIC KEY")
  if not cert_pub_der then
    return nil, nil, nil, nil, error_mod.new("io", "failed parsing TLS certificate public key")
  end

  local signature, sig_err = keys.sign(identity_keypair, M.CERT_PREFIX .. cert_pub_der)
  if not signature then
    return nil, nil, nil, nil, sig_err
  end

  local ext_der = tls_verify.encode_signed_key_extension(pubkey_proto, signature)
  local ext_hex = (ext_der:gsub(".", function(c)
    return string.format("%02X", string.byte(c))
  end))
  local extension, ext_err = ossl_x509_extension.new(M.EXTENSION_OID, "DER:" .. ext_hex)
  if not extension then
    return nil, nil, nil, nil, error_mod.new("io", "failed creating TLS identity extension", { cause = ext_err })
  end

  local name = ossl_x509_name.new()
  name:add("CN", "libp2p")
  local cert = ossl_x509.new()
  cert:setVersion(3)
  cert:setSerial(math.random(1, 0x3fffffff))
  cert:setSubject(name)
  cert:setIssuer(name)
  cert:setPublicKey(cert_key)
  cert:addExtension(extension)
  cert:setLifetime(os.time() - 3600, os.time() + 100 * 365 * 24 * 3600)
  local signed_ok, signed_err = cert:sign(cert_key, "sha256")
  if not signed_ok then
    return nil, nil, nil, nil, error_mod.new("io", "failed signing TLS certificate", { cause = signed_err })
  end
  local cert_pem, cert_pem_err = cert:toPEM()
  if not cert_pem then
    return nil, nil, nil, nil, error_mod.new("io", "failed exporting TLS certificate PEM", { cause = cert_pem_err })
  end
  local key_pem, key_pem_err = cert_key:toPEM("private")
  if not key_pem then
    return nil, nil, nil, nil, error_mod.new("io", "failed exporting TLS private key PEM", { cause = key_pem_err })
  end
  return cert, cert_key, cert_pem, key_pem
end

local function wait_fd_tls(raw_conn, retry_err, ctx)
  if ctx and retry_err and retry_err.kind == "want_read" and type(ctx.wait_read) == "function" then
    log.debug("fd_tls waiting for read readiness", { kind = retry_err.kind })
    return ctx:wait_read(raw_conn)
  end
  if ctx and retry_err and retry_err.kind == "want_write" and type(ctx.wait_write) == "function" then
    log.debug("fd_tls waiting for write readiness", { kind = retry_err.kind })
    return ctx:wait_write(raw_conn)
  end
  if ok_cqueues and raw_conn ~= nil then
    log.debug("fd_tls polling via cqueues", { kind = retry_err and retry_err.kind or nil })
    local ok, err = cqueues.poll(raw_conn, 0.05)
    if ok == nil and err ~= nil then
      return nil, tostring(err)
    end
    return true
  end
  return nil, "no poller available to drive fd_tls WANT_READ/WANT_WRITE"
end

local function tls_error_text(err)
  if type(err) == "table" and type(err.message) == "string" then
    return err.message
  end
  return tostring(err or "unknown error")
end

local function normalize_tls_io_error(operation, err)
  if error_mod.is_error(err) then
    return err
  end

  local text = tls_error_text(err)
  local fd_kind = type(err) == "table" and err.kind or nil
  local kind = "io"
  if fd_kind == "closed" or text == "eof" or text:find("closed", 1, true) ~= nil then
    kind = "closed"
  elseif fd_kind == "timeout" then
    kind = "timeout"
  elseif fd_kind == "verify" then
    kind = "verify"
  elseif fd_kind == "want_read" or fd_kind == "want_write" then
    kind = "busy"
  end

  return error_mod.new(kind, "fd_tls " .. operation .. " failed", {
    cause = text,
    fd_tls_kind = fd_kind,
    retryable = type(err) == "table" and err.retryable or nil,
  })
end

local function run_fd_tls_handshake(fd_conn, raw_conn, ctx)
  for _ = 1, 2000 do
    local ok, err = fd_conn:handshake_step()
    if ok then
      return true
    end
    if not err or err.retryable ~= true then
      log.debug("fd_tls handshake failed", { cause = tostring(err and err.message or err) })
      return nil, normalize_tls_io_error("handshake", err)
    end
    local _, poll_err = wait_fd_tls(raw_conn, err, ctx)
    if poll_err then
      return nil, error_mod.new("io", "failed polling TLS socket during handshake", { cause = poll_err })
    end
  end
  return nil, error_mod.new("timeout", "tls handshake exceeded iteration budget")
end

local function tls_handshake(raw_conn, is_server, opts)
  local options = opts or {}
  if raw_conn == nil then
    return nil,
      nil,
      error_mod.new(
        "unsupported",
        "tls handshake requires cqueues socket-compatible connection (starttls/checktls)",
        { protocol = M.PROTOCOL_ID }
      )
  end

  local cert, cert_key, cert_pem, key_pem, cert_err = build_certificate(options.identity_keypair)
  if not cert then
    return nil, nil, cert_err
  end

  if ok_fd_tls and raw_conn and type(raw_conn.pollfd) == "function" then
    local fd, fd_err
    if type(raw_conn.begin_fd_tls) == "function" then
      fd, fd_err = raw_conn:begin_fd_tls()
    else
      fd, fd_err = raw_conn:pollfd()
    end
    if not fd then
      return nil, nil, error_mod.new("io", "failed to read raw socket fd for fd_tls", { cause = fd_err })
    end
    log.debug("fd_tls backend selected", {
      direction = is_server and "inbound" or "outbound",
      fd = fd,
      begin_fd_tls = type(raw_conn.begin_fd_tls) == "function",
    })

    local advertised = {}
    local muxers = options.muxer_protocols
    if type(muxers) == "table" then
      for _, id in ipairs(muxers) do
        advertised[#advertised + 1] = tostring(id)
      end
    end
    advertised[#advertised + 1] = M.ALPN_FALLBACK

    local ctor = is_server and fd_tls.new_server or fd_tls.new_client
    local fd_conn, fd_conn_err = ctor(fd, {
      require_peer_cert = true,
      insecure_skip_chain_verify = true,
      close_fd_on_close = false,
      cert_pem = cert_pem,
      key_pem = key_pem,
      alpn = advertised,
    })
    if not fd_conn then
      return nil, nil, error_mod.new("io", "failed creating fd_tls connection", { cause = fd_conn_err })
    end

    local hs_ok, hs_err = run_fd_tls_handshake(fd_conn, raw_conn, options.ctx)
    if not hs_ok then
      return nil, nil, hs_err
    end
    log.debug("fd_tls handshake completed", {
      direction = is_server and "inbound" or "outbound",
    })

    local der_cert, der_err = fd_conn:get_peer_cert_der()
    if not der_cert then
      return nil, nil, error_mod.new("verify", "failed to read peer cert from fd_tls", { cause = der_err })
    end
    local peer_cert, peer_cert_err = tls_verify.der_cert_to_x509(der_cert)
    if not peer_cert then
      return nil, nil, peer_cert_err
    end
    local verified, verify_err = tls_verify.verify_peer_certificate(peer_cert, options.expected_remote_peer_id)
    if not verified then
      return nil, nil, verify_err
    end

    local selected_alpn = fd_conn:get_alpn_selected()
    if selected_alpn == M.ALPN_FALLBACK then
      selected_alpn = nil
    end

    return fd_conn,
      {
        remote_peer = verified.peer_id,
        remote_public_key = verified.public_key_data,
        remote_key_type = verified.key_type,
        selected_muxer = selected_alpn,
        used_early_muxer_negotiation = selected_alpn ~= nil,
        using_fd_tls = true,
      }
  end

  local ctx, ctx_err = tls_ctx_new(is_server)
  if not ctx then
    return nil, nil, ctx_err
  end
  ctx:setCertificate(cert)
  ctx:setPrivateKey(cert_key)
  if is_server then
    ctx:setVerify(ssl.VERIFY_FAIL_IF_NO_PEER_CERT)
  else
    ctx:setVerify(ssl.VERIFY_NONE)
  end

  local advertised = {}
  local muxers = options.muxer_protocols
  if type(muxers) == "table" then
    for _, id in ipairs(muxers) do
      advertised[#advertised + 1] = tostring(id)
    end
  end
  advertised[#advertised + 1] = M.ALPN_FALLBACK
  if type(ctx.setAlpnProtos) == "function" then
    ctx:setAlpnProtos(advertised)
  end

  if is_server and type(ctx.setAlpnSelect) == "function" and type(muxers) == "table" then
    ctx:setAlpnSelect(function(_, protocols)
      for _, remote_id in ipairs(protocols) do
        for _, local_id in ipairs(muxers) do
          if remote_id == local_id then
            return remote_id
          end
        end
      end
      return M.ALPN_FALLBACK
    end)
  end

  local ok_starttls, ok, hs_err = pcall(function()
    return raw_conn:starttls(ctx)
  end)
  if not ok_starttls then
    return nil,
      nil,
      error_mod.new("unsupported", "tls handshake requires cqueues socket-compatible connection (starttls)", {
        cause = ok,
      })
  end
  if not ok then
    return nil, nil, error_mod.new("verify", "tls handshake failed", { cause = hs_err })
  end

  local ok_checktls, tls_obj, tls_obj_err = pcall(function()
    return raw_conn:checktls()
  end)
  if not ok_checktls then
    return nil,
      nil,
      error_mod.new("unsupported", "tls handshake requires cqueues socket-compatible connection (checktls)", {
        cause = tls_obj,
      })
  end
  if not tls_obj then
    return nil, nil, error_mod.new("verify", "tls handshake completed without SSL object", { cause = tls_obj_err })
  end
  local peer_cert = tls_obj:getPeerCertificate()
  local verified, verify_err = tls_verify.verify_peer_certificate(peer_cert, options.expected_remote_peer_id)
  if not verified then
    return nil, nil, verify_err
  end

  local selected_alpn = nil
  if type(tls_obj.getAlpnSelected) == "function" then
    selected_alpn = tls_obj:getAlpnSelected()
  end
  if selected_alpn == M.ALPN_FALLBACK then
    selected_alpn = nil
  end

  log.debug("tls handshake completed", {
    direction = is_server and "inbound" or "outbound",
    peer_id = verified.peer_id and verified.peer_id.id or nil,
    selected_alpn = selected_alpn,
  })

  return raw_conn,
    {
      remote_peer = verified.peer_id,
      remote_public_key = verified.public_key_data,
      remote_key_type = verified.key_type,
      selected_muxer = selected_alpn,
      used_early_muxer_negotiation = selected_alpn ~= nil,
    }
end

local SecureConn = {}
SecureConn.__index = SecureConn

function SecureConn:new(raw)
  return setmetatable({ _raw = raw }, self)
end

function SecureConn:read(n)
  if type(n) ~= "number" or n <= 0 then
    return nil, error_mod.new("input", "tls secure read length must be positive")
  end
  if type(self._raw.xread) == "function" then
    local out, err = self._raw:xread(n)
    if not out and err then
      return nil, normalize_tls_io_error("read", err)
    end
    return out
  end
  if type(self._raw.read_some) == "function" and type(self._raw.get_peer_cert_der) == "function" then
    for _ = 1, 2000 do
      local out, err = self._raw:read_some(n)
      if out then
        return out
      end
      if not err or err.retryable ~= true then
        log.debug("fd_tls read failed", { cause = tostring(err and err.message or err) })
        return nil, normalize_tls_io_error("read", err)
      end
      log.debug("fd_tls read retry", { kind = err.kind })
      local _, poll_err = wait_fd_tls(self._poll_raw, err, self._ctx)
      if poll_err then
        return nil, error_mod.new("io", "failed polling fd_tls read", { cause = poll_err })
      end
    end
    return nil, error_mod.new("timeout", "fd_tls read exceeded iteration budget")
  end
  local out, err = self._raw:read(n)
  if not out and err then
    return nil, normalize_tls_io_error("read", err)
  end
  return out
end

function SecureConn:write(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "tls payload must be bytes")
  end
  if type(self._raw.xwrite) == "function" then
    local ok, err = self._raw:xwrite(payload)
    if not ok then
      return nil, normalize_tls_io_error("write", err)
    end
    return true
  end
  if type(self._raw.write_some) == "function" and type(self._raw.get_peer_cert_der) == "function" then
    local offset = 1
    while offset <= #payload do
      local wrote, err = self._raw:write_some(payload:sub(offset))
      if wrote and wrote > 0 then
        offset = offset + wrote
      elseif err and err.retryable == true then
        log.debug("fd_tls write retry", {
          kind = err.kind,
          offset = offset,
          remaining = #payload - offset + 1,
        })
        local _, poll_err = wait_fd_tls(self._poll_raw, err, self._ctx)
        if poll_err then
          return nil, error_mod.new("io", "failed polling fd_tls write", { cause = poll_err })
        end
      else
        log.debug("fd_tls write failed", { cause = tostring(err and err.message or err) })
        return nil, normalize_tls_io_error("write", err)
      end
    end
    return true
  end
  local ok, err = self._raw:write(payload)
  if not ok and err then
    return nil, normalize_tls_io_error("write", err)
  end
  return ok, err
end

function SecureConn:close()
  log.debug("fd_tls secure connection closing")
  local ok, err = self._raw:close()
  if self._poll_raw and self._poll_raw ~= self._raw and type(self._poll_raw.close) == "function" then
    local raw_ok, raw_err = self._poll_raw:close()
    if ok == nil then
      return ok, err
    end
    if raw_ok == nil then
      return raw_ok, raw_err
    end
  end
  return ok, err
end

function SecureConn:socket()
  if self._raw and self._raw.socket then
    return self._raw:socket()
  end
  if self._poll_raw and type(self._poll_raw.socket) == "function" then
    return self._poll_raw:socket()
  end
  return nil
end

function SecureConn:watch_luv_readable(on_readable)
  if self._raw and type(self._raw.watch_luv_readable) == "function" then
    return self._raw:watch_luv_readable(on_readable)
  end
  if self._poll_raw and type(self._poll_raw.watch_luv_readable) == "function" then
    return self._poll_raw:watch_luv_readable(on_readable)
  end
  return nil, error_mod.new("unsupported", "raw tls connection does not support luv readable watches")
end

function SecureConn:watch_luv_write(on_write)
  if self._raw and type(self._raw.watch_luv_write) == "function" then
    return self._raw:watch_luv_write(on_write)
  end
  if self._poll_raw and type(self._poll_raw.watch_luv_write) == "function" then
    return self._poll_raw:watch_luv_write(on_write)
  end
  return nil, error_mod.new("unsupported", "raw tls connection does not support luv write watches")
end

function SecureConn:set_context(ctx)
  self._ctx = ctx
  if self._raw and type(self._raw.set_context) == "function" then
    return self._raw:set_context(ctx)
  end
  return true
end

function SecureConn:local_multiaddr()
  if self._raw.local_multiaddr then
    return self._raw:local_multiaddr()
  end
  return nil
end

function SecureConn:remote_multiaddr()
  if self._raw.remote_multiaddr then
    return self._raw:remote_multiaddr()
  end
  return nil
end

---@param raw_conn table
---@param opts Libp2pTlsHandshakeOptions
---@return table|nil secure_conn
---@return Libp2pTlsHandshakeState|nil state
---@return table|nil err
function M.handshake_outbound(raw_conn, opts)
  local secure_raw, state, err = tls_handshake(raw_conn, false, opts)
  if not secure_raw then
    return nil, nil, err
  end
  local secure = SecureConn:new(secure_raw)
  secure._poll_raw = raw_conn
  secure._ctx = opts and opts.ctx or nil
  return secure, state
end

---@param raw_conn table
---@param opts Libp2pTlsHandshakeOptions
---@return table|nil secure_conn
---@return Libp2pTlsHandshakeState|nil state
---@return table|nil err
function M.handshake_inbound(raw_conn, opts)
  local secure_raw, state, err = tls_handshake(raw_conn, true, opts)
  if not secure_raw then
    return nil, nil, err
  end
  local secure = SecureConn:new(secure_raw)
  secure._poll_raw = raw_conn
  secure._ctx = opts and opts.ctx or nil
  return secure, state
end

return M
