--- Noise transport handshake and secure channel.
-- @module lua_libp2p.connection_encrypter_noise.protocol
local error_mod = require("lua_libp2p.error")
local keys = require("lua_libp2p.crypto.keys")
local key_pb = require("lua_libp2p.crypto.key_pb")
local log = require("lua_libp2p.log").subsystem("noise")
local peerid = require("lua_libp2p.peerid")
local varint = require("lua_libp2p.multiformats.varint")

local ok_sodium, sodium = pcall(require, "luasodium")
if not ok_sodium then
  error("luasodium is required for noise support")
end

local M = {}

M.PROTOCOL_ID = "/noise"
M.PROTOCOL_NAME = "Noise_XX_25519_ChaChaPoly_SHA256"
M.SIG_PREFIX = "noise-libp2p-static-key:"

local function read_exact(conn, n)
  if n == 0 then
    return ""
  end

  local out = {}
  local remaining = n
  while remaining > 0 do
    local chunk, err = conn:read(remaining)
    if not chunk then
      return nil, err or error_mod.new("io", "unexpected EOF")
    end
    if #chunk == 0 then
      return nil, error_mod.new("io", "unexpected empty read")
    end
    out[#out + 1] = chunk
    remaining = remaining - #chunk
  end
  return table.concat(out)
end

local function u16be(n)
  return string.char((n >> 8) & 0xFF, n & 0xFF)
end

local MAX_TRANSPORT_MESSAGE_SIZE = 0xFFFF
local CHACHA20POLY1305_TAG_SIZE = 16
-- The noise transport frame length includes ciphertext + AEAD tag.
-- Max plaintext per frame is therefore 65535 - 16 = 65519 bytes.
local MAX_PLAINTEXT_MESSAGE_SIZE = MAX_TRANSPORT_MESSAGE_SIZE - CHACHA20POLY1305_TAG_SIZE

local function parse_u16be(bytes)
  local a, b = bytes:byte(1, 2)
  return a * 256 + b
end

local function encode_len_field(field_no, value)
  local tag, tag_err = varint.encode_u64(field_no * 8 + 2)
  if not tag then
    return nil, tag_err
  end
  local len, len_err = varint.encode_u64(#value)
  if not len then
    return nil, len_err
  end
  return tag .. len .. value
end

local function skip_unknown(payload, index, wire)
  if wire == 0 then
    local _, next_i_or_err = varint.decode_u64(payload, index)
    if not next_i_or_err then
      return nil, next_i_or_err
    end
    return next_i_or_err
  end
  if wire == 1 then
    local finish = index + 7
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 64-bit protobuf field")
    end
    return finish + 1
  end
  if wire == 2 then
    local length, after_len_or_err = varint.decode_u64(payload, index)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated length-delimited protobuf field")
    end
    return finish + 1
  end
  if wire == 5 then
    local finish = index + 3
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated 32-bit protobuf field")
    end
    return finish + 1
  end
  return nil, error_mod.new("decode", "unsupported protobuf wire type", { wire = wire })
end

function M.new_static_keypair()
  local public_key, private_key = sodium.crypto_box_keypair()
  return {
    public_key = public_key,
    private_key = private_key,
  }
end

function M.make_identity_signature(identity_keypair, noise_static_public_key)
  if type(noise_static_public_key) ~= "string" or #noise_static_public_key ~= 32 then
    return nil, error_mod.new("input", "noise static public key must be 32 bytes")
  end
  return keys.sign(identity_keypair, M.SIG_PREFIX .. noise_static_public_key)
end

function M.verify_identity_signature(identity_public_key, noise_static_public_key, identity_signature, identity_key_type)
  if type(noise_static_public_key) ~= "string" or #noise_static_public_key ~= 32 then
    return nil, error_mod.new("input", "noise static public key must be 32 bytes")
  end

  local key_type = identity_key_type
  if key_type == nil and type(identity_public_key) == "table" then
    key_type = identity_public_key.type
  end

  local message = M.SIG_PREFIX .. noise_static_public_key
  return keys.verify_signature(identity_public_key, message, identity_signature, key_type)
end

function M.encode_extensions(ext)
  if ext == nil then
    return ""
  end
  if type(ext) ~= "table" then
    return nil, error_mod.new("input", "noise extensions must be a table")
  end

  local out = {}
  if ext.webtransport_certhashes then
    for _, v in ipairs(ext.webtransport_certhashes) do
      local field, err = encode_len_field(1, v)
      if not field then
        return nil, err
      end
      out[#out + 1] = field
    end
  end
  if ext.stream_muxers then
    for _, v in ipairs(ext.stream_muxers) do
      local field, err = encode_len_field(2, v)
      if not field then
        return nil, err
      end
      out[#out + 1] = field
    end
  end

  return table.concat(out)
end

function M.decode_extensions(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "noise extensions payload must be bytes")
  end

  local out = {
    webtransport_certhashes = {},
    stream_muxers = {},
  }

  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err

    if wire ~= 2 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end

    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated noise extension field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.webtransport_certhashes[#out.webtransport_certhashes + 1] = value
    elseif field_no == 2 then
      out.stream_muxers[#out.stream_muxers + 1] = value
    end

    ::continue::
  end

  return out
end

function M.encode_handshake_payload(payload)
  if type(payload) ~= "table" then
    return nil, error_mod.new("input", "noise handshake payload must be a table")
  end

  local out = {}
  if payload.identity_key ~= nil then
    local field, err = encode_len_field(1, payload.identity_key)
    if not field then
      return nil, err
    end
    out[#out + 1] = field
  end
  if payload.identity_sig ~= nil then
    local field, err = encode_len_field(2, payload.identity_sig)
    if not field then
      return nil, err
    end
    out[#out + 1] = field
  end
  if payload.extensions ~= nil then
    local encoded_ext, ext_err = M.encode_extensions(payload.extensions)
    if not encoded_ext then
      return nil, ext_err
    end
    local field, err = encode_len_field(4, encoded_ext)
    if not field then
      return nil, err
    end
    out[#out + 1] = field
  end
  return table.concat(out)
end

function M.decode_handshake_payload(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "noise handshake payload must be bytes")
  end

  local out = {}
  local i = 1
  while i <= #payload do
    local tag, next_i_or_err = varint.decode_u64(payload, i)
    if not tag then
      return nil, next_i_or_err
    end
    local field_no = math.floor(tag / 8)
    local wire = tag % 8
    i = next_i_or_err

    if wire ~= 2 then
      local next_i, skip_err = skip_unknown(payload, i, wire)
      if not next_i then
        return nil, skip_err
      end
      i = next_i
      goto continue
    end

    local length, after_len_or_err = varint.decode_u64(payload, i)
    if not length then
      return nil, after_len_or_err
    end
    local finish = after_len_or_err + length - 1
    if finish > #payload then
      return nil, error_mod.new("decode", "truncated noise handshake payload field")
    end
    local value = payload:sub(after_len_or_err, finish)
    i = finish + 1

    if field_no == 1 then
      out.identity_key = value
    elseif field_no == 2 then
      out.identity_sig = value
    elseif field_no == 4 then
      local ext, ext_err = M.decode_extensions(value)
      if not ext then
        return nil, ext_err
      end
      out.extensions = ext
    end

    ::continue::
  end

  return out
end

function M.make_handshake_payload(identity_keypair, noise_static_public_key, extensions)
  local identity_key, key_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, identity_keypair.public_key)
  if not identity_key then
    return nil, key_err
  end
  local identity_sig, sig_err = M.make_identity_signature(identity_keypair, noise_static_public_key)
  if not identity_sig then
    return nil, sig_err
  end

  return {
    identity_key = identity_key,
    identity_sig = identity_sig,
    extensions = extensions,
  }
end

function M.verify_handshake_payload(payload, noise_static_public_key, expected_remote_peer_id)
  if type(payload) ~= "table" then
    return nil, error_mod.new("input", "noise handshake payload must be a table")
  end
  if type(payload.identity_key) ~= "string" or type(payload.identity_sig) ~= "string" then
    return nil, error_mod.new("verify", "noise handshake payload missing identity fields")
  end

  local identity_pub, pub_err = key_pb.decode_public_key(payload.identity_key)
  if not identity_pub then
    return nil, pub_err
  end
  if identity_pub.type ~= key_pb.KEY_TYPE.Ed25519 and identity_pub.type ~= key_pb.KEY_TYPE.RSA and identity_pub.type ~= key_pb.KEY_TYPE.ECDSA and identity_pub.type ~= key_pb.KEY_TYPE.Secp256k1 then
    return nil, error_mod.new("unsupported", "unsupported identity key type for noise verification", {
      received_key_type = identity_pub.type,
      received_key_type_name = identity_pub.type_name,
      supported = { "ed25519", "rsa", "ecdsa", "secp256k1" },
    })
  end

  local sig_ok, sig_err = M.verify_identity_signature({
    public_key = identity_pub.data,
    data = identity_pub.data,
    type = identity_pub.type,
  }, noise_static_public_key, payload.identity_sig, identity_pub.type)
  if sig_err then
    return nil, sig_err
  end
  if not sig_ok then
    return nil, error_mod.new("verify", "noise identity signature verification failed")
  end

  local pid, pid_err = peerid.from_public_key_proto(payload.identity_key, identity_pub.type_name)
  if not pid then
    return nil, pid_err
  end

  if expected_remote_peer_id then
    local expected = peerid.parse(expected_remote_peer_id)
    local expected_bytes = expected and expected.bytes or expected_remote_peer_id
    if expected_bytes ~= pid.bytes then
      return nil, error_mod.new("verify", "noise remote peer id mismatch", {
        expected = expected and expected.id or peerid.to_base58(expected_bytes),
        received = pid.id,
      })
    end
  end

  return {
    peer_id = pid,
    identity_public_key = identity_pub,
    extensions = payload.extensions,
  }
end

function M.write_message(conn, message)
  if type(message) ~= "string" then
    return nil, error_mod.new("input", "noise message must be bytes")
  end
  if #message > MAX_TRANSPORT_MESSAGE_SIZE then
    return nil, error_mod.new("input", "noise message too large")
  end
  local ok, err = conn:write(u16be(#message) .. message)
  if not ok then
    return nil, err
  end
  return true
end

function M.read_message(conn)
  local len_bytes, len_err = read_exact(conn, 2)
  if not len_bytes then
    return nil, len_err
  end
  local length = parse_u16be(len_bytes)
  return read_exact(conn, length)
end

local function sha256(data)
  return sodium.crypto_hash_sha256(data)
end

local function hmac_sha256(key, data)
  return sodium.crypto_auth_hmacsha256(data, key)
end

local function hkdf2(chaining_key, input_key_material)
  local temp_key = hmac_sha256(chaining_key, input_key_material)
  local out1 = hmac_sha256(temp_key, string.char(0x01))
  local out2 = hmac_sha256(temp_key, out1 .. string.char(0x02))
  return out1, out2
end

local function nonce12(n)
  local lo = n & 0xFFFFFFFF
  local hi = (n >> 32) & 0xFFFFFFFF
  return string.char(
    0, 0, 0, 0,
    lo & 0xFF,
    (lo >> 8) & 0xFF,
    (lo >> 16) & 0xFF,
    (lo >> 24) & 0xFF,
    hi & 0xFF,
    (hi >> 8) & 0xFF,
    (hi >> 16) & 0xFF,
    (hi >> 24) & 0xFF
  )
end

local function cipher_encrypt(key, nonce, ad, plaintext)
  return sodium.crypto_aead_chacha20poly1305_ietf_encrypt(key, plaintext, nonce12(nonce), ad or "")
end

local function cipher_decrypt(key, nonce, ad, ciphertext)
  local plain = sodium.crypto_aead_chacha20poly1305_ietf_decrypt(key, ciphertext, nonce12(nonce), ad or "")
  if plain == nil then
    return nil, error_mod.new("verify", "noise decryption failed")
  end
  return plain
end

local function dh(private_key, public_key)
  return sodium.crypto_scalarmult(private_key, public_key)
end

local HandshakeState = {}
HandshakeState.__index = HandshakeState

--- Create Noise XX handshake state.
-- `opts.protocol_name` overrides handshake protocol label.
-- `opts.prologue` (`string`) mixes application prologue bytes.
-- `opts.initiator` (`boolean`) selects initiator/responder behavior.
-- `opts.static_keypair` supplies static DH keypair.
function HandshakeState:new(opts)
  local options = opts or {}
  local protocol_name = options.protocol_name or M.PROTOCOL_NAME
  local ck
  if #protocol_name <= 32 then
    ck = protocol_name .. string.rep("\0", 32 - #protocol_name)
  else
    ck = sha256(protocol_name)
  end
  local prologue = options.prologue or ""
  if type(prologue) ~= "string" then
    error("noise prologue must be bytes")
  end
  local h = sha256(ck .. prologue)
  return setmetatable({
    initiator = not not options.initiator,
    ck = ck,
    h = h,
    k = nil,
    n = 0,
    s = options.static_keypair,
    e = nil,
    rs = nil,
    re = nil,
  }, self)
end

function HandshakeState:mix_hash(data)
  self.h = sha256(self.h .. data)
end

function HandshakeState:mix_key(input_key_material)
  local ck, temp_k = hkdf2(self.ck, input_key_material)
  self.ck = ck
  self.k = temp_k
  self.n = 0
end

function HandshakeState:encrypt_and_hash(plaintext)
  if self.k == nil then
    self:mix_hash(plaintext)
    return plaintext
  end
  local ciphertext = cipher_encrypt(self.k, self.n, self.h, plaintext)
  self.n = self.n + 1
  self:mix_hash(ciphertext)
  return ciphertext
end

function HandshakeState:decrypt_and_hash(ciphertext)
  if self.k == nil then
    self:mix_hash(ciphertext)
    return ciphertext
  end
  local plaintext, dec_err = cipher_decrypt(self.k, self.n, self.h, ciphertext)
  if not plaintext then
    return nil, dec_err
  end
  self.n = self.n + 1
  self:mix_hash(ciphertext)
  return plaintext
end

function HandshakeState:split()
  local k1, k2 = hkdf2(self.ck, "")
  if self.initiator then
    return k1, k2
  end
  return k2, k1
end

local SecureConn = {}
SecureConn.__index = SecureConn

function SecureConn:new(raw_conn, send_key, recv_key)
  return setmetatable({
    _raw = raw_conn,
    _send_key = send_key,
    _recv_key = recv_key,
    _send_nonce = 0,
    _recv_nonce = 0,
    _recv_buf = "",
  }, self)
end

function SecureConn:read(n)
  if type(n) ~= "number" or n <= 0 then
    return nil, error_mod.new("input", "noise secure read length must be positive")
  end

  while #self._recv_buf < n do
    local msg, msg_err = M.read_message(self._raw)
    if not msg then
      return nil, msg_err
    end
    local plain, dec_err = cipher_decrypt(self._recv_key, self._recv_nonce, "", msg)
    if not plain then
      return nil, dec_err
    end
    self._recv_nonce = self._recv_nonce + 1
    self._recv_buf = self._recv_buf .. plain
  end

  local out = self._recv_buf:sub(1, n)
  self._recv_buf = self._recv_buf:sub(n + 1)
  return out
end

function SecureConn:write(payload)
  if type(payload) ~= "string" then
    return nil, error_mod.new("input", "noise payload must be bytes")
  end

  local offset = 1
  while offset <= #payload do
    local chunk_end = offset + MAX_PLAINTEXT_MESSAGE_SIZE - 1
    if chunk_end > #payload then
      chunk_end = #payload
    end

    local chunk = payload:sub(offset, chunk_end)
    local ciphertext = cipher_encrypt(self._send_key, self._send_nonce, "", chunk)
    self._send_nonce = self._send_nonce + 1

    local ok, err = M.write_message(self._raw, ciphertext)
    if not ok then
      return nil, err
    end

    offset = chunk_end + 1
  end

  return true
end

function SecureConn:close()
  return self._raw:close()
end

function SecureConn:socket()
  if self._raw and self._raw.socket then
    return self._raw:socket()
  end
  return nil
end

function SecureConn:watch_luv_readable(on_readable)
  if self._raw and type(self._raw.watch_luv_readable) == "function" then
    return self._raw:watch_luv_readable(on_readable)
  end
  return nil, error_mod.new("unsupported", "raw noise connection does not support luv readable watches")
end

function SecureConn:watch_luv_write(on_write)
  if self._raw and type(self._raw.watch_luv_write) == "function" then
    return self._raw:watch_luv_write(on_write)
  end
  return nil, error_mod.new("unsupported", "raw noise connection does not support luv write watches")
end

function SecureConn:set_context(ctx)
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

local function payload_for_side(identity_keypair, static_public, extensions)
  local payload, payload_err = M.make_handshake_payload(identity_keypair, static_public, extensions)
  if not payload then
    return nil, payload_err
  end
  return M.encode_handshake_payload(payload)
end

local function extension_summary(extensions)
  if type(extensions) ~= "table" then
    return nil
  end
  return {
    stream_muxers = #(extensions.stream_muxers or {}),
    webtransport_certhashes = #(extensions.webtransport_certhashes or {}),
  }
end

local function handshake_failed(direction, stage, err, fields)
  local context = fields or {}
  context.direction = direction
  context.stage = stage
  context.cause = tostring(err)
  log.debug("noise handshake failed", context)
  return nil, nil, err
end

--- Perform outbound Noise XX handshake.
-- `opts.identity_keypair` (required) is used for identity payload signatures.
-- `opts.static_keypair` overrides static Noise keypair.
-- `opts.expected_remote_peer_id` verifies remote identity.
-- `opts.extensions` includes optional handshake extensions.
function M.handshake_xx_outbound(raw_conn, opts)
  local options = opts or {}
  local identity = options.identity_keypair
  if not identity then
    return nil, nil, error_mod.new("input", "noise outbound handshake requires identity_keypair")
  end
  log.debug("noise handshake started", {
    direction = "outbound",
    expected_remote_peer_id = options.expected_remote_peer_id,
    extensions = extension_summary(options.extensions),
  })

  local hs = HandshakeState:new({
    initiator = true,
    static_keypair = options.static_keypair or M.new_static_keypair(),
  })

  -- msg1: -> e
  hs.e = M.new_static_keypair()
  hs:mix_hash(hs.e.public_key)
  hs:encrypt_and_hash("")
  local ok, err = M.write_message(raw_conn, hs.e.public_key)
  if not ok then
    return handshake_failed("outbound", "write_msg1", err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end

  -- msg2: <- e, ee, s, es, payload
  local msg2, msg2_err = M.read_message(raw_conn)
  if not msg2 then
    return handshake_failed("outbound", "read_msg2", msg2_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  if #msg2 < 32 + 48 + 16 then
    return handshake_failed("outbound", "decode_msg2", error_mod.new("decode", "noise msg2 too short"), {
      expected_remote_peer_id = options.expected_remote_peer_id,
      message_size = #msg2,
    })
  end
  hs.re = { public_key = msg2:sub(1, 32) }
  hs:mix_hash(hs.re.public_key)
  hs:mix_key(dh(hs.e.private_key, hs.re.public_key))

  local enc_rs = msg2:sub(33, 80)
  local rs_pub, rs_err = hs:decrypt_and_hash(enc_rs)
  if not rs_pub then
    return handshake_failed("outbound", "decrypt_msg2_static_key", error_mod.wrap("verify", "noise msg2 static key decrypt failed", rs_err), {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  hs.rs = { public_key = rs_pub }

  hs:mix_key(dh(hs.e.private_key, hs.rs.public_key))

  local enc_payload2 = msg2:sub(81)
  local payload2_bytes, payload2_err = hs:decrypt_and_hash(enc_payload2)
  if not payload2_bytes then
    return handshake_failed("outbound", "decrypt_msg2_payload", error_mod.wrap("verify", "noise msg2 payload decrypt failed", payload2_err), {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  local payload2, decode_err = M.decode_handshake_payload(payload2_bytes)
  if not payload2 then
    return handshake_failed("outbound", "decode_msg2_payload", decode_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  local verified2, verify2_err = M.verify_handshake_payload(payload2, hs.rs.public_key, options.expected_remote_peer_id)
  if not verified2 then
    return handshake_failed("outbound", "verify_msg2_payload", verify2_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  log.debug("noise remote identity verified", {
    direction = "outbound",
    peer_id = verified2.peer_id and verified2.peer_id.id or nil,
    extensions = extension_summary(verified2.extensions),
  })

  -- msg3: -> s, se, payload
  local msg3_payload_plain, msg3_payload_plain_err = payload_for_side(identity, hs.s.public_key, options.extensions)
  if not msg3_payload_plain then
    return handshake_failed("outbound", "encode_msg3_payload", msg3_payload_plain_err, {
      peer_id = verified2.peer_id and verified2.peer_id.id or nil,
    })
  end
  local enc_s = hs:encrypt_and_hash(hs.s.public_key)
  hs:mix_key(dh(hs.s.private_key, hs.re.public_key))
  local enc_payload3 = hs:encrypt_and_hash(msg3_payload_plain)
  ok, err = M.write_message(raw_conn, enc_s .. enc_payload3)
  if not ok then
    return handshake_failed("outbound", "write_msg3", err, {
      peer_id = verified2.peer_id and verified2.peer_id.id or nil,
    })
  end

  local send_key, recv_key = hs:split()
  local secure = SecureConn:new(raw_conn, send_key, recv_key)
  log.debug("noise handshake completed", {
    direction = "outbound",
    peer_id = verified2.peer_id and verified2.peer_id.id or nil,
    extensions = extension_summary(verified2.extensions),
  })
  return secure, {
    remote_peer = verified2.peer_id,
    remote_extensions = verified2.extensions,
  }
end

--- Perform inbound Noise XX handshake.
-- Uses same `opts.<field>` values as @{handshake_xx_outbound}.
function M.handshake_xx_inbound(raw_conn, opts)
  local options = opts or {}
  local identity = options.identity_keypair
  if not identity then
    return nil, nil, error_mod.new("input", "noise inbound handshake requires identity_keypair")
  end
  log.debug("noise handshake started", {
    direction = "inbound",
    expected_remote_peer_id = options.expected_remote_peer_id,
    extensions = extension_summary(options.extensions),
  })

  local hs = HandshakeState:new({
    initiator = false,
    static_keypair = options.static_keypair or M.new_static_keypair(),
  })

  -- msg1: <- e
  local msg1, msg1_err = M.read_message(raw_conn)
  if not msg1 then
    return handshake_failed("inbound", "read_msg1", msg1_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  if #msg1 ~= 32 then
    return handshake_failed("inbound", "decode_msg1", error_mod.new("decode", "noise msg1 must be 32 bytes"), {
      expected_remote_peer_id = options.expected_remote_peer_id,
      message_size = #msg1,
    })
  end
  hs.re = { public_key = msg1 }
  hs:mix_hash(hs.re.public_key)
  hs:decrypt_and_hash("")

  -- msg2: -> e, ee, s, es, payload
  hs.e = M.new_static_keypair()
  hs:mix_hash(hs.e.public_key)
  hs:mix_key(dh(hs.e.private_key, hs.re.public_key))
  local enc_s = hs:encrypt_and_hash(hs.s.public_key)
  hs:mix_key(dh(hs.s.private_key, hs.re.public_key))

  local msg2_payload_plain, msg2_payload_plain_err = payload_for_side(identity, hs.s.public_key, options.extensions)
  if not msg2_payload_plain then
    return handshake_failed("inbound", "encode_msg2_payload", msg2_payload_plain_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  local enc_payload2 = hs:encrypt_and_hash(msg2_payload_plain)
  local ok, err = M.write_message(raw_conn, hs.e.public_key .. enc_s .. enc_payload2)
  if not ok then
    return handshake_failed("inbound", "write_msg2", err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end

  -- msg3: <- s, se, payload
  local msg3, msg3_err = M.read_message(raw_conn)
  if not msg3 then
    return handshake_failed("inbound", "read_msg3", msg3_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  if #msg3 < 48 + 16 then
    return handshake_failed("inbound", "decode_msg3", error_mod.new("decode", "noise msg3 too short"), {
      expected_remote_peer_id = options.expected_remote_peer_id,
      message_size = #msg3,
    })
  end
  local enc_rs = msg3:sub(1, 48)
  local rs_pub, rs_err = hs:decrypt_and_hash(enc_rs)
  if not rs_pub then
    return handshake_failed("inbound", "decrypt_msg3_static_key", error_mod.wrap("verify", "noise msg3 static key decrypt failed", rs_err), {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  hs.rs = { public_key = rs_pub }
  hs:mix_key(dh(hs.e.private_key, hs.rs.public_key))

  local enc_payload3 = msg3:sub(49)
  local payload3_bytes, payload3_err = hs:decrypt_and_hash(enc_payload3)
  if not payload3_bytes then
    return handshake_failed("inbound", "decrypt_msg3_payload", error_mod.wrap("verify", "noise msg3 payload decrypt failed", payload3_err), {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  local payload3, decode_err = M.decode_handshake_payload(payload3_bytes)
  if not payload3 then
    return handshake_failed("inbound", "decode_msg3_payload", decode_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  local verified3, verify3_err = M.verify_handshake_payload(payload3, hs.rs.public_key, options.expected_remote_peer_id)
  if not verified3 then
    return handshake_failed("inbound", "verify_msg3_payload", verify3_err, {
      expected_remote_peer_id = options.expected_remote_peer_id,
    })
  end
  log.debug("noise remote identity verified", {
    direction = "inbound",
    peer_id = verified3.peer_id and verified3.peer_id.id or nil,
    extensions = extension_summary(verified3.extensions),
  })

  local send_key, recv_key = hs:split()
  local secure = SecureConn:new(raw_conn, send_key, recv_key)
  log.debug("noise handshake completed", {
    direction = "inbound",
    peer_id = verified3.peer_id and verified3.peer_id.id or nil,
    extensions = extension_summary(verified3.extensions),
  })
  return secure, {
    remote_peer = verified3.peer_id,
    remote_extensions = verified3.extensions,
  }
end

return M
