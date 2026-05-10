local error_mod = require("lua_libp2p.error")
local key_pb = require("lua_libp2p.crypto.key_pb")
local keys = require("lua_libp2p.crypto.keys")
local mime = require("mime")
local peerid = require("lua_libp2p.peerid")

local ossl_x509 = require("openssl.x509")

local M = {}

M.CERT_PREFIX = "libp2p-tls-handshake:"
M.EXTENSION_OID = "1.3.6.1.4.1.53594.1.1"

local function der_len(n)
  if n < 0x80 then
    return string.char(n)
  end
  local bytes = {}
  while n > 0 do
    table.insert(bytes, 1, string.char(n & 0xFF))
    n = math.floor(n / 256)
  end
  return string.char(0x80 + #bytes) .. table.concat(bytes)
end

local function der_octet_string(bytes)
  return string.char(0x04) .. der_len(#bytes) .. bytes
end

local function der_sequence(contents)
  return string.char(0x30) .. der_len(#contents) .. contents
end

function M.pem_body_to_der(pem, label)
  local body = tostring(pem or "")
    :gsub("%-%-%-%-%-BEGIN " .. label .. "%-%-%-%-%-", "")
    :gsub("%-%-%-%-%-END " .. label .. "%-%-%-%-%-", "")
    :gsub("%s+", "")
  if body == "" then
    return nil
  end
  return mime.unb64(body)
end

function M.encode_signed_key_extension(pubkey_proto, signature)
  return der_sequence(der_octet_string(pubkey_proto) .. der_octet_string(signature))
end

local function read_der_length(bytes, i)
  local first = bytes:byte(i)
  if not first then
    return nil, nil, error_mod.new("decode", "truncated DER length")
  end
  if first < 0x80 then
    return first, i + 1
  end
  local count = first - 0x80
  if count == 0 or count > 4 then
    return nil, nil, error_mod.new("decode", "unsupported DER length form")
  end
  local n = 0
  local idx = i + 1
  for _ = 1, count do
    local b = bytes:byte(idx)
    if not b then
      return nil, nil, error_mod.new("decode", "truncated DER length payload")
    end
    n = n * 256 + b
    idx = idx + 1
  end
  return n, idx
end

local function decode_signed_key_extension(data)
  if type(data) ~= "string" or data == "" then
    return nil, error_mod.new("decode", "tls extension data must be non-empty bytes")
  end
  local i = 1
  if data:byte(i) ~= 0x30 then
    return nil, error_mod.new("decode", "tls extension must be DER sequence")
  end
  local seq_len, seq_i, seq_err = read_der_length(data, i + 1)
  if not seq_len then
    return nil, seq_err
  end
  if seq_i + seq_len - 1 ~= #data then
    return nil, error_mod.new("decode", "unexpected tls extension sequence length")
  end
  i = seq_i

  local function read_octets()
    if data:byte(i) ~= 0x04 then
      return nil, error_mod.new("decode", "tls extension field must be octet string")
    end
    local l, ni, lerr = read_der_length(data, i + 1)
    if not l then
      return nil, lerr
    end
    local end_i = ni + l - 1
    if end_i > #data then
      return nil, error_mod.new("decode", "truncated tls extension octet string")
    end
    local out = data:sub(ni, end_i)
    i = end_i + 1
    return out
  end

  local pubkey_proto, pub_err = read_octets()
  if not pubkey_proto then
    return nil, pub_err
  end
  local signature, sig_err = read_octets()
  if not signature then
    return nil, sig_err
  end
  if i ~= #data + 1 then
    return nil, error_mod.new("decode", "unexpected trailing tls extension bytes")
  end
  return { pubkey_proto = pubkey_proto, signature = signature }
end

local function public_key_spki_der_from_cert(cert)
  local cert_pubkey, cert_pubkey_err = cert:getPublicKey()
  if not cert_pubkey then
    return nil, error_mod.new("verify", "failed to read certificate public key", { cause = cert_pubkey_err })
  end
  local pub_pem, pem_err = cert_pubkey:toPEM("public")
  if not pub_pem then
    return nil, error_mod.new("verify", "failed to export certificate public key", { cause = pem_err })
  end
  local der = M.pem_body_to_der(pub_pem, "PUBLIC KEY")
  if not der then
    return nil, error_mod.new("verify", "failed to parse certificate public key")
  end
  return der
end

function M.der_cert_to_x509(der_cert)
  if type(der_cert) ~= "string" or der_cert == "" then
    return nil, error_mod.new("verify", "missing TLS peer certificate bytes")
  end
  local pem = "-----BEGIN CERTIFICATE-----\n" .. mime.b64(der_cert) .. "\n-----END CERTIFICATE-----\n"
  local cert, cert_err = ossl_x509.new(pem)
  if not cert then
    return nil, error_mod.new("verify", "failed to decode peer certificate", { cause = cert_err })
  end
  return cert
end

function M.verify_peer_certificate(peer_cert, expected_remote_peer_id)
  if not peer_cert then
    return nil, error_mod.new("verify", "missing TLS peer certificate")
  end

  local peer_pub_der, peer_pub_der_err = public_key_spki_der_from_cert(peer_cert)
  if not peer_pub_der then
    return nil, peer_pub_der_err
  end

  local ext = peer_cert:getExtension(M.EXTENSION_OID)
  if not ext then
    return nil, error_mod.new("verify", "peer TLS certificate missing libp2p extension")
  end
  local decoded_ext, decode_err = decode_signed_key_extension(ext:getData())
  if not decoded_ext then
    return nil, decode_err
  end

  local decoded_key, key_err = key_pb.decode_public_key(decoded_ext.pubkey_proto)
  if not decoded_key then
    return nil, key_err
  end

  local peer, peer_err = peerid.from_public_key_proto(decoded_ext.pubkey_proto, decoded_key.type_name)
  if not peer then
    return nil, peer_err
  end

  local sig_ok, sig_err = keys.verify_signature(decoded_key.data, M.CERT_PREFIX .. peer_pub_der, decoded_ext.signature, decoded_key.type_name)
  if not sig_ok then
    return nil, sig_err or error_mod.new("verify", "peer TLS identity signature invalid")
  end

  if type(expected_remote_peer_id) == "string" and expected_remote_peer_id ~= "" then
    local expected = peerid.parse(expected_remote_peer_id)
    local expected_bytes = expected and expected.bytes or expected_remote_peer_id
    if expected_bytes ~= peer.bytes then
      return nil, error_mod.new("verify", "remote peer id did not match expected", {
        expected = expected and expected.id or expected_remote_peer_id,
        received = peer.id,
      })
    end
  end

  return {
    peer_id = peer,
    key_type = decoded_key.type_name,
    public_key_data = decoded_key.data,
  }
end

function M.verify_der_certificate(der_cert, expected_remote_peer_id)
  local cert, cert_err = M.der_cert_to_x509(der_cert)
  if not cert then
    return nil, cert_err
  end
  return M.verify_peer_certificate(cert, expected_remote_peer_id)
end

return M
