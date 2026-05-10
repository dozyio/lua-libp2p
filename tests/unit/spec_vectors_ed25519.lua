local hex = require("tests.helpers.hex")
local peerid = require("lua_libp2p.peerid")
local ed25519 = require("lua_libp2p.crypto.ed25519")
local sodium = require("luasodium")

local function run()
  local public_key_proto = hex.decode("080112201ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e")
  local private_key_proto = hex.decode(
    "080112407e0830617c4a7de83925dfb2694556b12936c477a0e1feb2e148ec9da60fee7d1ed1e8fae2c4a144b8be8fd4b47bf3d3b34b871c3cacf6010f0e42d474fce27e"
  )

  local pub_data = public_key_proto:sub(5)
  local priv_data = private_key_proto:sub(5)

  if #pub_data ~= 32 then
    return nil, "expected 32-byte ed25519 public key payload"
  end
  if #priv_data ~= 64 then
    return nil, "expected 64-byte ed25519 private key payload"
  end

  local derived_pub = sodium.crypto_sign_ed25519_sk_to_pk(priv_data)
  if derived_pub ~= pub_data then
    return nil, "public key did not derive from spec private key vector"
  end

  local pid, pid_err = peerid.from_public_key_proto(public_key_proto, "ed25519")
  if not pid then
    return nil, pid_err
  end

  local parsed, parse_err = peerid.parse(pid.id)
  if not parsed then
    return nil, parse_err
  end
  if parsed.id ~= pid.id then
    return nil, "peer id did not roundtrip in base58"
  end

  local signature, sig_err = ed25519.sign({ private_key = priv_data }, "spec-vector-check")
  if not signature then
    return nil, sig_err
  end

  local ok, verify_err = ed25519.verify({ public_key = pub_data }, "spec-vector-check", signature)
  if verify_err then
    return nil, verify_err
  end
  if not ok then
    return nil, "ed25519 signature verification failed for spec key vector"
  end

  return true
end

return {
  name = "spec ed25519 key vectors",
  run = run,
}
