local error_mod = require("lua_libp2p.error")

local ok, sodium = pcall(require, "luasodium")
if not ok then
  error("luasodium is required for ed25519 support")
end

local M = {}

local PK_BYTES = sodium.crypto_sign_ed25519_PUBLICKEYBYTES
local SK_BYTES = sodium.crypto_sign_ed25519_SECRETKEYBYTES
local SIG_BYTES = sodium.crypto_sign_ed25519_BYTES

local function read_file(path, mode)
  local handle, open_err = io.open(path, mode or "rb")
  if not handle then
    return nil, error_mod.new("io", "open failed", { path = path, cause = open_err })
  end
  local data = handle:read("*a")
  handle:close()
  return data
end

local function write_file(path, data, mode)
  local handle, open_err = io.open(path, mode or "wb")
  if not handle then
    return nil, error_mod.new("io", "open failed", { path = path, cause = open_err })
  end
  local ok_write, write_err = handle:write(data)
  handle:close()
  if not ok_write then
    return nil, error_mod.new("io", "write failed", { path = path, cause = write_err })
  end
  return true
end

local function expect_len(name, value, expected)
  if #value ~= expected then
    return nil, error_mod.new("input", string.format("%s must be %d bytes", name, expected), {
      actual = #value,
      expected = expected,
    })
  end
  return true
end

local function extract_public_key(input)
  if type(input) == "string" then
    return input
  end
  if type(input) == "table" and type(input.public_key) == "string" then
    return input.public_key
  end
  return nil
end

function M.generate_keypair()
  local public_key, private_key = sodium.crypto_sign_ed25519_keypair()
  return {
    private_key = private_key,
    public_key = public_key,
  }
end

function M.public_key_from_raw(raw_public_key)
  if type(raw_public_key) ~= "string" then
    return nil, error_mod.new("input", "public key must be a string")
  end
  local ok_len, err = expect_len("ed25519 public key", raw_public_key, PK_BYTES)
  if not ok_len then
    return nil, err
  end
  return { public_key = raw_public_key }
end

function M.save_private_key(path, keypair)
  if not keypair or type(keypair.private_key) ~= "string" then
    return nil, error_mod.new("input", "missing private key")
  end
  local ok_len, err = expect_len("ed25519 private key", keypair.private_key, SK_BYTES)
  if not ok_len then
    return nil, err
  end
  return write_file(path, keypair.private_key, "wb")
end

function M.load_private_key(path)
  local private_key, read_err = read_file(path, "rb")
  if not private_key then
    return nil, read_err
  end

  local ok_len, len_err = expect_len("ed25519 private key", private_key, SK_BYTES)
  if not ok_len then
    return nil, len_err
  end

  local public_key = sodium.crypto_sign_ed25519_sk_to_pk(private_key)
  return {
    private_key = private_key,
    public_key = public_key,
  }
end

function M.sign(keypair, message)
  if not keypair or type(keypair.private_key) ~= "string" then
    return nil, error_mod.new("input", "missing private key")
  end
  local ok_len, err = expect_len("ed25519 private key", keypair.private_key, SK_BYTES)
  if not ok_len then
    return nil, err
  end
  local payload = message or ""
  return sodium.crypto_sign_ed25519_detached(payload, keypair.private_key)
end

function M.verify(public_key, message, signature)
  local raw_public_key = extract_public_key(public_key)
  if not raw_public_key then
    return nil, error_mod.new("input", "missing public key")
  end
  local ok_pk, pk_err = expect_len("ed25519 public key", raw_public_key, PK_BYTES)
  if not ok_pk then
    return nil, pk_err
  end
  if type(signature) ~= "string" then
    return nil, error_mod.new("input", "missing signature")
  end
  local ok_sig, sig_err = expect_len("ed25519 signature", signature, SIG_BYTES)
  if not ok_sig then
    return nil, sig_err
  end

  local payload = message or ""
  return sodium.crypto_sign_ed25519_verify_detached(signature, payload, raw_public_key)
end

return M
