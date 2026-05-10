local ed25519 = require("lua_libp2p.crypto.ed25519")
local hex = require("tests.helpers.hex")

local function run()
  local keypair, gen_err = ed25519.generate_keypair()
  if not keypair then
    return nil, gen_err
  end

  if #keypair.public_key ~= 32 then
    return nil, "expected 32-byte ed25519 public key"
  end

  local message = "lua-libp2p-m1"
  local signature, sign_err = ed25519.sign(keypair, message)
  if not signature then
    return nil, sign_err
  end

  local ok, verify_err = ed25519.verify(keypair, message, signature)
  if verify_err then
    return nil, verify_err
  end
  if not ok then
    return nil, "expected signature verification success"
  end

  local bad = ed25519.verify(keypair, message .. "-tampered", signature)
  if bad then
    return nil, "expected tampered message verification failure"
  end

  local path = os.tmpname() .. ".pem"
  local save_ok, save_err = ed25519.save_private_key(path, keypair)
  if not save_ok then
    return nil, save_err
  end

  local loaded, load_err = ed25519.load_private_key(path)
  os.remove(path)
  if not loaded then
    return nil, load_err
  end
  if loaded.public_key ~= keypair.public_key then
    return nil, "loaded keypair public key mismatch"
  end

  local vec_public = hex.decode("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c")
  local vec_sig = hex.decode(
    "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da"
      .. "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
  )
  local vec_key, vec_key_err = ed25519.public_key_from_raw(vec_public)
  if not vec_key then
    return nil, vec_key_err
  end
  local vec_ok, vec_verify_err = ed25519.verify(vec_key, hex.decode("72"), vec_sig)
  if vec_verify_err then
    return nil, vec_verify_err
  end
  if not vec_ok then
    return nil, "rfc8032 vector signature did not verify"
  end

  return true
end

return {
  name = "ed25519 generate/sign/verify/load",
  run = run,
}
