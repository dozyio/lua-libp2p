local key_pb = require("lua_libp2p.crypto.key_pb")
local noise = require("lua_libp2p.security.noise")

local function read_file(path)
  local f = io.open(path, "rb")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

local function write_file(path, payload)
  local f, err = io.open(path, "wb")
  if not f then
    return nil, err
  end
  local ok, write_err = f:write(payload)
  f:close()
  if not ok then
    return nil, write_err
  end
  return true
end

local function shell_quote(text)
  return "'" .. tostring(text):gsub("'", "'\\''") .. "'"
end

local function run_cmd(cmd)
  local ok, why, code = os.execute(cmd)
  if ok == true or code == 0 then
    return true
  end
  return nil, string.format("command failed (%s/%s): %s", tostring(why), tostring(code), cmd)
end

local function run()
  local check_ok = os.execute("openssl version >/dev/null 2>/dev/null")
  if check_ok ~= true and check_ok ~= 0 then
    return nil, "openssl command is required for rsa noise test"
  end

  local priv_pem = os.tmpname()
  local priv_der = os.tmpname()
  local pub_der = os.tmpname()
  local msg_path = os.tmpname()
  local sig_path = os.tmpname()

  local cleanup = function()
    os.remove(priv_pem)
    os.remove(priv_der)
    os.remove(pub_der)
    os.remove(msg_path)
    os.remove(sig_path)
  end

  local ok, err = run_cmd("openssl genrsa -out " .. shell_quote(priv_pem) .. " 2048 >/dev/null 2>/dev/null")
  if not ok then
    cleanup()
    return nil, err
  end

  ok, err = run_cmd("openssl rsa -in " .. shell_quote(priv_pem) .. " -pubout -outform DER -out " .. shell_quote(pub_der) .. " >/dev/null 2>/dev/null")
  if not ok then
    cleanup()
    return nil, err
  end

  ok, err = run_cmd("openssl rsa -in " .. shell_quote(priv_pem) .. " -traditional -outform DER -out " .. shell_quote(priv_der) .. " >/dev/null 2>/dev/null")
  if not ok then
    cleanup()
    return nil, err
  end

  local public_der = read_file(pub_der)
  if not public_der or public_der == "" then
    cleanup()
    return nil, "failed to read generated rsa public key"
  end

  local public_proto, proto_err = key_pb.encode_public_key(key_pb.KEY_TYPE.RSA, public_der)
  if not public_proto then
    cleanup()
    return nil, proto_err
  end

  local static_pub = string.rep("\x11", 32)
  local signature_message = noise.SIG_PREFIX .. static_pub
  local wrote, write_err = write_file(msg_path, signature_message)
  if not wrote then
    cleanup()
    return nil, write_err
  end

  ok, err = run_cmd("openssl dgst -sha256 -sign " .. shell_quote(priv_pem) .. " -out " .. shell_quote(sig_path) .. " " .. shell_quote(msg_path) .. " >/dev/null 2>/dev/null")
  if not ok then
    cleanup()
    return nil, err
  end

  local signature = read_file(sig_path)
  if not signature or signature == "" then
    cleanup()
    return nil, "failed to read generated rsa signature"
  end

  local verified, verify_err = noise.verify_handshake_payload({
    identity_key = public_proto,
    identity_sig = signature,
  }, static_pub)
  if not verified then
    cleanup()
    return nil, verify_err
  end
  if verified.peer_id.type ~= "rsa" then
    cleanup()
    return nil, "expected rsa peer id after noise payload verification"
  end

  local bad_sig = string.char((signature:byte(1) ~ 0x01)) .. signature:sub(2)
  local _, bad_err = noise.verify_handshake_payload({
    identity_key = public_proto,
    identity_sig = bad_sig,
  }, static_pub)
  if not bad_err then
    cleanup()
    return nil, "expected rsa signature verification failure"
  end

  cleanup()
  return true
end

return {
  name = "noise rsa identity verification",
  run = run,
}
