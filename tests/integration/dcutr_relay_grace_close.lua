local ed25519 = require("lua_libp2p.crypto.ed25519")
local host_mod = require("lua_libp2p.host")
local dcutr_service = require("lua_libp2p.protocol_dcutr.service")
local identify_service = require("lua_libp2p.protocol_identify.service")

local function run()
  local keypair = assert(ed25519.generate_keypair())
  local host, host_err = host_mod.new({
    runtime = "poll",
    identity = keypair,
    services = {
      identify = { module = identify_service },
      dcutr = { module = dcutr_service, config = { auto_on_relay_connection = false, relay_grace_seconds = 0 } },
    },
    blocking = false,
  })
  if not host then
    return nil, host_err
  end

  local closed = 0
  local original_find = host._find_connection_by_id
  host._find_connection_by_id = function(_, connection_id)
    if connection_id ~= 5001 then
      return nil
    end
    return {
      state = {
        remote_peer_id = "peer-relayed",
        relay = { limit_kind = "limited" },
      },
      conn = {
        close = function()
          closed = closed + 1
          return true
        end,
      },
    }
  end

  host.dcutr:_schedule_relay_close({
    connection_id = 5001,
    relay = { limit_kind = "limited" },
  }, "test", nil, "peer-relayed")

  host.dcutr:_schedule_relay_close({
    connection_id = 5001,
    relay = { limit_kind = "limited" },
  }, "test", nil, "relay-server")

  host.dcutr:_schedule_relay_close({
    connection_id = 5001,
    relay = { limit_kind = "unlimited" },
  }, "test")

  host._find_connection_by_id = original_find

  if closed ~= 1 then
    return nil, "expected relay close only for matching limited peer connection state"
  end

  return true
end

return {
  name = "dcutr relay close grace policy",
  run = run,
}
