local ed25519 = require("lua_libp2p.crypto.ed25519")
local key_pb = require("lua_libp2p.crypto.key_pb")
local keys = require("lua_libp2p.crypto.keys")
local host = require("lua_libp2p.host")
local identify = require("lua_libp2p.protocol_identify.protocol")
local identify_service = require("lua_libp2p.protocol_identify.service")
local ping_service = require("lua_libp2p.protocol_ping.service")
local upgrader = require("lua_libp2p.network.upgrader")
local perf = require("lua_libp2p.protocol_perf.protocol")
local perf_service = require("lua_libp2p.protocol_perf.service")
local ping = require("lua_libp2p.protocol_ping.protocol")
local kad_dht_service = require("lua_libp2p.kad_dht")
local upnp_nat_service = require("lua_libp2p.upnp.nat")
local peer_discovery_bootstrap = require("lua_libp2p.peer_discovery_bootstrap")
local relay_discovery_service = require("lua_libp2p.relay_discovery")

local function run()
  local keypair, key_err = ed25519.generate_keypair()
  if not keypair then
    return nil, key_err
  end

  local bad_host, bad_err = host.new({ runtime = "invalid-runtime" })
  if bad_host ~= nil or not bad_err then
    return nil, "expected invalid runtime to return input error"
  end

  local default_host, default_host_err = host.new({
    identity = keypair,
    blocking = false,
  })
  if not default_host then
    return nil, default_host_err
  end
  if default_host._runtime ~= "luv" then
    return nil, "host should default to luv"
  end

  local poll_host, poll_err = host.new({ runtime = "poll" })
  if poll_host ~= nil or not poll_err then
    return nil, "expected removed poll runtime to be rejected"
  end

  local bad_circuit_host, bad_circuit_err = host.new({
    runtime = "luv",
    identity = keypair,
    listen_addrs = { "/p2p-circuit" },
    blocking = false,
  })
  if bad_circuit_host ~= nil or not bad_circuit_err then
    return nil, "expected /p2p-circuit listen addr without autorelay to fail"
  end

  local circuit_only_autorelay_service = {
    new = function()
      return {
        start = function()
          return true
        end,
      }
    end,
  }
  local circuit_only_host, circuit_only_host_err = host.new({
    runtime = "luv",
    identity = keypair,
    listen_addrs = { "/p2p-circuit" },
    services = {
      autorelay = { module = circuit_only_autorelay_service },
    },
    blocking = false,
  })
  if not circuit_only_host then
    return nil, circuit_only_host_err
  end
  local circuit_started, circuit_start_err = circuit_only_host:start()
  if not circuit_started then
    return nil, circuit_start_err
  end
  circuit_only_host:stop()

  local default_discovery_host, default_discovery_host_err = host.new({
    runtime = "luv",
    identity = keypair,
    peer_discovery = {
      bootstrap = { module = peer_discovery_bootstrap, config = {} },
    },
    blocking = false,
  })
  if not default_discovery_host then
    return nil, default_discovery_host_err
  end
  if
    not default_discovery_host._bootstrap_discovery
    or type(default_discovery_host._bootstrap_discovery.config.list) ~= "table"
    or #default_discovery_host._bootstrap_discovery.config.list == 0
  then
    return nil, "empty bootstrap config should use default bootstrappers"
  end

  local discovery_host, discovery_host_err = host.new({
    runtime = "luv",
    identity = keypair,
    peer_discovery = {
      bootstrap = {
        module = peer_discovery_bootstrap,
        config = {
          list = {
            "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV",
            "/ip4/127.0.0.1/tcp/4002/p2p/12D3KooWQWZLu9qXWPTDnF9rTRrAiVGZrXCbHAvkqYrsG8cW4UHg",
          },
        },
      },
    },
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      kad_dht = { module = kad_dht_service },
    },
    blocking = false,
  })
  if not discovery_host then
    return nil, discovery_host_err
  end
  if not discovery_host.peer_discovery then
    return nil, "host should build peer discovery from config"
  end
  if not discovery_host.kad_dht or discovery_host.kad_dht.peer_discovery ~= discovery_host.peer_discovery then
    return nil, "kad_dht service should default to host peer_discovery"
  end
  if
    discovery_host.components.identify ~= discovery_host.services.identify
    or discovery_host.components.ping ~= discovery_host.services.ping
    or discovery_host.components.kad_dht ~= discovery_host.services.kad_dht
    or discovery_host.components.peer_routing ~= discovery_host.services.kad_dht
  then
    return nil, "host should register service capabilities in components"
  end

  local missing_dep_host, missing_dep_err = host.new({
    runtime = "luv",
    identity = keypair,
    services = {
      kad_dht = { module = kad_dht_service },
    },
    blocking = false,
  })
  if missing_dep_host ~= nil or not missing_dep_err or missing_dep_err.kind ~= "state" then
    return nil, "kad_dht without identify/ping should fail dependency validation"
  end

  local bootstrap_dialed = {}
  function discovery_host:dial(target)
    bootstrap_dialed[#bootstrap_dialed + 1] = target
    return {}, { remote_peer_id = target.peer_id }
  end
  discovery_host._bootstrap_discovery.timeout = 0
  local discovery_started, discovery_start_err = discovery_host:start()
  if not discovery_started then
    return nil, discovery_start_err
  end
  local discovery_polled, discovery_poll_err = discovery_host:_poll_once(0)
  if not discovery_polled then
    return nil, discovery_poll_err
  end
  if #bootstrap_dialed ~= 2 then
    return nil, "host should spawn scheduler dials for all bootstrap peers after startup delay"
  end
  local bootstrap_tags = discovery_host.peerstore:get_tags(bootstrap_dialed[1].peer_id)
  if not bootstrap_tags.bootstrap or bootstrap_tags.bootstrap.value ~= 50 then
    return nil, "host should tag bootstrap peers"
  end
  local bootstrap_stats = discovery_host:task_stats()
  if
    (bootstrap_stats.by_name["host.bootstrap_discovery"] or 0) < 1
    or (bootstrap_stats.by_name["host.bootstrap_dial"] or 0) < 2
    or (bootstrap_stats.by_status.completed or 0) < 3
  then
    return nil, "host task stats should include completed bootstrap dial tasks"
  end
  local host_stats = discovery_host:stats()
  if host_stats.runtime ~= "luv" or host_stats.tasks.total ~= bootstrap_stats.total then
    return nil, "host stats should expose runtime and task stats"
  end
  if not host_stats.connections or host_stats.connections.max_parallel_dials == nil then
    return nil, "host stats should expose connection manager stats"
  end

  local failed_discovery_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    peer_discovery = {
      bootstrap = {
        module = peer_discovery_bootstrap,
        config = {
          list = {
            "/ip4/127.0.0.1/tcp/4003/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV",
          },
          timeout = 0,
        },
      },
    },
    blocking = false,
  }))
  function failed_discovery_host:dial()
    return nil, nil, require("lua_libp2p.error").new("io", "forced bootstrap failure")
  end
  assert(failed_discovery_host:start())
  assert(failed_discovery_host:_poll_once(0))
  local failed_tasks = failed_discovery_host:list_tasks()
  if
    #failed_tasks ~= 2
    or failed_tasks[1].name ~= "host.bootstrap_discovery"
    or failed_tasks[2].name ~= "host.bootstrap_dial"
    or failed_tasks[2].result ~= false
  then
    return nil, "bootstrap dial task should report false when dial returns nil,error"
  end
  failed_discovery_host:stop()
  local missed_need_more_service = {
    provides = { "autorelay" },
    new = function(service_host)
      return {
        host = service_host,
        need_more_relays = false,
        start = function(self)
          self.need_more_relays = true
          service_host:emit("relay:not-enough-relays", { reason = "start" })
          return true
        end,
        tick = function()
          return true
        end,
      }
    end,
  }
  local missed_need_more_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    services = {
      autorelay = { module = missed_need_more_service },
      relay_discovery = { module = relay_discovery_service, config = { cooldown_seconds = 1 } },
    },
    blocking = false,
  }))
  if not missed_need_more_host.relay_discovery.pending then
    return nil, "relay discovery should reconcile autorelay need_more state emitted before subscription"
  end
  assert(missed_need_more_host:start())
  if
    not missed_need_more_host.relay_discovery.task
    or missed_need_more_host.relay_discovery.task.name ~= "relay_discovery.replenish"
  then
    return nil, "host start should schedule pending relay discovery replenishment"
  end
  missed_need_more_host._bootstrap_discovery = { dial_on_start = true }
  missed_need_more_host.kad_dht = { routing_table = {
    all_peers = function()
      return {}
    end,
  } }
  missed_need_more_host.relay_discovery.task = nil
  local replenish_ok, replenish_err = missed_need_more_host.relay_discovery:run_once(os.time())
  if not replenish_ok then
    return nil, replenish_err
  end
  if not missed_need_more_host.relay_discovery.pending then
    return nil, "relay candidate replenish should remain pending while bootstrap reseeds empty kad table"
  end
  local replenish_task = missed_need_more_host.relay_discovery.task
  if not replenish_task or replenish_task.name ~= "relay_discovery.replenish" then
    return nil, "empty kad table should reschedule relay candidate replenish after bootstrap reseed"
  end
  missed_need_more_host:stop()
  discovery_host._identify_inflight["peer-a"] = true
  discovery_host._identify_inflight["peer-b"] = true
  if discovery_host:task_stats().identify_inflight ~= 2 then
    return nil, "host task stats should expose identify inflight count"
  end
  discovery_host._identify_inflight = {}
  discovery_host:stop()

  local identify_direct_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local remote_public_key, remote_public_key_err = key_pb.encode_public_key(key_pb.KEY_TYPE.Ed25519, keypair.public_key)
  if not remote_public_key then
    return nil, remote_public_key_err
  end
  local identify_writer = {
    data = "",
    write = function(self, chunk)
      self.data = self.data .. chunk
      return true
    end,
  }
  local wrote_identify, wrote_identify_err = identify.write(identify_writer, {
    protocolVersion = "/lua-libp2p/test",
    agentVersion = "lua-libp2p/test",
    publicKey = remote_public_key,
    listenAddrs = {},
    protocols = { identify.ID },
  })
  if not wrote_identify then
    return nil, wrote_identify_err
  end
  local identify_response_stream = {
    data = identify_writer.data,
    read = function(self, n)
      if #self.data < n then
        return nil, "unexpected EOF"
      end
      local chunk = self.data:sub(1, n)
      self.data = self.data:sub(n + 1)
      return chunk
    end,
    close = function()
      return true
    end,
  }
  local direct_identify_opened = false
  local direct_identify_conn = {
    new_stream = function(_, protocols)
      direct_identify_opened = true
      return identify_response_stream, protocols[1]
    end,
  }
  identify_direct_host.dial = function()
    return nil, nil, "identify should reuse the connection from peer_connected"
  end
  local direct_identify_result, direct_identify_err = identify_direct_host:_request_identify("peer-direct", {
    connection = direct_identify_conn,
    state = { remote_peer_id = "peer-direct", direction = "inbound" },
  })
  if not direct_identify_result then
    return nil, direct_identify_err
  end
  if not direct_identify_opened or direct_identify_result.connection ~= direct_identify_conn then
    return nil, "identify on connect should open its stream on the existing connection"
  end

  local limited_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    services = {
      ping = { module = ping_service },
    },
    blocking = false,
  }))
  local opened_streams = {}
  local limited_conn = {
    new_stream = function(_, protocols)
      opened_streams[#opened_streams + 1] = protocols[1]
      return {}, protocols[1]
    end,
  }
  function limited_host:dial()
    return limited_conn, { relay = { limit_kind = "limited" } }
  end
  local app_stream, _, _, app_err = limited_host:new_stream("peer-a", { "/app/1.0.0" })
  if app_stream or not app_err or app_err.kind ~= "permission" or #opened_streams ~= 0 then
    return nil, "limited connections should reject protocols without handler opt-in before opening streams"
  end
  local handled_ok, handled_err = limited_host:handle("/app/allowed/1.0.0", function()
    return true
  end, {
    run_on_limited_connection = true,
  })
  if not handled_ok then
    return nil, handled_err
  end
  local allowed_stream, allowed_selected, _, allowed_err = limited_host:new_stream("peer-a", { "/app/allowed/1.0.0" })
  if not allowed_stream or allowed_selected ~= "/app/allowed/1.0.0" then
    return nil, allowed_err or "limited connections should allow protocols opted in at handler registration"
  end
  local ping_stream, ping_selected, _, ping_stream_err = limited_host:new_stream("peer-a", { ping.ID })
  if not ping_stream or ping_selected ~= ping.ID then
    return nil, ping_stream_err or "ping service should opt into limited connections at handler registration"
  end

  local multi_dial_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local dialed = {}
  local dial_timeouts = {}
  multi_dial_host._tcp_transport = {
    dial = function(endpoint, opts)
      dialed[#dialed + 1] = endpoint.host .. ":" .. tostring(endpoint.port)
      dial_timeouts[#dial_timeouts + 1] = opts and opts.timeout
      if #dialed == 1 then
        return nil, require("lua_libp2p.error").new("timeout", "tcp connect timed out")
      end
      return {
        close = function()
          return true
        end,
      }
    end,
  }
  local original_upgrade_outbound = upgrader.upgrade_outbound
  local multi_peer_id = "12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV"
  upgrader.upgrade_outbound = function()
    return {
      close = function()
        return true
      end,
    }, { remote_peer_id = multi_peer_id }
  end
  local multi_conn, _, multi_err = multi_dial_host:dial({
    peer_id = multi_peer_id,
    addrs = {
      "/ip4/127.0.0.1/tcp/4001/p2p/" .. multi_peer_id,
      "/ip4/8.8.8.8/tcp/4001/p2p/" .. multi_peer_id,
      "/ip4/1.1.1.1/tcp/4001/p2p/" .. multi_peer_id,
    },
  }, { address_dial_timeout = 2, dial_timeout = 5 })
  upgrader.upgrade_outbound = original_upgrade_outbound
  if not multi_conn then
    return nil, multi_err
  end
  if #dialed ~= 2 or dialed[1] ~= "8.8.8.8:4001" or dialed[2] ~= "1.1.1.1:4001" then
    return nil, "connection manager should rank public peer addresses before other addresses"
  end
  if dial_timeouts[1] ~= 2 or dial_timeouts[2] ~= 2 then
    return nil, "connection manager should apply per-address dial timeout"
  end

  local family_pref_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    listen_addrs = { "/ip4/0.0.0.0/tcp/4001" },
    blocking = false,
  }))
  local family_dialed = {}
  family_pref_host._tcp_transport = {
    dial = function(endpoint)
      family_dialed[#family_dialed + 1] = endpoint.host .. ":" .. tostring(endpoint.port)
      if #family_dialed == 1 then
        return nil, require("lua_libp2p.error").new("io", "forced first dial failure")
      end
      return {
        close = function()
          return true
        end,
      }
    end,
  }
  local original_family_upgrade = upgrader.upgrade_outbound
  upgrader.upgrade_outbound = function()
    return {
      close = function()
        return true
      end,
    }, { remote_peer_id = multi_peer_id }
  end
  local family_conn, _, family_err = family_pref_host:dial({
    peer_id = multi_peer_id,
    addrs = {
      "/ip6/2001:db8::1/tcp/4001/p2p/" .. multi_peer_id,
      "/ip4/8.8.8.8/tcp/4001/p2p/" .. multi_peer_id,
    },
  }, { address_dial_timeout = 2, dial_timeout = 5 })
  upgrader.upgrade_outbound = original_family_upgrade
  if not family_conn then
    return nil, family_err
  end
  if #family_dialed ~= 2 or family_dialed[1] ~= "8.8.8.8:4001" or family_dialed[2] ~= "2001:db8::1:4001" then
    return nil, "dial_direct should prefer family-compatible addresses before fallback candidates"
  end

  local ipv6_ranked = multi_dial_host.connection_manager:rank_addrs({
    "/ip6/2001:db8::1/tcp/4001/p2p/" .. multi_peer_id,
  })
  if ipv6_ranked[1] == nil or ipv6_ranked[1]:sub(1, 5) ~= "/ip6/" then
    return nil, "connection manager should retain direct ipv6 tcp addresses"
  end

  local selection_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local limited_stream_opened = false
  assert(selection_host:_register_connection({
    new_stream = function()
      limited_stream_opened = true
      return {}, "/app/1.0.0"
    end,
    close = function()
      return true
    end,
  }, {
    remote_peer_id = multi_peer_id,
    direction = "outbound",
    relay = { limit_kind = "limited" },
  }))
  local direct_dialed = false
  selection_host._tcp_transport = {
    dial = function()
      direct_dialed = true
      return {
        close = function()
          return true
        end,
      }
    end,
  }
  local original_selection_upgrade = upgrader.upgrade_outbound
  local direct_conn = {
    new_stream = function(_, protocols)
      return { direct = true }, protocols[1]
    end,
    close = function()
      return true
    end,
  }
  upgrader.upgrade_outbound = function()
    return direct_conn, { remote_peer_id = multi_peer_id, direction = "outbound" }
  end
  local direct_stream, direct_selected, direct_stream_conn, direct_state_or_err = selection_host:new_stream({
    peer_id = multi_peer_id,
    addrs = { "/ip4/8.8.8.8/tcp/4001/p2p/" .. multi_peer_id },
  }, { "/app/1.0.0" })
  upgrader.upgrade_outbound = original_selection_upgrade
  if not direct_stream then
    return nil, direct_state_or_err
  end
  if
    not direct_dialed
    or limited_stream_opened
    or direct_selected ~= "/app/1.0.0"
    or direct_stream_conn ~= direct_conn
  then
    return nil, "new_stream should prefer dialing an unlimited connection over reusing a limited one"
  end

  local prune_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_connections = 2,
      low_water = 1,
      grace_period = 0,
      silence_period = 0,
    },
  }))
  local closed = {}
  local function fake_conn(label)
    return {
      close = function()
        closed[label] = true
        return true
      end,
    }
  end
  assert(prune_host.connection_manager:protect("peer-protected"))
  assert(prune_host:_register_connection(fake_conn("protected"), { remote_peer_id = "peer-protected" }))
  assert(prune_host:_register_connection(fake_conn("old"), { remote_peer_id = "peer-old" }))
  assert(prune_host:_register_connection(fake_conn("new"), { remote_peer_id = "peer-new" }))
  if not closed.old or closed.protected or closed.new then
    return nil, "connection manager should prune unprotected old connections before protected ones"
  end
  local prune_stats = prune_host.connection_manager:stats()
  if prune_stats.connections ~= 2 or prune_stats.pruned ~= 1 then
    return nil, "connection manager should track open and pruned connections"
  end

  local weighted_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_connections = 3,
      low_water = 2,
      grace_period = 0,
      silence_period = 0,
    },
  }))
  local weighted_closed = {}
  local function weighted_conn(label)
    return {
      close = function()
        weighted_closed[label] = true
        return true
      end,
    }
  end
  assert(weighted_host.connection_manager:tag_peer("peer-low", "test", 1))
  assert(weighted_host.connection_manager:tag_peer("peer-mid", "test", 10))
  assert(weighted_host.connection_manager:tag_peer("peer-high", "test", 100))
  assert(weighted_host:_register_connection(weighted_conn("low"), { remote_peer_id = "peer-low" }))
  assert(weighted_host:_register_connection(weighted_conn("mid"), { remote_peer_id = "peer-mid" }))
  assert(weighted_host:_register_connection(weighted_conn("high"), { remote_peer_id = "peer-high" }))
  assert(weighted_host:_register_connection(weighted_conn("new"), { remote_peer_id = "peer-new" }))
  if not weighted_closed.low or weighted_closed.new or weighted_closed.mid or weighted_closed.high then
    return nil, "connection manager should prune lowest-value existing peer before admitting at max_connections"
  end
  if weighted_host.connection_manager:stats().connections ~= 3 then
    return nil, "connection manager should admit new connection after pruning below max_connections"
  end

  local default_watermark_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local default_watermark_stats = default_watermark_host.connection_manager:stats()
  if default_watermark_stats.high_water ~= 192 or default_watermark_stats.low_water ~= 160 then
    return nil, "connection manager should install default watermarks below resource limits"
  end

  local watermark_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      high_water = 3,
      low_water = 2,
      grace_period = 0,
      silence_period = 0,
    },
  }))
  local watermark_closed = {}
  local function watermark_conn(label)
    return {
      close = function()
        watermark_closed[label] = true
        return true
      end,
    }
  end
  assert(watermark_host.connection_manager:tag_peer("peer-low", "test", 1))
  assert(watermark_host.connection_manager:tag_peer("peer-mid", "test", 10))
  assert(watermark_host.connection_manager:tag_peer("peer-high", "test", 100))
  assert(watermark_host:_register_connection(watermark_conn("low"), { remote_peer_id = "peer-low" }))
  assert(watermark_host:_register_connection(watermark_conn("mid"), { remote_peer_id = "peer-mid" }))
  assert(watermark_host:_register_connection(watermark_conn("high"), { remote_peer_id = "peer-high" }))
  assert(watermark_host:_register_connection(watermark_conn("new"), { remote_peer_id = "peer-new" }))
  if not watermark_closed.new or not watermark_closed.low or watermark_closed.mid or watermark_closed.high then
    return nil, "connection manager should prune lowest-value peers down to low_water after high_water"
  end
  if watermark_host.connection_manager:stats().connections ~= 2 then
    return nil, "connection manager should prune high_water overflow down to low_water"
  end

  local grace_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      high_water = 2,
      low_water = 1,
      grace_period = 60,
    },
  }))
  local grace_closed = {}
  local function grace_conn(label)
    return {
      close = function()
        grace_closed[label] = true
        return true
      end,
    }
  end
  assert(grace_host:_register_connection(grace_conn("a"), { remote_peer_id = "peer-grace-a" }))
  assert(grace_host:_register_connection(grace_conn("b"), { remote_peer_id = "peer-grace-b" }))
  assert(grace_host:_register_connection(grace_conn("c"), { remote_peer_id = "peer-grace-c" }))
  if grace_closed.a or grace_closed.b or grace_closed.c then
    return nil, "connection manager should not prune fresh connections during grace period"
  end
  if grace_host.connection_manager:stats().connections ~= 3 then
    return nil, "connection manager should temporarily exceed high_water during grace period"
  end

  local silence_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      high_water = 2,
      low_water = 1,
      grace_period = 0,
      silence_period = 10,
    },
  }))
  local silence_closed = {}
  local function silence_conn(label)
    return {
      close = function()
        silence_closed[label] = true
        return true
      end,
    }
  end
  assert(silence_host:_register_connection(silence_conn("a"), { remote_peer_id = "peer-silence-a" }))
  assert(silence_host:_register_connection(silence_conn("b"), { remote_peer_id = "peer-silence-b" }))
  assert(silence_host:_register_connection(silence_conn("c"), { remote_peer_id = "peer-silence-c" }))
  local after_first_prune = silence_host.connection_manager:stats().connections
  if next(silence_closed) == nil then
    return nil, "connection manager should close a connection during soft pruning"
  end
  assert(silence_host:_register_connection(silence_conn("d"), { remote_peer_id = "peer-silence-d" }))
  if silence_host.connection_manager:stats().connections <= after_first_prune then
    return nil, "connection manager should suppress repeated soft pruning during silence period"
  end

  local all_protected_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_connections = 1,
      low_water = 0,
    },
  }))
  assert(all_protected_host.connection_manager:protect("peer-protected-a"))
  assert(all_protected_host:_register_connection(fake_conn("protected-a"), { remote_peer_id = "peer-protected-a" }))
  local rejected_entry, rejected_err = all_protected_host:_register_connection(fake_conn("rejected"), {
    remote_peer_id = "peer-rejected",
  })
  if rejected_entry ~= nil or not rejected_err or rejected_err.kind ~= "resource" then
    return nil, "connection manager should reject new connection when all existing peers are protected"
  end
  if all_protected_host.connection_manager:stats().connections ~= 1 then
    return nil, "connection manager should not track rejected connections"
  end

  local cleanup_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local cleanup_entry =
    assert(cleanup_host:_register_connection(fake_conn("cleanup"), { remote_peer_id = "peer-cleanup" }))
  if cleanup_host.connection_manager:stats().connections ~= 1 then
    return nil, "connection manager should track registered connection"
  end
  assert(
    cleanup_host:_unregister_connection(nil, cleanup_entry, require("lua_libp2p.error").new("closed", "test close"))
  )
  if
    cleanup_host.connection_manager:stats().connections ~= 0
    or cleanup_host.connection_manager.connections_by_peer["peer-cleanup"] ~= nil
  then
    return nil, "connection manager should clean tracking on unregister"
  end

  local direction_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_inbound_connections = 1,
      max_outbound_connections = 1,
    },
  }))
  local direction_closed = {}
  local function direction_conn(label)
    return {
      close = function()
        direction_closed[label] = true
        return true
      end,
    }
  end
  assert(direction_host:_register_connection(direction_conn("in-a"), {
    remote_peer_id = "peer-in-a",
    direction = "inbound",
  }))
  assert(direction_host:_register_connection(direction_conn("out-a"), {
    remote_peer_id = "peer-out-a",
    direction = "outbound",
  }))
  assert(direction_host:_register_connection(direction_conn("in-b"), {
    remote_peer_id = "peer-in-b",
    direction = "inbound",
  }))
  if not direction_closed["in-a"] or direction_closed["out-a"] or direction_closed["in-b"] then
    return nil, "inbound limit should prune inbound connections without pruning outbound ones"
  end
  local direction_stats = direction_host.connection_manager:stats()
  if direction_stats.inbound_connections ~= 1 or direction_stats.outbound_connections ~= 1 then
    return nil, "connection manager should track inbound and outbound connection counts"
  end

  local per_peer_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_connections_per_peer = 1,
    },
  }))
  assert(per_peer_host:_register_connection(fake_conn("per-peer-a"), {
    remote_peer_id = "peer-same",
    direction = "inbound",
  }))
  local per_peer_entry, per_peer_err = per_peer_host:_register_connection(fake_conn("per-peer-b"), {
    remote_peer_id = "peer-same",
    direction = "outbound",
  })
  if per_peer_entry ~= nil or not per_peer_err or per_peer_err.kind ~= "resource" then
    return nil, "connection manager should reject connections over per-peer limit"
  end

  local protected_inbound_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
    dial_queue = {
      max_inbound_connections = 1,
    },
  }))
  assert(protected_inbound_host.connection_manager:protect("peer-in-protected"))
  assert(protected_inbound_host:_register_connection(fake_conn("in-protected"), {
    remote_peer_id = "peer-in-protected",
    direction = "inbound",
  }))
  local protected_inbound_entry, protected_inbound_err =
    protected_inbound_host:_register_connection(fake_conn("in-rejected"), {
      remote_peer_id = "peer-in-rejected",
      direction = "inbound",
    })
  if protected_inbound_entry ~= nil or not protected_inbound_err or protected_inbound_err.kind ~= "resource" then
    return nil, "inbound limit should reject when only protected inbound connections exist"
  end

  local total_timeout_host = assert(host.new({
    runtime = "luv",
    identity = keypair,
    blocking = false,
  }))
  local observed_timeout
  total_timeout_host._tcp_transport = {
    dial = function(_, opts)
      observed_timeout = opts and opts.timeout
      return nil, require("lua_libp2p.error").new("timeout", "tcp connect timed out")
    end,
  }
  total_timeout_host:dial({
    peer_id = multi_peer_id,
    addrs = { "/ip4/8.8.4.4/tcp/4001/p2p/" .. multi_peer_id },
  }, { address_dial_timeout = 6, dial_timeout = 1 })
  if type(observed_timeout) ~= "number" or observed_timeout > 1 or observed_timeout <= 0 then
    return nil, "connection manager should cap address timeout by total dial timeout"
  end

  for _, key_type in ipairs({ "rsa", "ecdsa", "secp256k1" }) do
    local identity, identity_err = keys.generate_keypair(key_type)
    if not identity then
      return nil, identity_err
    end
    local typed_host, typed_host_err = host.new({
      runtime = "luv",
      identity = identity,
      blocking = false,
    })
    if not typed_host then
      return nil, typed_host_err
    end
    if typed_host:peer_id().type ~= key_type then
      return nil, "expected host peer id type " .. key_type
    end
  end

  local h, h_err = host.new({
    runtime = "luv",
    identity = keypair,
    listen_addrs = { "/ip4/127.0.0.1/tcp/0" },
    transports = { "tcp" },
    security_transports = { "/plaintext/2.0.0" },
    muxers = { "/yamux/1.0.0" },
    services = {
      identify = { module = identify_service },
      ping = { module = ping_service },
      perf = { module = perf_service },
      kad_dht = { module = kad_dht_service },
    },
    blocking = false,
    accept_timeout = 0.01,
  })
  if not h then
    return nil, h_err
  end

  local ok, reg_err = h:handle(ping.ID, function()
    return true
  end)
  if not ok then
    return nil, reg_err
  end

  if type(h._handlers[identify.ID]) ~= "function" then
    return nil, "identify service should register /ipfs/id/1.0.0 handler"
  end
  if type(h._handlers[identify.PUSH_ID]) ~= "function" then
    return nil, "identify service should register /ipfs/id/push/1.0.0 handler"
  end

  if type(h._handlers[perf.ID]) ~= "function" then
    return nil, "perf service should register /perf/1.0.0 handler"
  end
  if not h.kad_dht or h.kad_dht.mode ~= "client" then
    return nil, "kad_dht host service should default to client mode"
  end
  if h._handlers[h.kad_dht.protocol_id] ~= nil then
    return nil, "client-mode kad_dht service should not advertise/register handler"
  end

  local upnp_mod = require("lua_libp2p.upnp.nat")
  local original_upnp_new = upnp_mod.new
  local upnp_seen_opts
  upnp_mod.new = function(_, opts)
    upnp_seen_opts = opts
    return {
      start = function()
        return true
      end,
    }
  end
  local upnp_host, upnp_host_err = host.new({
    runtime = "luv",
    identity = keypair,
    services = {
      upnp_nat = {
        module = upnp_nat_service,
        config = { internal_client = "192.168.1.124" },
      },
    },
    blocking = false,
  })
  upnp_mod.new = original_upnp_new
  if not upnp_host then
    return nil, upnp_host_err
  end
  if not upnp_seen_opts or upnp_seen_opts.internal_client ~= "192.168.1.124" then
    return nil, "host should pass upnp_nat service options"
  end

  local started, start_err = h:start()
  if not started then
    return nil, start_err
  end

  local addrs = h:get_multiaddrs_raw()
  if #addrs ~= 1 then
    return nil, "expected one listen address"
  end
  if not addrs[1]:match("^/ip4/127.0.0.1/tcp/%d+$") then
    return nil, "unexpected listen address shape"
  end

  local listed = h:get_listen_addrs()
  if #listed ~= 1 or listed[1] ~= addrs[1] then
    return nil, "get_listen_addrs mismatch"
  end

  local local_peer = h:peer_id()
  if type(local_peer) ~= "table" or type(local_peer.id) ~= "string" or local_peer.id == "" then
    return nil, "peer_id method should return local peer id record"
  end
  if not h.peerstore then
    return nil, "expected host peerstore"
  end
  if not h.address_manager then
    return nil, "expected host address manager"
  end

  local addrs_raw = h:get_multiaddrs_raw()
  if #addrs_raw ~= 1 or addrs_raw[1] ~= addrs[1] then
    return nil, "get_multiaddrs_raw mismatch"
  end

  local full_addrs = h:get_multiaddrs()
  if #full_addrs ~= 1 then
    return nil, "expected one full multiaddr"
  end
  if full_addrs[1] ~= addrs[1] .. "/p2p/" .. local_peer.id then
    return nil, "get_multiaddrs should append local peer id"
  end

  h.address_manager:add_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")
  local with_relay = h:get_multiaddrs()
  if #with_relay ~= 2 then
    return nil, "expected listen and relay advertised addrs"
  end
  if with_relay[2] ~= "/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit/p2p/" .. local_peer.id then
    return nil, "expected relay addr to be advertised with local peer id"
  end
  h.address_manager:remove_relay_addr("/ip4/198.51.100.1/tcp/4001/p2p/relay/p2p-circuit")

  local matched_peer = nil
  local protocol_cb, protocol_cb_err = h:on_protocol("/relay/test/1.0.0", function(peer_id)
    matched_peer = peer_id
    return true
  end)
  if not protocol_cb then
    return nil, protocol_cb_err
  end
  local protocol_handlers = h._event_handlers.peer_protocols_updated
  if type(protocol_handlers) ~= "table" or type(protocol_handlers[1]) ~= "function" then
    return nil, "expected protocol update handler"
  end
  for _, handler_fn in ipairs(protocol_handlers) do
    handler_fn({
      peer_id = "peer-relay",
      protocols = { "/relay/test/1.0.0" },
      added_protocols = { "/relay/test/1.0.0" },
    })
  end
  if matched_peer ~= "peer-relay" then
    return nil, "on_protocol should match protocol update events"
  end

  h.listen_addrs = {
    "/ip4/203.0.113.1/tcp/4001/p2p/12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV/p2p-circuit",
  }
  local relay_addrs = h:get_multiaddrs()
  if #relay_addrs ~= 1 then
    return nil, "expected one relay multiaddr"
  end
  if relay_addrs[1] ~= h.listen_addrs[1] .. "/p2p/" .. local_peer.id then
    return nil, "get_multiaddrs should append local peer id after p2p-circuit"
  end

  h.listen_addrs = {
    "/ip4/127.0.0.1/tcp/4001/p2p/" .. local_peer.id,
  }
  local terminal = h:get_multiaddrs()
  if #terminal ~= 1 or terminal[1] ~= h.listen_addrs[1] then
    return nil, "get_multiaddrs should preserve terminal p2p address"
  end

  h._start_blocking = false
  h._start_max_iterations = 1
  h._start_poll_interval = 0
  started, start_err = h:start()
  if not started then
    return nil, start_err
  end
  if not h:is_running() then
    return nil, "host should be running after start"
  end

  local stopped, stop_err = h:stop()
  if not stopped then
    return nil, stop_err
  end
  if h:is_running() then
    return nil, "host should not be running after stop"
  end

  local _, _, dial_err = h:dial("12D3KooWCryG7Mon9orvQxcS1rYZjotPgpwoJNHHKcLLfE4Hf5mV")
  if not dial_err then
    return nil, "expected dial error for peer id without address"
  end

  h:close()
  return true
end

return {
  name = "host config and lifecycle",
  run = run,
}
