local kad_dht = require("lua_libp2p.kad_dht")
local error_mod = require("lua_libp2p.error")
local kad_protocol = require("lua_libp2p.kad_dht.protocol")
local memory_datastore = require("lua_libp2p.datastore.memory")
local multihash = require("lua_libp2p.multiformats.multihash")
local providers = require("lua_libp2p.kad_dht.providers")
local sqlite = require("lua_libp2p.datastore.sqlite")
local varint = require("lua_libp2p.multiformats.varint")
local CONTENT_KEY = assert(multihash.sha2_256("cid-key"))

local function fake_hash(value)
  local map = {
    local_peer = string.char(0) .. string.rep("\0", 31),
    provider_a = string.char(128) .. string.rep("\0", 31),
    provider_b = string.char(129) .. string.rep("\0", 31),
    [CONTENT_KEY] = string.char(128) .. string.rep("\0", 31),
  }
  return map[value] or (string.char(255) .. string.rep("\0", 31))
end

local function hex_encode(value)
  return (value:gsub(".", function(c)
    return string.format("%02x", c:byte())
  end))
end

local function new_host()
  local host = {
    _peer = { id = "local_peer" },
    peerstore = {
      _merged = {},
    },
  }
  function host:peer_id()
    return self._peer
  end
  function host.peerstore:merge(peer_id, info, opts)
    self._merged[peer_id] = info
    self._merge_opts = self._merge_opts or {}
    self._merge_opts[peer_id] = opts
    return true
  end
  function host.peerstore:get_addrs()
    return {}
  end
  return host
end

local function new_lifecycle_host()
  local host = new_host()
  host.tasks = {}
  host.cancelled = {}
  host.events = {}
  function host:spawn_task(name, fn)
    local task = {
      id = #self.tasks + 1,
      name = name,
      fn = fn,
    }
    self.tasks[#self.tasks + 1] = task
    return task
  end
  function host:cancel_task(id)
    self.cancelled[#self.cancelled + 1] = id
    return true
  end
  function host:emit(name, payload)
    self.events[#self.events + 1] = {
      name = name,
      payload = payload,
    }
    return true
  end
  return host
end

local function new_scripted_stream(message)
  local payload = assert(kad_protocol.encode_message(message))
  local frame = assert(varint.encode_u64(#payload)) .. payload
  local stream = {
    _in = frame,
    _out = "",
    _reset = false,
  }
  function stream:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  function stream:write(payload_bytes)
    self._out = self._out .. payload_bytes
    return true
  end
  function stream:close_write()
    return true
  end
  function stream:reset()
    self._reset = true
    return true
  end
  return stream
end

local function read_response(stream)
  local reader = { _in = stream._out }
  function reader:read(n)
    if #self._in < n then
      return nil, "unexpected EOF"
    end
    local out = self._in:sub(1, n)
    self._in = self._in:sub(n + 1)
    return out
  end
  return kad_protocol.read(reader)
end

local function run()
  local now = 1000
  local store = providers.new({
    default_ttl_seconds = 10,
    now = function()
      return now
    end,
  })

  local added, add_err = store:add(CONTENT_KEY, {
    peer_id = "provider_a",
    addrs = { "/ip4/8.8.8.8/tcp/4001" },
  })
  if not added then
    return nil, add_err
  end
  local entries, entries_err = store:get(CONTENT_KEY)
  if not entries then
    return nil, entries_err
  end
  if #entries ~= 1 or entries[1].peer_id ~= "provider_a" then
    return nil, "provider store should return stored provider"
  end
  if entries[1].first_seen_at ~= 1000 or entries[1].updated_at ~= 1000 or entries[1].expires_at ~= 1010 then
    return nil, "provider store should set provider timestamps"
  end

  now = 1011
  entries = assert(store:get(CONTENT_KEY))
  if #entries ~= 0 then
    return nil, "provider store should expire provider entries"
  end

  local raw_provider_store = memory_datastore.new()
  local corrupt_provider_key = "kad_dht/providers/" .. hex_encode(CONTENT_KEY) .. "/" .. hex_encode("bad-peer")
  assert(raw_provider_store:put(corrupt_provider_key, {
    key = CONTENT_KEY,
    peer_id = "bad-peer",
    addrs = { 123 },
  }))
  local corrupt_provider_store = providers.new({ datastore = raw_provider_store })
  local corrupt_entries, corrupt_entries_err = corrupt_provider_store:get(CONTENT_KEY)
  if not corrupt_entries then
    return nil, corrupt_entries_err
  end
  if #corrupt_entries ~= 0 or raw_provider_store:get(corrupt_provider_key) ~= nil then
    return nil, "provider store should delete and ignore malformed persisted entries"
  end

  local host = new_host()
  local dht = assert(kad_dht.new(host, {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
    provider_ttl_seconds = 60,
  }))

  local add_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = CONTENT_KEY,
    provider_peers = {
      { id = "provider_a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
    },
  })
  local handled, handle_err = dht:_handle_rpc(add_stream)
  if not handled then
    return nil, handle_err
  end
  if add_stream._out ~= "" then
    return nil, "ADD_PROVIDER should not write a response on success"
  end
  local local_providers = assert(dht:get_local_providers(CONTENT_KEY))
  if #local_providers ~= 1 or local_providers[1].peer_id ~= "provider_a" then
    return nil, "ADD_PROVIDER should store provider locally"
  end
  if not host.peerstore._merged.provider_a then
    return nil, "ADD_PROVIDER should merge provider addrs into peerstore"
  end
  if
    not host.peerstore._merge_opts.provider_a
    or host.peerstore._merge_opts.provider_a.ttl ~= kad_dht.DEFAULT_PROVIDER_ADDR_TTL_SECONDS
  then
    return nil, "ADD_PROVIDER should merge provider addrs with provider address ttl"
  end

  local custom_ttl_host = new_host()
  local custom_ttl_dht = assert(kad_dht.new(custom_ttl_host, {
    mode = "server",
    hash_function = fake_hash,
    address_filter = "all",
    provider_addr_ttl_seconds = 123,
  }))
  local custom_ttl_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = CONTENT_KEY,
    provider_peers = {
      { id = "provider_a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
    },
  })
  assert(custom_ttl_dht:_handle_rpc(custom_ttl_stream))
  if
    not custom_ttl_host.peerstore._merge_opts.provider_a
    or custom_ttl_host.peerstore._merge_opts.provider_a.ttl ~= 123
  then
    return nil, "provider_addr_ttl_seconds override should control peerstore provider addr ttl"
  end

  local invalid_add_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = "not-a-multihash",
    provider_peers = {
      { id = "provider_a", addrs = { "/ip4/8.8.8.8/tcp/4001" } },
    },
  })
  local invalid_handled, invalid_handle_err = dht:_handle_rpc(invalid_add_stream)
  if invalid_handled ~= nil or not invalid_handle_err then
    return nil, "invalid ADD_PROVIDER should fail"
  end
  if invalid_add_stream._out ~= "" or invalid_add_stream._reset ~= true then
    return nil, "invalid ADD_PROVIDER should reset without writing a response"
  end

  local get_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = CONTENT_KEY,
  })
  handled, handle_err = dht:_handle_rpc(get_stream)
  if not handled then
    return nil, handle_err
  end
  local get_response, get_response_err = read_response(get_stream)
  if not get_response then
    return nil, get_response_err
  end
  if get_response.type ~= kad_protocol.MESSAGE_TYPE.GET_PROVIDERS then
    return nil, "GET_PROVIDERS should return GET_PROVIDERS response"
  end
  if #get_response.provider_peers ~= 1 or get_response.provider_peers[1].id ~= "provider_a" then
    return nil, "GET_PROVIDERS should return local provider peers"
  end

  local invalid_get_stream = new_scripted_stream({
    type = kad_protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = "not-a-multihash",
  })
  invalid_handled, invalid_handle_err = dht:_handle_rpc(invalid_get_stream)
  if invalid_handled ~= nil or not invalid_handle_err then
    return nil, "invalid GET_PROVIDERS should fail"
  end
  if invalid_get_stream._out ~= "" or invalid_get_stream._reset ~= true then
    return nil, "invalid GET_PROVIDERS should reset without writing a response"
  end

  local client_dht = assert(kad_dht.new(new_host(), {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local rejected, rejected_err = client_dht:_handle_add_provider({
    type = kad_protocol.MESSAGE_TYPE.ADD_PROVIDER,
    key = CONTENT_KEY,
    provider_peers = {
      { id = "provider_b" },
    },
  })
  if rejected ~= nil or not rejected_err or rejected_err.kind ~= "unsupported" then
    return nil, "client-mode dht should reject provider writes"
  end

  local invalid_provider_key = "not-a-multihash"
  local invalid_added, invalid_added_err = dht:add_provider(invalid_provider_key, {
    peer_id = "provider_a",
    addrs = { "/ip4/8.8.8.8/tcp/4001" },
  })
  if invalid_added ~= nil or not invalid_added_err or invalid_added_err.kind ~= "input" then
    return nil, "provider APIs should reject keys that are not valid multihashes"
  end
  local oversized_provider_key = assert(multihash.identity(string.rep("x", 79)))
  local oversized_result, oversized_err = dht:_handle_get_providers({
    type = kad_protocol.MESSAGE_TYPE.GET_PROVIDERS,
    key = oversized_provider_key,
  })
  if oversized_result ~= nil or not oversized_err or oversized_err.kind ~= "input" then
    return nil, "provider RPCs should reject keys over 80 bytes"
  end

  local rpc_request
  dht._rpc = function(_, peer_or_addr, request, expected_type, opts)
    if peer_or_addr ~= "peer-a" then
      return nil, "unexpected add provider target"
    end
    if not opts or opts.add_provider ~= true then
      return nil, "ADD_PROVIDER should mark the RPC as provider publication"
    end
    rpc_request = request
    return {
      type = expected_type,
      key = request.key,
      closer_peers = {},
    }
  end
  local add_result, add_result_err = dht:_add_provider("peer-a", CONTENT_KEY, {
    peer_id = "local_peer",
    addrs = { "/ip4/8.8.4.4/tcp/4001" },
  })
  if not add_result then
    return nil, add_result_err
  end
  if
    not rpc_request
    or rpc_request.type ~= kad_protocol.MESSAGE_TYPE.ADD_PROVIDER
    or rpc_request.key ~= CONTENT_KEY
    or #rpc_request.provider_peers ~= 1
    or rpc_request.provider_peers[1].id ~= "local_peer"
  then
    return nil, "_add_provider should send ADD_PROVIDER request with provider peer"
  end

  local no_response_host = new_host()
  local no_response_dht = assert(kad_dht.new(no_response_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  function no_response_host:new_stream()
    local stream = {}
    function stream:write()
      return true
    end
    function stream:close_write()
      return true
    end
    function stream:read()
      return nil, error_mod.new("closed", "yamux stream closed during read")
    end
    return stream
  end
  local no_response_result, no_response_err = no_response_dht:_add_provider("peer-a", CONTENT_KEY, {
    peer_id = "local_peer",
    addrs = { "/ip4/8.8.4.4/tcp/4001" },
  })
  if not no_response_result then
    return nil, no_response_err
  end

  local reset_host = new_host()
  local reset_dht = assert(kad_dht.new(reset_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  function reset_host:new_stream()
    local stream = {}
    function stream:write()
      return true
    end
    function stream:close_write()
      return true
    end
    function stream:read()
      return nil, error_mod.new("closed", "yamux stream is reset")
    end
    return stream
  end
  local reset_result, reset_err = reset_dht:_add_provider("peer-a", CONTENT_KEY, {
    peer_id = "local_peer",
    addrs = { "/ip4/8.8.4.4/tcp/4001" },
  })
  if reset_result ~= nil or not reset_err or reset_err.message ~= "yamux stream is reset" then
    return nil, "ADD_PROVIDER no-response compatibility should not mask stream reset errors"
  end

  local provide_host = new_host()
  function provide_host:get_multiaddrs_raw()
    return { "/ip4/8.8.4.4/tcp/4001" }
  end
  function provide_host:spawn_task(_, fn)
    local task = { id = 1 }
    local function pack(...)
      return { n = select("#", ...), ... }
    end
    local ctx = {
      checkpoint = function()
        return true
      end,
    }
    local results = pack(fn(ctx))
    task.results = results
    if results[1] == nil and results[2] ~= nil then
      task.status = "failed"
      task.error = results[2]
    else
      task.status = "completed"
      task.result = results[1]
    end
    return task
  end
  function provide_host:run_until_task(task)
    if task.status ~= "completed" then
      return nil, task.error
    end
    return table.unpack(task.results, 1, task.results.n)
  end
  local provide_dht = assert(kad_dht.new(provide_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  provide_dht._get_closest_peers = function(_, key, opts)
    if key ~= CONTENT_KEY or not opts.scheduler_task then
      return nil, "provide should run closest-peer lookup as scheduler task"
    end
    return {
      { peer_id = "server-a", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
      { peer_id = "server-b", addrs = { "/ip4/1.1.1.2/tcp/4001" } },
    }, { termination = "test" }
  end
  local announced = {}
  provide_dht._add_provider = function(_, target, key, provider_info)
    announced[#announced + 1] = {
      target = target,
      key = key,
      provider = provider_info,
    }
    return { key = key, closer_peers = {} }
  end
  local op, op_err = provide_dht:provide(CONTENT_KEY, { count = 2 })
  if not op then
    return nil, op_err
  end
  local report, report_err = op:result({ timeout = 1 })
  if not report then
    return nil, report_err
  end
  if report.attempted ~= 2 or report.succeeded ~= 2 or report.failed ~= 0 or #announced ~= 2 then
    return nil, "provide should announce to closest peers and report successes"
  end
  if announced[1].key ~= CONTENT_KEY or announced[1].provider.peer_id ~= "local_peer" then
    return nil, "provide should announce local provider info"
  end
  local stored_local = assert(provide_dht:get_local_providers(CONTENT_KEY))
  if #stored_local ~= 1 or stored_local[1].peer_id ~= "local_peer" then
    return nil, "provide should store local provider record"
  end

  local keys, keys_err = provide_dht:list_local_provider_keys()
  if not keys then
    return nil, keys_err
  end
  if #keys ~= 1 or keys[1] ~= CONTENT_KEY then
    return nil, "list_local_provider_keys should return keys provided by local peer"
  end

  local foreign_store, foreign_store_err = provide_dht.provider_store:add("foreign-key", {
    peer_id = "remote-peer",
    addrs = { "/ip4/2.2.2.2/tcp/4001" },
  })
  if not foreign_store then
    return nil, foreign_store_err
  end
  keys = assert(provide_dht:list_local_provider_keys())
  if #keys ~= 1 or keys[1] ~= CONTENT_KEY then
    return nil, "list_local_provider_keys should ignore remote provider records"
  end

  local reprovided = {}
  provide_dht._provide = function(_, key, opts)
    reprovided[#reprovided + 1] = {
      key = key,
      opts = opts,
    }
    return { key = key, attempted = 1, succeeded = 1, failed = 0 }
  end
  local reprovide_op, reprovide_op_err = provide_dht:reprovide({
    peers = {
      { peer_id = "server-a", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
    },
  })
  if not reprovide_op then
    return nil, reprovide_op_err
  end
  local reprovide_report, reprovide_err = reprovide_op:result({ timeout = 1 })
  if not reprovide_report then
    return nil, reprovide_err
  end
  if reprovide_report.attempted ~= 1 or reprovide_report.succeeded ~= 1 or reprovide_report.failed ~= 0 then
    return nil, "reprovide should report local provider re-announcements"
  end
  if
    #reprovide_report.items ~= 1
    or reprovide_report.items[1].key ~= CONTENT_KEY
    or reprovide_report.items[1].ok ~= true
  then
    return nil, "reprovide should include per-key item results"
  end
  if #reprovided ~= 1 or reprovided[1].key ~= CONTENT_KEY then
    return nil, "reprovide should only re-announce local provider keys"
  end

  reprovided = {}
  reprovide_report = assert(provide_dht:_reprovide({
    keys = { "explicit-key" },
    peers = {
      { peer_id = "server-a", addrs = { "/ip4/1.1.1.1/tcp/4001" } },
    },
  }))
  if reprovide_report.attempted ~= 1 or #reprovided ~= 1 or reprovided[1].key ~= "explicit-key" then
    return nil, "reprovide should honor explicit key list"
  end

  local invalid_parallel, invalid_parallel_err = provide_dht:_reprovide({ keys = { "x" }, max_parallel = 0 })
  if invalid_parallel ~= nil or not invalid_parallel_err or invalid_parallel_err.kind ~= "input" then
    return nil, "reprovider_max_parallel should reject values below one"
  end

  local parallel_host = new_host()
  parallel_host.spawned = {}
  function parallel_host:spawn_task(name, fn)
    local result, err = fn()
    local task = {
      name = name,
      status = result and "completed" or "failed",
      result = result,
      error = err,
    }
    self.spawned[#self.spawned + 1] = task
    return task
  end
  local parallel_dht = assert(kad_dht.new(parallel_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  local parallel_provided = {}
  function parallel_dht:_provide(key)
    parallel_provided[#parallel_provided + 1] = key
    return { key = key, attempted = 1, succeeded = 1, failed = 0 }
  end
  local parallel_ctx = {
    await_any_task = function()
      return true
    end,
  }
  local parallel_report = assert(parallel_dht:_reprovide({
    keys = { "a", "b", "c" },
    max_parallel = 2,
    ctx = parallel_ctx,
  }))
  if
    parallel_report.attempted ~= 3
    or parallel_report.succeeded ~= 3
    or #parallel_host.spawned ~= 3
    or #parallel_provided ~= 3
  then
    return nil, "reprovider_max_parallel should schedule item tasks when a task context is available"
  end

  local event_host = new_lifecycle_host()
  local event_dht = assert(kad_dht.new(event_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
  }))
  event_dht._provide = function(_, key)
    if key == "bad-key" then
      return nil, "bad-key failed"
    end
    return { key = key, attempted = 1, succeeded = 1, failed = 0 }
  end
  local event_report = assert(event_dht:_reprovide({ keys = { "good-key", "bad-key" } }))
  if event_report.attempted ~= 2 or event_report.succeeded ~= 1 or event_report.failed ~= 1 then
    return nil, "reprovide should continue after item failure by default"
  end
  if #event_report.items ~= 2 or event_report.items[1].ok ~= true or event_report.items[2].ok ~= false then
    return nil, "reprovide should track success and failure item results"
  end
  if
    #event_host.events ~= 2
    or event_host.events[1].name ~= "kad_dht:reprovide:item"
    or event_host.events[1].payload.key ~= "good-key"
    or event_host.events[1].payload.ok ~= true
    or event_host.events[2].payload.key ~= "bad-key"
    or event_host.events[2].payload.ok ~= false
  then
    return nil, "reprovide should emit per-key item events"
  end

  local lifecycle_host = new_lifecycle_host()
  local lifecycle_dht = assert(kad_dht.new(lifecycle_host, {
    mode = "client",
    hash_function = fake_hash,
    address_filter = "all",
    reprovider_enabled = true,
    reprovider_interval_seconds = 1,
    reprovider_initial_delay_seconds = 0.25,
    reprovider_jitter_seconds = 0.5,
    reprovider_random = function()
      return 0.5
    end,
    reprovider_batch_size = 7,
    reprovider_max_parallel = 4,
    reprovider_timeout = 3,
  }))
  local started, start_err = lifecycle_dht:start()
  if not started then
    return nil, start_err
  end
  if not lifecycle_dht._reprovider_task then
    return nil, "reprovider should spawn background task when enabled"
  end
  local task = nil
  for _, candidate in ipairs(lifecycle_host.tasks) do
    if candidate.name == "kad.reprovider" then
      task = candidate
      break
    end
  end
  if not task then
    return nil, "reprovider should spawn background task when enabled"
  end
  local reprovide_calls = 0
  lifecycle_dht._reprovide = function(_, opts)
    reprovide_calls = reprovide_calls + 1
    if opts.batch_size ~= 7 or opts.timeout ~= 3 or opts.max_parallel ~= 4 then
      return nil, "reprovider task should pass configured options"
    end
    lifecycle_dht._running = false
    return { attempted = 0, succeeded = 0, failed = 0 }
  end
  local sleeps = {}
  local ctx = {
    sleep = function(_, seconds)
      sleeps[#sleeps + 1] = seconds
      return true
    end,
    checkpoint = function()
      return true
    end,
  }
  local task_ok, task_err = task.fn(ctx)
  if not task_ok then
    return nil, task_err
  end
  if reprovide_calls ~= 1 then
    return nil, "reprovider task should call _reprovide"
  end
  if #sleeps ~= 1 or sleeps[1] ~= 0.5 then
    return nil, "reprovider should apply initial delay and jitter"
  end
  if
    #lifecycle_host.events ~= 2
    or lifecycle_host.events[1].name ~= "kad_dht:reprovide:start"
    or lifecycle_host.events[1].payload.max_parallel ~= 4
    or lifecycle_host.events[2].name ~= "kad_dht:reprovide:complete"
  then
    return nil, "reprovider task should emit lifecycle events"
  end
  lifecycle_dht._running = true
  lifecycle_dht:stop()
  local reprovider_cancelled = false
  for _, cancelled_id in ipairs(lifecycle_host.cancelled) do
    if cancelled_id == task.id then
      reprovider_cancelled = true
      break
    end
  end
  if not reprovider_cancelled or lifecycle_dht._reprovider_task ~= nil then
    return nil, "stop should cancel reprovider task"
  end

  local has_luasql = pcall(require, "luasql.sqlite3")
  if has_luasql then
    local path = os.tmpname()
    os.remove(path)
    local sqlite_store, sqlite_store_err = sqlite.new({ path = path })
    if not sqlite_store then
      os.remove(path)
      return nil, sqlite_store_err
    end
    local persisted_dht = assert(kad_dht.new(new_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = sqlite_store,
      provider_ttl_seconds = false,
    }))
    local persisted, persisted_err = persisted_dht:add_provider(CONTENT_KEY, {
      peer_id = "provider_a",
      addrs = { "/ip4/8.8.8.8/tcp/4001" },
    })
    if not persisted then
      sqlite_store:close()
      os.remove(path)
      return nil, persisted_err
    end
    sqlite_store:close()

    local reopened_store, reopened_store_err = sqlite.new({ path = path })
    if not reopened_store then
      os.remove(path)
      return nil, reopened_store_err
    end
    local reopened_dht = assert(kad_dht.new(new_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = reopened_store,
      provider_ttl_seconds = false,
    }))
    local persisted_providers, persisted_providers_err = reopened_dht:get_local_providers(CONTENT_KEY)
    if not persisted_providers then
      reopened_store:close()
      os.remove(path)
      return nil, persisted_providers_err
    end
    if
      #persisted_providers ~= 1
      or persisted_providers[1].peer_id ~= "provider_a"
      or persisted_providers[1].addrs[1] ~= "/ip4/8.8.8.8/tcp/4001"
    then
      reopened_store:close()
      os.remove(path)
      return nil, "sqlite-backed provider store should reload records in a new dht"
    end
    reopened_store:close()
    os.remove(path)

    local expired_path = os.tmpname()
    os.remove(expired_path)
    local expired_now = 1000
    local expired_store, expired_store_err = sqlite.new({ path = expired_path })
    if not expired_store then
      os.remove(expired_path)
      return nil, expired_store_err
    end
    local expiring_dht = assert(kad_dht.new(new_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = expired_store,
      provider_ttl_seconds = 10,
      now = function()
        return expired_now
      end,
    }))
    local expiring, expiring_err = expiring_dht:add_provider(CONTENT_KEY, {
      peer_id = "provider_a",
      addrs = { "/ip4/8.8.8.8/tcp/4001" },
    })
    if not expiring then
      expired_store:close()
      os.remove(expired_path)
      return nil, expiring_err
    end
    expired_store:close()

    expired_now = 1011
    local expired_reopened_store, expired_reopened_store_err = sqlite.new({ path = expired_path })
    if not expired_reopened_store then
      os.remove(expired_path)
      return nil, expired_reopened_store_err
    end
    local expired_reopened_dht = assert(kad_dht.new(new_host(), {
      mode = "server",
      hash_function = fake_hash,
      address_filter = "all",
      datastore = expired_reopened_store,
      provider_ttl_seconds = 10,
      now = function()
        return expired_now
      end,
    }))
    local expired_providers, expired_providers_err = expired_reopened_dht:get_local_providers(CONTENT_KEY)
    if not expired_providers then
      expired_reopened_store:close()
      os.remove(expired_path)
      return nil, expired_providers_err
    end
    if #expired_providers ~= 0 then
      expired_reopened_store:close()
      os.remove(expired_path)
      return nil, "sqlite-backed provider store should not reload expired records"
    end
    expired_reopened_store:close()
    os.remove(expired_path)
  end

  return true
end

return {
  name = "kad-dht provider store and server RPCs",
  run = run,
}
