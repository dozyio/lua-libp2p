--- Host bootstrap discovery scheduling internals.
-- @module lua_libp2p.host.bootstrap
local discovery = require("lua_libp2p.discovery")
local error_mod = require("lua_libp2p.error")
local log = require("lua_libp2p.log").subsystem("host")

local M = {}

local function build_peer_discovery(config)
  if config == nil or config == false then
    return nil
  end
  if type(config) == "table" and type(config.discover) == "function" then
    return config
  end
  if type(config) ~= "table" then
    return nil, nil, error_mod.new("input", "peer_discovery must be a config table or discovery object")
  end

  local sources = {}
  local bootstrap_config = nil

  local function append_source(source_name, source_spec)
    local source
    if type(source_spec) == "table" and type(source_spec.discover) == "function" then
      source = source_spec
    elseif type(source_spec) == "table" and source_spec.module ~= nil then
      local module = source_spec.module
      if type(module) ~= "table" or type(module.new) ~= "function" then
        return nil, error_mod.new("input", "peer_discovery module entry must expose .new(opts)", {
          source = source_name,
        })
      end
      local built_source, built_err = module.new(source_spec.config or {})
      if not built_source then
        return nil, built_err
      end
      source = built_source
    elseif type(source_spec) == "table" and type(source_spec.new) == "function" then
      local built_source, built_err = source_spec.new({})
      if not built_source then
        return nil, built_err
      end
      source = built_source
    else
      return nil, error_mod.new("input", "peer_discovery entries must be source objects or module specs", {
        source = source_name,
      })
    end

    if type(source) ~= "table" or type(source.discover) ~= "function" then
      return nil, error_mod.new("input", "peer_discovery source must provide discover(opts)", {
        source = source_name,
      })
    end
    if source_name == "bootstrap" and type(source._bootstrap_config) == "table" then
      bootstrap_config = source._bootstrap_config
    end
    sources[#sources + 1] = source
    return true
  end

  if type(config.sources) == "table" then
    for idx, source in ipairs(config.sources) do
      local ok, err = append_source("source_" .. tostring(idx), source)
      if not ok then
        return nil, nil, err
      end
    end
  else
    for source_name, source_spec in pairs(config) do
      if source_name ~= "sources" then
        local ok, err = append_source(source_name, source_spec)
        if not ok then
          return nil, nil, err
        end
      end
    end
  end

  if #sources == 0 then
    return nil, nil, error_mod.new("input", "peer_discovery config must include at least one source")
  end
  local discoverer, discoverer_err = discovery.new({ sources = sources })
  if not discoverer then
    return nil, nil, discoverer_err
  end
  return discoverer, bootstrap_config
end

function M.init(host, peer_discovery_config)
  host.peer_discovery = nil
  host._bootstrap_discovery = nil
  host._bootstrap_discovery_done = false
  host._bootstrap_discovery_task = nil

  local peer_discovery, bootstrap_config, peer_discovery_err = build_peer_discovery(peer_discovery_config)
  if peer_discovery_err then
    return nil, peer_discovery_err
  end
  host.peer_discovery = peer_discovery
  if bootstrap_config then
    host._bootstrap_discovery = {
      config = bootstrap_config,
      dial_on_start = bootstrap_config.dial_on_start ~= false,
      timeout = bootstrap_config.timeout or bootstrap_config.delay or 1,
      tag_name = bootstrap_config.tag_name or "bootstrap",
      tag_value = bootstrap_config.tag_value or 50,
      tag_ttl = bootstrap_config.tag_ttl == nil and 120 or bootstrap_config.tag_ttl,
    }
  end
  return true
end

function M.install(Host)
  function Host:_schedule_bootstrap_discovery(delay_seconds)
    local cfg = self._bootstrap_discovery
    if not cfg or self._bootstrap_discovery_done or cfg.dial_on_start == false then
      return true
    end
    local existing = self._bootstrap_discovery_task
    if existing and existing.status ~= "completed" and existing.status ~= "failed" and existing.status ~= "cancelled" then
      return true
    end
    local delay = type(delay_seconds) == "number" and math.max(0, delay_seconds) or 0
    local task, task_err = self:spawn_task("host.bootstrap_discovery", function(ctx)
      if delay > 0 then
        local slept, sleep_err = ctx:sleep(delay)
        if slept == nil and sleep_err then
          return nil, sleep_err
        end
      end
      if not self._running or self._bootstrap_discovery_done then
        return true
      end
      return self:_run_bootstrap_discovery_once()
    end, { service = "host" })
    if not task then
      return nil, task_err
    end
    self._bootstrap_discovery_task = task
    return true
  end

  function Host:_run_bootstrap_discovery_once()
    local cfg = self._bootstrap_discovery
    if not cfg or self._bootstrap_discovery_done or cfg.dial_on_start == false then
      return true
    end
    self._bootstrap_discovery_done = true
    if not self.peer_discovery then
      return true
    end

    local peers, discover_err = self.peer_discovery:discover({
      dialable_only = true,
      ignore_resolve_errors = true,
      ignore_source_errors = true,
    })
    if not peers then
      return nil, discover_err
    end
    log.info("bootstrap discovery completed", {
      discovered = #peers,
    })
    for _, peer in ipairs(peers) do
      if type(peer) == "table" and type(peer.peer_id) == "string" and peer.peer_id ~= "" then
        if self.peerstore then
          self.peerstore:merge(peer.peer_id, { addrs = peer.addrs or {} })
          self.peerstore:tag(peer.peer_id, cfg.tag_name, {
            value = cfg.tag_value,
            ttl = cfg.tag_ttl,
          })
        end
        self:spawn_task("host.bootstrap_dial", function(ctx)
          if self:_find_connection(peer.peer_id) then
            log.debug("bootstrap dial skipped existing connection", {
              peer_id = peer.peer_id,
            })
            return true
          end
          log.debug("bootstrap dial attempt", {
            peer_id = peer.peer_id,
            addr_count = #(peer.addrs or {}),
            addr = (peer.addrs and peer.addrs[1]) or nil,
          })
          local ok, dial_conn, _, dial_err = pcall(function()
            return self:dial({
              peer_id = peer.peer_id,
              addrs = peer.addrs,
            }, {
              timeout = self._connect_timeout,
              io_timeout = self._io_timeout,
              ctx = ctx,
            })
          end)
          if ok and dial_conn then
            log.info("bootstrap dial succeeded", {
              peer_id = peer.peer_id,
            })
          else
            local cause = ok and dial_err or dial_conn
            log.warn("bootstrap dial failed", {
              peer_id = peer.peer_id,
              cause = tostring(cause),
            })
          end
          -- Bootstrap dials are opportunistic; identify events seed interested services.
          return ok == true and dial_conn ~= nil
        end, {
          service = "host",
          peer_id = peer.peer_id,
        })
      end
    end
    return true
  end
end

return M
