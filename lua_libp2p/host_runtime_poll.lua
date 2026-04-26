local error_mod = require("lua_libp2p.error")

local ok_socket, socket = pcall(require, "socket")

local M = {}

local function unwrap_socket(value, seen)
  if type(value) ~= "table" then
    return nil
  end
  local visited = seen or {}
  if visited[value] then
    return nil
  end
  visited[value] = true

  if type(value.socket) == "function" then
    local ok, sock = pcall(value.socket, value)
    if ok and sock then
      return sock
    end
  end

  if value._socket then
    return value._socket
  end
  if value._server then
    return value._server
  end

  if value._raw then
    local sock = unwrap_socket(value._raw, visited)
    if sock then
      return sock
    end
  end
  if value._raw_conn then
    local sock = unwrap_socket(value._raw_conn, visited)
    if sock then
      return sock
    end
  end

  return nil
end

M.unwrap_socket = unwrap_socket

function M.build_ready_map(host_obj, timeout)
  if not (ok_socket and type(socket.select) == "function") then
    return nil
  end

  local read_set = {}
  local by_socket = {}

  for _, listener in ipairs(host_obj._listeners) do
    local sock = unwrap_socket(listener)
    if sock then
      read_set[#read_set + 1] = sock
      by_socket[sock] = { kind = "listener", value = listener }
    end
  end

  for _, entry in ipairs(host_obj._connections) do
    local sock = unwrap_socket(entry.conn)
    if sock then
      read_set[#read_set + 1] = sock
      by_socket[sock] = { kind = "connection", value = entry }
    end
  end

  if #read_set == 0 then
    return {}
  end

  local wait_timeout = timeout
  if wait_timeout == nil then
    wait_timeout = host_obj._accept_timeout or 0
  end

  local readable, _, sel_err = socket.select(read_set, nil, wait_timeout)
  if not readable then
    return nil, error_mod.new("io", "socket select failed", { cause = sel_err })
  end

  local ready = {}
  for _, sock in ipairs(readable) do
    local item = by_socket[sock]
    if item then
      ready[item.value] = true
    end
  end
  return ready
end

function M.start(_host)
  return true, nil, false
end

function M.stop(_host)
  return true
end

function M.poll_once(host, timeout)
  return host:_poll_once_poll(timeout)
end

return M
