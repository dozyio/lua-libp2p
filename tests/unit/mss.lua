local error_mod = require("lua_libp2p.error")
local mss = require("lua_libp2p.multistream_select.protocol")
local varint = require("lua_libp2p.multiformats.varint")

local function new_scripted_conn(incoming)
  local conn = {
    _in = incoming or "",
    _out = "",
  }

  function conn:read(n)
    local want = n or #self._in
    if want <= 0 then
      return ""
    end
    if #self._in < want then
      return nil, error_mod.new("io", "unexpected EOF")
    end
    local out = self._in:sub(1, want)
    self._in = self._in:sub(want + 1)
    return out
  end

  function conn:write(payload)
    self._out = self._out .. payload
    return true
  end

  function conn:writes()
    return self._out
  end

  return conn
end

local function run()
  local frame_conn = new_scripted_conn("")
  local ok, write_err = mss.write_frame(frame_conn, "/toy/1.0.0")
  if not ok then
    return nil, write_err
  end
  local ok2, write_err2 = mss.write_frame(frame_conn, "na")
  if not ok2 then
    return nil, write_err2
  end

  local frame_reader = new_scripted_conn(frame_conn:writes())
  local f1, f1_err = mss.read_frame(frame_reader)
  if not f1 then
    return nil, f1_err
  end
  if f1 ~= "/toy/1.0.0" then
    return nil, "unexpected first frame"
  end
  local f2, f2_err = mss.read_frame(frame_reader)
  if not f2 then
    return nil, f2_err
  end
  if f2 ~= "na" then
    return nil, "unexpected second frame"
  end

  local oversized_payload = string.rep("a", mss.MAX_PROTOCOL_LENGTH) .. "\n"
  local oversized_frame = assert(varint.encode_u64(#oversized_payload)) .. oversized_payload
  local oversized_conn = new_scripted_conn(oversized_frame)
  local oversized_read, oversized_err = mss.read_frame(oversized_conn)
  if oversized_read ~= nil then
    return nil, "expected oversized multistream frame to fail"
  end
  if not oversized_err or oversized_err.kind ~= "decode" then
    return nil, "expected decode error for oversized multistream frame"
  end

  local p1 = "/toy/1.0.0"
  local success_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(p1)),
  })
  local success_conn = new_scripted_conn(success_in)
  local selected, select_err = mss.select(success_conn, { p1 })
  if not selected then
    return nil, select_err
  end
  if selected ~= p1 then
    return nil, "select did not choose expected protocol"
  end
  local expected_success_writes = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(p1)),
  })
  if success_conn:writes() ~= expected_success_writes then
    return nil, "unexpected select success write sequence"
  end

  local p2 = "/toy/2.0.0"
  local fallback_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(mss.NA)),
    assert(mss.encode_frame(p2)),
  })
  local fallback_conn = new_scripted_conn(fallback_in)
  local fallback_selected, fallback_err = mss.select(fallback_conn, { p1, p2 })
  if not fallback_selected then
    return nil, fallback_err
  end
  if fallback_selected ~= p2 then
    return nil, "select fallback did not choose second protocol"
  end

  local unsupported_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(mss.NA)),
    assert(mss.encode_frame(mss.NA)),
  })
  local unsupported_conn = new_scripted_conn(unsupported_in)
  local unsupported_selected, unsupported_err = mss.select(unsupported_conn, { p1, p2 })
  if unsupported_selected ~= nil then
    return nil, "expected unsupported selection failure"
  end
  if not unsupported_err or unsupported_err.kind ~= "unsupported" then
    return nil, "expected unsupported error kind"
  end

  local router = mss.new_router()
  local handled = false
  local reg_ok, reg_err = router:register(p2, function()
    handled = true
    return true
  end)
  if not reg_ok then
    return nil, reg_err
  end

  local router_in = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(p1)),
    assert(mss.encode_frame(p2)),
  })
  local router_conn = new_scripted_conn(router_in)
  local proto, handler, neg_err = router:negotiate(router_conn)
  if not proto then
    return nil, neg_err
  end
  if proto ~= p2 then
    return nil, "router negotiated unexpected protocol"
  end

  local expected_router_writes = table.concat({
    assert(mss.encode_frame(mss.PROTOCOL_ID)),
    assert(mss.encode_frame(mss.NA)),
    assert(mss.encode_frame(p2)),
  })
  if router_conn:writes() ~= expected_router_writes then
    return nil, "unexpected router write sequence"
  end

  local h_ok, h_err = handler()
  if not h_ok then
    return nil, h_err
  end
  if not handled then
    return nil, "router handler was not executed"
  end

  return true
end

return {
  name = "multistream-select negotiation",
  run = run,
}
