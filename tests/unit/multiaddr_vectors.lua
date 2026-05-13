local multiaddr = require("lua_libp2p.multiformats.multiaddr")
local error_mod = require("lua_libp2p.error")
local vectors = require("tests.helpers.multiaddr_vectors")

local function check_valid(addr)
  local parsed, err = multiaddr.parse(addr)
  if not parsed then
    return nil, string.format("expected valid multiaddr, got error for %s: %s", addr, tostring(err))
  end
  local formatted, fmt_err = multiaddr.format(parsed)
  if not formatted then
    return nil, string.format("format failed for %s: %s", addr, tostring(fmt_err))
  end
  local reparsed, parse_err = multiaddr.parse(formatted)
  if not reparsed then
    return nil, string.format("reparse failed for %s: %s", formatted, tostring(parse_err))
  end
  local formatted2, fmt2_err = multiaddr.format(reparsed)
  if not formatted2 then
    return nil, string.format("second format failed for %s: %s", formatted, tostring(fmt2_err))
  end
  if formatted2 ~= formatted then
    return nil, string.format("roundtrip mismatch for %s", addr)
  end
  return true
end

local function run()
  for _, addr in ipairs(vectors.valid) do
    local ok, err = check_valid(addr)
    if not ok then
      return nil, err
    end
  end

  for _, addr in ipairs(vectors.invalid) do
    local parsed, err = multiaddr.parse(addr)
    if parsed then
      return nil, string.format("expected invalid multiaddr: %s", addr)
    end
    if not err or not error_mod.is_error(err) then
      return nil, string.format("expected typed parse error for invalid multiaddr: %s", addr)
    end
  end

  for _, pair in ipairs(vectors.normalized) do
    local normalized, norm_err = multiaddr.parse(pair.input)
    if not normalized then
      return nil, norm_err
    end
    local normalized_text, format_err = multiaddr.format(normalized)
    if not normalized_text then
      return nil, format_err
    end
    if normalized_text ~= pair.output then
      return nil, string.format("normalization mismatch: expected %s got %s", pair.output, normalized_text)
    end
  end

  return true
end

return {
  name = "multiaddr vectors from go/js subset",
  run = run,
}
