local address_manager = require("lua_libp2p.address_manager")
local igd = require("lua_libp2p.upnp.igd")
local nat = require("lua_libp2p.upnp.nat")
local ssdp = require("lua_libp2p.upnp.ssdp")

local function run()
  local response = assert(ssdp.parse_response(table.concat({
    "HTTP/1.1 200 OK",
    "LOCATION: http://192.168.1.1:1900/rootDesc.xml",
    "ST: urn:schemas-upnp-org:device:InternetGatewayDevice:1",
    "USN: uuid:router::urn:schemas-upnp-org:device:InternetGatewayDevice:1",
    "",
    "",
  }, "\r\n")))
  if response.location ~= "http://192.168.1.1:1900/rootDesc.xml" then
    return nil, "ssdp should parse location header"
  end

  local request = ssdp.build_search_request("upnp:rootdevice", { mx = 1 })
  if not request:find("M%-SEARCH %* HTTP/1%.1") or not request:find("ST: upnp:rootdevice", 1, true) then
    return nil, "ssdp should build M-SEARCH request"
  end
  local service = assert(igd.parse_descriptor([[<?xml version="1.0"?>
<root><device><serviceList><service>
<serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
<controlURL>/upnp/control/WANIPConn1</controlURL>
<SCPDURL>/wanipconnSCPD.xml</SCPDURL>
</service></serviceList></device></root>]], "http://192.168.1.1:1900/rootDesc.xml"))
  if service.control_url ~= "http://192.168.1.1:1900/upnp/control/WANIPConn1" then
    return nil, "igd should resolve relative control URL"
  end
  local soap = igd.soap_envelope("AddPortMapping", { NewInternalClient = "192.168.1.2" }, service.service_type)
  if not soap:find("AddPortMapping", 1, true) or not soap:find("192.168.1.2", 1, true) then
    return nil, "igd should build SOAP envelope"
  end
  local ordered = igd.soap_envelope("AddPortMapping", {
    NewRemoteHost = "",
    NewExternalPort = 4003,
    NewProtocol = "TCP",
    NewInternalPort = 4003,
    NewInternalClient = "192.168.1.124",
    NewEnabled = 1,
    NewPortMappingDescription = "lua-libp2p",
    NewLeaseDuration = 720,
  }, service.service_type)
  local expected_order = {
    "NewRemoteHost",
    "NewExternalPort",
    "NewProtocol",
    "NewInternalPort",
    "NewInternalClient",
    "NewEnabled",
    "NewPortMappingDescription",
    "NewLeaseDuration",
  }
  local previous = 0
  for _, name in ipairs(expected_order) do
    local pos = ordered:find("<" .. name .. ">", 1, true)
    if not pos or pos < previous then
      return nil, "igd should emit AddPortMapping args in goupnp order"
    end
    previous = pos
  end

  local fake_client = {
    mappings = {},
    external_ip = "198.51.100.20",
    get_external_ip = function(self)
      return self.external_ip
    end,
    add_port_mapping = function(self, protocol, internal_port, external_port, internal_client, ttl)
      self.mappings[#self.mappings + 1] = {
        protocol = protocol,
        internal_port = internal_port,
        external_port = external_port,
        internal_client = internal_client,
        ttl = ttl,
      }
      return self.mappings[#self.mappings]
    end,
    get_specific_port_mapping = function(self, protocol, external_port)
      local mapping = self.mappings[#self.mappings]
      return {
        protocol = protocol,
        external_port = external_port,
        internal_client = mapping.internal_client,
        internal_port = mapping.internal_port,
      }
    end,
    get_generic_port_mapping = function(self, index)
      local mapping = self.mappings[index + 1]
      if not mapping then
        return nil, "not found"
      end
      return {
        index = index,
        external_port = mapping.external_port,
        protocol = string.upper(mapping.protocol),
        internal_port = mapping.internal_port,
        internal_client = mapping.internal_client,
        description = "lua-libp2p-test",
      }
    end,
    list_port_mappings = function(self, opts)
      local out = {}
      for index = 0, ((opts and opts.max) or 64) - 1 do
        local mapping = self:get_generic_port_mapping(index)
        if not mapping then
          break
        end
        out[#out + 1] = mapping
      end
      return out
    end,
  }
  local host = {
    address_manager = address_manager.new({
      listen_addrs = {
        "/ip4/192.168.1.44/tcp/4001",
        "/ip4/127.0.0.1/tcp/4002",
      },
    }),
    events = {},
    on = function()
      return true
    end,
    off = function()
      return true
    end,
    emit = function(self, event_name, payload)
      self.events[#self.events + 1] = { name = event_name, payload = payload }
      return true
    end,
  }
  local svc = assert(nat.new(host, {
    client = fake_client,
    ttl = 600,
    auto_confirm_address = true,
  }))
  local started, start_err = svc:start()
  if not started then
    return nil, start_err
  end
  local mapped = host.address_manager:get_public_address_mappings()
  if #mapped ~= 1 or fake_client.mappings[1].internal_port ~= 4001 then
    return nil, "upnp nat should map eligible private transport addr"
  end
  local ext = "/ip4/198.51.100.20/tcp/4001"
  local meta = host.address_manager:get_reachability(ext)
  if not meta or meta.type ~= "ip-mapping" or meta.verified ~= true or meta.status ~= "public" then
    return nil, "upnp nat should add confirmed public mapping metadata"
  end
  if meta.actual_internal_client ~= "192.168.1.44" or meta.actual_internal_port ~= 4001 then
    return nil, "upnp nat should store verified router mapping details"
  end
  local advertised = host.address_manager:get_advertise_addrs()
  local found = false
  for _, addr in ipairs(advertised) do
    if addr == ext then
      found = true
    end
  end
  if not found then
    return nil, "verified public mapping should be advertised"
  end
  if host.events[#host.events].name ~= "upnp_nat:mapping:active" then
    return nil, "upnp nat should emit active mapping event"
  end
  local listed = assert(svc:list_port_mappings({ max = 8 }))
  if #listed ~= 1 or listed[1].internal_client ~= "192.168.1.44" then
    return nil, "upnp nat should enumerate router port mappings"
  end
  host.address_manager:verify_public_address_mapping(ext, {
    source = "autonat_v2",
  })
  fake_client.external_ip = "198.51.100.21"
  local remapped, remap_err = svc:map_ip_addresses()
  if not remapped then
    return nil, remap_err
  end
  if host.address_manager:get_reachability(ext) ~= nil then
    return nil, "changed UPnP external address should remove old mapping metadata"
  end
  local replacement = "/ip4/198.51.100.21/tcp/4001"
  local replacement_meta = host.address_manager:get_reachability(replacement)
  if not replacement_meta or replacement_meta.verified ~= true or replacement_meta.status ~= "public" then
    return nil, "auto-confirmed replacement mapping should be recorded"
  end
  local removed_event = nil
  for _, event in ipairs(host.events) do
    if event.name == "upnp_nat:mapping:removed" then
      removed_event = event.payload
    end
  end
  if not removed_event
    or removed_event.external_addr ~= ext
    or removed_event.replacement_addr ~= replacement
    or removed_event.reason ~= "external_address_changed"
  then
    return nil, "changed UPnP external address should emit removal event"
  end

  return true
end

return {
  name = "upnp ssdp igd and nat service",
  run = run,
}
