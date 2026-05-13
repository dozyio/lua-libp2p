--- Custom LuaLS documentation exporter.
--
-- LuaLS's built-in exporter is intentionally type-centric. This wrapper keeps
-- the built-in collector/JSON output, but emits Markdown next to each public
-- module so it is easier to browse as project API documentation.

local jsonb = require("json-beautify")
local util = require("utility")

local function text(value)
  if value == nil then
    return ""
  end
  return tostring(value)
end

local function first_sentence(value)
  local s = text(value):gsub("\r\n", "\n")
  local first = s:match("^([^\n]+)") or s
  return first:gsub("%s+$", "")
end

local function clean_heading(value)
  local s = text(value)
  if s == "" then
    return "Unnamed"
  end
  return s:gsub("[#\n\r]", " "):gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")
end

local function code_block(out, lang, body)
  if body == nil or body == "" then
    return
  end
  out[#out + 1] = "```" .. (lang or "")
  out[#out + 1] = tostring(body)
  out[#out + 1] = "```"
  out[#out + 1] = ""
end

local function paragraph(out, body)
  if body == nil or body == "" then
    return
  end
  out[#out + 1] = tostring(body)
  out[#out + 1] = ""
end

local function normalize_desc(value)
  return text(value):gsub("\r\n", "\n"):gsub("^%s+", ""):gsub("%s+$", ""):gsub("%s+", " ")
end

local function dirname(path)
  local dir = tostring(path):match("^(.*)/[^/]+$")
  return dir or "."
end

local function strip_lua(path)
  return tostring(path):gsub("%.lua$", "")
end

local function read_file(path)
  local f = io.open(path, "r")
  if not f then
    return nil
  end
  local data = f:read("*a")
  f:close()
  return data
end

local function public_module_files()
  local files = {}
  local seen = {}
  local rockspec = read_file("lua-libp2p-0.1.0-1.rockspec") or ""
  for file in rockspec:gmatch('%["lua_libp2p[^"]+"%]%s*=%s*"([^"]+%.lua)"') do
    if file:sub(1, #"lua_libp2p/") == "lua_libp2p/" and not seen[file] then
      seen[file] = true
      files[#files + 1] = file
    end
  end
  table.sort(files)
  return files
end

local function module_summary(file)
  local data = read_file(file)
  if not data then
    return nil
  end
  local lines = {}
  for line in data:gmatch("([^\n]*)\n?") do
    local doc = line:match("^%-%-%-%s?(.*)$") or line:match("^%-%-%s?(.*)$")
    if doc == nil then
      if #lines > 0 then
        break
      end
    elseif doc:match("^@") or doc:match("^%-%-@") then
      -- Skip annotation/directive lines in the short module summary.
    elseif doc == "" then
      if #lines > 0 and lines[#lines] ~= "" then
        lines[#lines + 1] = ""
      end
    else
      lines[#lines + 1] = doc
    end
  end
  while lines[#lines] == "" do
    lines[#lines] = nil
  end
  if #lines == 0 then
    return nil
  end
  return table.concat(lines, "\n")
end

local function doc_file(doc)
  if doc.file then
    return doc.file
  end
  if doc.defines then
    for _, define in ipairs(doc.defines) do
      if define.file then
        return define.file
      end
    end
  end
  if doc.fields then
    for _, field in ipairs(doc.fields) do
      if field.file then
        return field.file
      end
    end
  end
  return "Types"
end

local function is_project_api_doc(doc)
  local file = doc_file(doc)
  return file:sub(1, #"lua_libp2p/") == "lua_libp2p/"
end

local function output_path_for_file(file)
  if file:sub(-9) == "/init.lua" then
    return dirname(file) .. "/README.md"
  end
  return strip_lua(file) .. ".md"
end

local function title_for_file(file)
  if file:sub(-9) == "/init.lua" then
    return dirname(file):gsub("^lua_libp2p/", "lua_libp2p."):gsub("/", ".")
  end
  return strip_lua(file):gsub("/", ".")
end

local function sort_by_name(a, b)
  local an = text(a.name)
  local bn = text(b.name)
  if an ~= bn then
    return an < bn
  end
  return text(doc_file(a)) < text(doc_file(b))
end

local function render_define(out, define, module_desc)
  if not define then
    return
  end
  if define.extends and define.extends.view then
    code_block(out, "lua", define.extends.view)
  elseif define.view and define.view ~= define.name then
    code_block(out, "lua", define.view)
  end
  if normalize_desc(define.desc) ~= module_desc then
    paragraph(out, define.desc)
  end
end

local function render_fields(out, fields)
  if type(fields) ~= "table" or #fields == 0 then
    return
  end
  out[#out + 1] = "#### Fields"
  out[#out + 1] = ""
  table.sort(fields, sort_by_name)
  local seen = {}
  for _, field in ipairs(fields) do
    local name = clean_heading(field.name)
    if not seen[name] then
      seen[name] = true
      out[#out + 1] = "- `" .. name .. "`"
      local view = field.extends and field.extends.view or field.view
      if view and view ~= "" and view ~= name then
        out[#out] = out[#out] .. ": `" .. tostring(view):gsub("`", "'") .. "`"
      end
      local summary = first_sentence(field.desc)
      if summary ~= "" then
        out[#out] = out[#out] .. " - " .. summary
      end
    end
  end
  out[#out + 1] = ""
end

local function render_doc(out, doc, module_desc)
  out[#out + 1] = "### " .. clean_heading(doc.name)
  out[#out + 1] = ""
  if doc.view and doc.view ~= doc.name then
    code_block(out, "lua", doc.view)
  end
  if normalize_desc(doc.desc) ~= module_desc then
    paragraph(out, doc.desc)
  end
  if doc.defines then
    for _, define in ipairs(doc.defines) do
      render_define(out, define, module_desc)
    end
  end
  render_fields(out, doc.fields)
end

local function collect_docs_by_file(docs)
  table.sort(docs, sort_by_name)
  local by_file = {}
  local files = {}
  local seen_files = {}
  for _, file in ipairs(public_module_files()) do
    by_file[file] = {}
    files[#files + 1] = file
    seen_files[file] = true
  end
  for _, doc in ipairs(docs) do
    if doc and doc.name and doc.name ~= "LuaLS" and is_project_api_doc(doc) then
      local file = doc_file(doc)
      if not by_file[file] then
        by_file[file] = {}
        if not seen_files[file] then
          files[#files + 1] = file
          seen_files[file] = true
        end
      end
      by_file[file][#by_file[file] + 1] = doc
    end
  end
  table.sort(files)
  return by_file, files
end

local function render_index(files)
  local out = {
    "# lua-libp2p API Reference",
    "",
    "## Contents",
    "",
  }
  for _, file in ipairs(files) do
    out[#out + 1] = "- [" .. title_for_file(file) .. "](../../" .. output_path_for_file(file) .. ")"
  end
  out[#out + 1] = ""
  return table.concat(out, "\n") .. "\n"
end

local function render_module_markdown(file, docs)
  local out = {
    "# " .. title_for_file(file),
    "",
  }
  local summary = module_summary(file)
  local normalized_summary = normalize_desc(summary)
  paragraph(out, summary)
  table.sort(docs, sort_by_name)
  if #docs == 0 then
    out[#out + 1] = "No public LuaLS symbols are documented for this module yet."
    out[#out + 1] = ""
  else
    for _, doc in ipairs(docs) do
      render_doc(out, doc, normalized_summary)
    end
  end
  return table.concat(out, "\n") .. "\n"
end

function export.serializeAndExport(docs, output_dir)
  local json_path = output_dir .. "/doc.json"
  local index_path = output_dir .. "/README.md"

  local old_sparse = jsonb.supportSparseArray
  jsonb.supportSparseArray = true
  local json_ok, json_err = util.saveFile(json_path, jsonb.beautify(docs))
  jsonb.supportSparseArray = old_sparse

  local by_file, files = collect_docs_by_file(docs)
  local output_paths = { json_path, index_path }
  local errs = { json_err, nil }

  local index_ok, index_err = util.saveFile(index_path, render_index(files))
  errs[2] = index_err
  local all_ok = json_ok and index_ok

  for _, file in ipairs(files) do
    local path = output_path_for_file(file)
    local ok, err = util.saveFile(path, render_module_markdown(file, by_file[file]))
    output_paths[#output_paths + 1] = path
    errs[#errs + 1] = err
    all_ok = all_ok and ok
  end

  if not all_ok then
    return false, output_paths, errs
  end
  return true, output_paths
end

return export
