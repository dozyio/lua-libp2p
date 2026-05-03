std = "lua54"
max_line_length = false

ignore = {
  "211", -- unused locals are part of the current baseline
  "212", -- method signatures commonly keep self/opts for shape compatibility
  "213", -- unused loop variables
  "231", -- assigned-but-never-accessed locals
  "241", -- mutated-but-never-accessed locals
  "311", -- overwritten values
  "411", -- redefining locals in small scopes
  "431", -- shadowing upvalues
  "432", -- shadowing arguments
  "521", -- unused labels
  "542", -- intentionally empty branches
}

read_globals = {
  "arg",
}
