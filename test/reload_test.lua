-- test/feature_test.lua
local pg = require('luapg')
local log = require('log')
local fiber = require('fiber')

local t = require('luatest')
local g = t.group('luapg-reload')

--[[
   export PGHOST="IP"  \
   PGDATABASE="DATABASE" \
   PGUSER="USER" \
   PGPASSWORD="ZZZ"
]]

g.before_each(function()
      local conn, err = pg.connect('')
      t.assert(err == nil, err)
      t.assert(conn ~= nil)
      g.conn = conn
end)

g.after_each(function()
      g.conn = nil
      collectgarbage('collect')
end)

g.test_reload = function()
   package.loaded['luapg'] = nil
   t.assert(require('luapg'), "Could not reload module luapg")
end
