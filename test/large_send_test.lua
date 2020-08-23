-- test/feature_test.lua
local pg = require('luapg')
local log = require('log')

local t = require('luatest')
local g = t.group('luapg_large_send')

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


g.test_large_send = function()
   local count = 1e5
   local conn = g.conn
   local selects = {}
   for i=1,count do
      table.insert(selects, 'select 1;')
   end
   local query = table.concat(selects)
   local rc, err = conn:exec(query)
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)
   t.assert(#rc == count)

   local rc, err = conn:exec([[select 1, 'a', NULL, 2.33, 4::bigint]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)
end
