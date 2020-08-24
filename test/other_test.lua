-- test/feature_test.lua
local pg = require('luapg')
local log = require('log')

local t = require('luatest')
local g = t.group('luapg-other')

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


g.test_other = function()
   local conn = g.conn
   -- one result
   local rc, err = conn:exec([[ select 1 ]])
   if err ~= nil then
      print(err)
   end
   t.assert_equals(rc[1](0, 0), 1)

   -- multiresult
   local rc, err = conn:exec([[ select 1; select 'a', 'b', 'c' ]])
   if err ~= nil then
      print(err)
   end
   t.assert_equals(rc[1](0, 0), 1)
   t.assert_equals(rc[2](0, 0), 'a')
   t.assert_equals(rc[2](0, 1), 'b')

   local rc, err = conn:exec([[ select $1, $2::money ]], {1, '$5'}, {timeout = 5})
   t.assert_equals(rc[1](0, 0), '1')
   t.assert_equals(rc[1](0, 1), '$5.00')
end
