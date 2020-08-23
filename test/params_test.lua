-- test/feature_test.lua
local pg = require('luapg')
local log = require('log')

local t = require('luatest')
local g = t.group('luapg')

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


g.test_params = function()
   local conn = g.conn
   local rc, err = conn:exec([[ select $1::int, $2, $3::date, $4::money;]],
      {1, "a", '2030-01-01', '$5'}
   )
   t.assert(rc ~= nil)
   t.assert(err == nil)
   t.assert_equals(rc[1](0, 0), 1)
   t.assert_equals(rc[1](0, 1), 'a')
   t.assert_equals(rc[1](0, 2), '2030-01-01')
   t.assert_equals(rc[1](0, 3), '$5.00')
end

g.test_not_enough_params = function()
   local conn = g.conn
   local rc, err = conn:exec([[ select $1::int, $2, $3::date, $4::money]],
      {1, "a"}
   )
   t.assert(rc ~= nil)
   t.assert(err == nil)
   t.assert_equals(type(rc[1]), 'string')
end

g.test_not_type_params = function()
   local conn = g.conn
   local rc, err = conn:exec([[ select $1::int]],
      {'aaaaa'}
   )
   t.assert(rc ~= nil)
   t.assert(err == nil)
   t.assert_equals(type(rc[1]), 'string')
end

g.test_not_that_place = function()
   local conn = g.conn
   local rc, err = conn:exec([[ select * from $1]],
      {'aaaaa'}
   )
   t.assert(rc ~= nil)
   t.assert(err == nil)
   t.assert_equals(type(rc[1]), 'string')
end

g.test_multi_statement_params = function()
   local conn = g.conn
   local rc, err = conn:exec([[ select $1::int; select 'Hello']],
      {'aaaaa'}
   )
   t.assert(rc ~= nil)
   t.assert(err == nil)
   t.assert_equals(type(rc[1]), 'string') -- multiple statement
end
