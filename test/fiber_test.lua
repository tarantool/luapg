-- test/feature_test.lua
require('strict').on()

local pg = require('luapg')
local log = require('log')
local fiber = require('fiber')

local t = require('luatest')
local g = t.group('luapg-fiber')

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

--[[
   First fiber interrupt because of timeout
   Second fiber gets no results, becuase timeout shutdown connection
]]
g.test_fiber = function()
   local data = {}
   for i=1,1e5 do
      table.insert(data, 'Hello world')
   end
   data = table.concat(data)

   local errfiber = fiber.create(function(conn)
         local rc, err = conn:exec(([[select pg_sleep(5);
             select '%s'
         ]]):format(data), {}, {timeout=1})
         t.assert(rc == nil, rc)
         t.assert(err ~= nil, err)
                                 end,
      g.conn
   )

   local result = -1
   local checker = fiber.new(function(conn)
         local rc, err = conn:exec([[select 1, 2, 3]])
         if rc ~= nil then
            result = -1
            t.fail('Timeout not shutdown connection')
         end
         t.assert(rc == nil)
         t.assert(err ~= nil)
         result = 0
             end,
      g.conn
   )

   t.helpers.retrying({delay=0.3}, function()
         t.assert(checker:status() == 'dead')
   end)
   t.assert_equals(result, 0, "Checker fiber error")
end

g.test_transaction = function()
   local ok = false
   local begin = fiber.create(function(conn)
         local rc, err = conn:exec([[begin]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert_equals(rc[1], 'command ok')
         ok = true
                              end, g.conn)

   t.helpers.retrying({}, t.assert_equals, ok, true)
   ok = false

   local commit = fiber.create(function(conn)
         local rc, err = conn:exec([[commit]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert_equals(rc[1], 'command ok')
         ok = true
                            end, g.conn)
   t.helpers.retrying({}, t.assert_equals, ok, true)
end

g.test_transaction_abort = function()
   local ok = false
   local begin = fiber.create(function(conn)
         local rc, err = conn:exec([[begin]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert(rc[1] == 'command ok')
         ok = true
                              end, g.conn)
   t.helpers.retrying({}, t.assert_equals, ok, true)
   ok = false

   local raiser = fiber.create(function(conn)
         local rc, err = conn:exec([[select 1/0]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert(type(rc[1]) == 'string')
         ok = true
                              end, g.conn)

   t.helpers.retrying({}, t.assert_equals, ok, true)
   ok = false

   local commit = fiber.create(function(conn)
         local rc, err = conn:exec([[commit]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert(rc[1] == 'command ok')
         ok = true
                               end, g.conn)

   t.helpers.retrying({}, t.assert_equals, ok, true)
   ok = false

   local abort = fiber.create(function(conn)
         local rc, err = conn:exec([[abort]])
         t.assert(err == nil)
         t.assert(rc ~= nil)
         t.assert(rc[1] == 'command ok')
         ok = true
                              end, g.conn)

   t.helpers.retrying({}, t.assert_equals, ok, true)
   ok = false
end
