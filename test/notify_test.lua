-- test/feature_test.lua
local pg = require('luapg')
local log = require('log')
local fiber = require('fiber')

local t = require('luatest')
local g = t.group('luapg-notify')

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

g.test_notify = function()
   local channel = 'Channel'
   local data = 'push data'
   local count = 100
   local producer = fiber.new(function(conn)
         for i = 1, count do
            local rc, err = conn:exec(([[ notify "%s", '%s'
]]):format(channel, data))
            local rc, err = conn:exec(([[ notify "Other_%s", '%s'
]]):format(channel, data))
            t.assert(err == nil, err)
         end
      end, g.conn)
   producer:name('Producer')

   local received = {}
   local subscriber = function(conn, chan, payload, id)
      t.assert(conn ~= nil)
      t.assert(chan == channel, chan)
      t.assert(payload == data, payload)
      t.assert(id ~= nil)
      table.insert(received, payload)
   end

   g.conn.subscriber = subscriber
   g.conn:exec(([[ listen "%s"]]):format(channel))

   t.helpers.retrying({}, function()
         t.assert(#received == count)
   end)
end


g.test_large_notify = function()
   local channel = 'Channel'
   local data = 'push data'
   local buffer = {}
   for i=1,100 do
      table.insert(buffer, data)
   end
   data = table.concat(buffer)
   local count = 100
   local producer = fiber.new(function(conn)
         for i = 1, count do
            local rc, err = conn:exec(([[ notify "%s", '%s'
]]):format(channel, data))
            local rc, err = conn:exec(([[ notify "Other_%s", '%s'
]]):format(channel, data))
            t.assert(err == nil, err)
         end
       end, g.conn)
   producer:name('Producer')

   local received = {}
   local subscriber = function(conn, chan, payload, id)
      t.assert(conn ~= nil)
      t.assert(chan == channel, chan)
      t.assert(payload == data, payload)
      t.assert(id ~= nil)
      table.insert(received, payload)
   end

   g.conn.subscriber = subscriber
   g.conn:exec(([[ listen "%s"]]):format(channel))

   t.helpers.retrying({timeout=5, delay = 0.1}, function()
         t.assert(#received == count)
   end)
end

g.test_unlisten = function()
   local channel = 'Channel'
   local data = 'push data'
   local count = 100
   local producer = fiber.new(function(conn)
         for i = 1, count do
            local rc, err = conn:exec(([[ notify "%s", '%s'
]]):format(channel, data))
            local rc, err = conn:exec(([[ notify "Other_%s", '%s'
]]):format(channel, data))
            t.assert(err == nil, err)
         end
   end, g.conn)
   producer:name('Producer')

   local received = {}
   local subscriber = function(conn, chan, payload, id)
      t.assert(conn ~= nil)
      t.assert(chan == channel or chan == 'Other_'..channel, chan)
      t.assert(payload == data, payload)
      t.assert(id ~= nil)
      table.insert(received, payload)
   end

   g.conn.subscriber = subscriber
   g.conn:exec(([[ listen "%s"]]):format(channel))
   g.conn:exec(([[ listen "Other_%s"]]):format(channel))

   t.helpers.retrying({}, function()
         t.assert(#received == 2*count)
   end)

   g.conn:exec(([[ unlisten "%s"]]):format(channel))

   received = {}
   for i = 1, count do
      local rc, err = g.conn:exec(([[ notify "%s", '%s'
]]):format(channel, data))
      local rc, err = g.conn:exec(([[ notify "Other_%s", '%s'
]]):format(channel, data))
      t.assert(err == nil, err)
   end

   t.helpers.retrying({}, function()
         t.assert(#received == count)
   end)

   received = {}
   g.conn.subscriber = nil

   for i = 1, count do
      local rc, err = g.conn:exec(([[ notify "%s", '%s'
]]):format(channel, data))
      local rc, err = g.conn:exec(([[ notify "Other_%s", '%s'
]]):format(channel, data))
      t.assert(err == nil, err)
   end
end
