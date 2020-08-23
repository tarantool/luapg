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

g.test_bad_host = function()
   local conn, err = pg.connect('postgresql://127.0.0.2', 2)
   t.assert(err ~= nil)
   local conn, err = pg.connect('postgresql://127.0.0.1:111', 2)
   t.assert(err ~= nil)

   local origuser = os.environ()['PGUSER']
   os.setenv('PGUSER', 'NOTAVALIDUSER')
   local conn, err = pg.connect('', 2)
   t.assert(err ~= nil)
   os.setenv('PGUSER', origuser)
end


g.test_types = function()
   local conn = g.conn

   local rc, err = conn:exec([[select
       '-32768'::smallint,
       '+32767'::smallint,
       '-2147483648'::integer,
       '+2147483647'::integer,
       '-9223372036854775808'::bigint,
       '+9223372036854775807'::bigint,
       '1234.56789'::numeric,
       '1234.56789'::decimal,
       '1e4'::real,
       '2e5'::double precision,
       '52093.89'::money,

       NULL,
       'Hello world'::text,

       '\xDEADBEEF'::bytea,

       '1997-12-17 07:37:16'::timestamp without time zone,
       '1 hour'::interval,
       true,
       false
       ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(rc[1](0,0) == -32768)
   t.assert(rc[1](0,1) == 32767)
   t.assert(rc[1](0,2) == -2147483648, rc[1](0,2))
   t.assert(rc[1](0,3) == 2147483647, rc[1](0,3))
   t.assert(rc[1](0,4) == tonumber64('-9223372036854775808'))
   t.assert(rc[1](0,5) == tonumber64('9223372036854775807ULL'))
   t.assert(rc[1](0,6) == '1234.56789')
   t.assert(rc[1](0,7) == '1234.56789')

   t.assert(rc[1](0,8) == 10000.0, rc[1](0,8))
   t.assert(rc[1](0,9) == 2e5)

   -- default locale is dollars
   t.assert(rc[1](0,10) == '$52,093.89')
   t.assert(type(rc[1](0,11)) == 'cdata')
   t.assert(rc[1](0,11) == box.NULL)
   t.assert(rc[1](0,12) == 'Hello world')
   -- HEX string with \x escaping (not a binary)
   t.assert(rc[1](0,13) == '\\xdeadbeef', rc[1](0,13))
   t.assert(rc[1](0,14) == '1997-12-17 07:37:16', rc[1](0,14))
   t.assert(rc[1](0,15) == '01:00:00', rc[1](0,15))

   t.assert(rc[1](0,16) == true, rc[1](0,16))
   t.assert(rc[1](0,17) == false, rc[1](0,17))
end

g.test_values = function()
   local conn = g.conn

   local rc, err = conn:exec([[
     VALUES (1, 'one'), (2, 'two'), (3, 'three');
   ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(rc[1](0,0) == 1)
   t.assert(rc[1](0,1) == 'one')
   t.assert(rc[1](2,0) == 3)
   t.assert(rc[1](2,1) == 'three')
end

g.test_transaction = function()
   local conn = g.conn

   local rc, err = conn:exec([[
       begin;
       select 1;
       commit;
       ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(rc[1] == 'command ok', rc[1])
   t.assert(rc[2](0,0) == 1, rc[2])
   t.assert(rc[3] == 'command ok', rc[3])
end

g.test_transaction_abort = function()
   local conn = g.conn

   local rc, err = conn:exec([[
       begin;
       create table test(id serial);
       insert into test values(default) returning *;
       abort;
       ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(rc[1] == 'command ok', rc[1])
   t.assert(rc[2] == 'command ok', rc[2])
   t.assert(rc[3](0,0) == 1, rc[3])
   t.assert(rc[4] == 'command ok', rc[3])

   local rc, err = conn:exec([[
       select * from test;
       ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(type(rc[1]) == 'string' , rc[1])
   t.assert(rc[1]:find('does not exist'), rc[1])
end

g.test_force_disconnect = function()
   local conn = g.conn

   local sock = pg.libpq.PQsocket(conn.conn)
   t.assert(sock ~= -1, sock)

   local backpid = pg.libpq.PQbackendPID(conn.conn);
   local rc, err = conn:exec(([[
     select pg_terminate_backend(%i)
   ]]):format(backpid),
      {}, {timeout=timeout})
   t.assert(err:find('not open') or err:find('server closed'), err)

   t.helpers.retrying({}, function()
         t.assert(conn._notify_worker:status() == 'dead')
   end)
   local sock = pg.libpq.PQsocket(conn.conn)
   t.assert(sock == -1, sock)

   local weak_ref = setmetatable({conn=g.conn}, {__mode='v'})
   t.assert(weak_ref.conn~=nil)
   conn = nil
   g.conn = nil
   collectgarbage('collect')
   t.assert(weak_ref.conn==nil)
end

g.test_field_names = function()
   local conn = g.conn

   local rc, err = conn:exec([[
       select 1 as A, 2 as B, 3 as C
       ]])
   t.assert(rc ~= nil, err)
   t.assert(err == nil, err)

   t.assert(rc[1](0,'A') == 1, rc[1])
   t.assert(rc[1](0,'B') == 2, rc[1])
   t.assert(rc[1](0,'C') == 3, rc[1])
end

g.test_wrong_sql = function()
   local conn = g.conn

   local rc, err = conn:exec([[
     select "sdfasdfa
]])
   t.assert(rc ~= nil)
   t.assert(type(rc[1])=='string', rc[1])
   t.assert(err == nil)
end

g.test_shutdown = function()
   local conn = g.conn

   local sock = pg.libpq.PQsocket(conn.conn)
   t.assert(sock ~= -1, sock)

   conn:shutdown()

   local rc, err = conn:exec([[ select 1; ]])
   t.assert(rc == nil)
   t.assert(err ~= nil)

   local sock = pg.libpq.PQsocket(conn.conn)
   t.assert(sock == -1, sock)

   local weak_ref = setmetatable({conn=g.conn}, {__mode='v'})
   t.assert(weak_ref.conn~=nil)
   conn = nil
   g.conn = nil
   collectgarbage('collect')
   t.assert(weak_ref.conn==nil)
end
