# Luapg - ffi postgresql connector

## Example

``` lua
pg = require('luapg')

conn = pg.connect('postgesql://user:password@host:port/database_name')

-- one result
local rc, err = conn:exec([[ select 1 ]])
if err ~= nil then
  print(err)
end
print(rc[1](0, 0)) -- 1

-- multiresult
local rc, err = conn:exec([[ select 1; select 'a', 'b', 'c' ]])
if err ~= nil then
  print(err)
end
print(rc[1](0, 0)) -- 1

print(rc[2](0, 0)) -- a
print(rc[2](0, 1)) -- b
print(rc[2](0, 2)) -- c

-- parametrized query with execution timeout
local rc, err = conn:exec([[ select $1, $2::money ]], {1, '$5'}, {timeout = 5})
print(rc[1](0, 1))

-- listen/notify
local subscriber = function(conn, chan, payload, id)
    print(chan, payload)
end

conn.subscriber = subscriber
local rc, err = conn:exec([[ listen pubsub ]])

local rc, err = conn:exec([[ notify pubsub, 'Push message' ]])

-- disconnect
conn = nil
collectgarbage('collect')
```

## Force disconnect

- From client size

``` lua
connnection:shutdown()
```

- From server side
``` lua
local backpid = libpq.PQbackendPID(self.conn);
local rc, err = self:exec(([[
     select pg_terminate_backend(%i)
   ]]):format(backpid),
   {}, {timeout=timeout})
if err:find('not open') then
    return true
elseif err:find('server closed') then
    return true
end
return rc, err
```

## Testing

``` bash
tarantoolctl rocks install luatest

export PGHOST="127.0.0.1" PGDATABASE="postgres" PGUSER="postgres" # replace creds
.rocks/bin/luatest -c
```
