package = "luapg"
version = "scm-1"
source = {
   url = "git+https://github.com/tarantool/luapg.git",
   branch = "master",
}
description = {
   summary = "FFI libpq-based Postgresql connector",
   detailed = [[
            Fiber-safe async Postgresql connector
   ]],
   homepage = "https://github.com/tarantool/luapg",
   license = "BSD-2-Clause License"
}
build = {
   type = "builtin",
   modules = {
      luapg = "luapg.lua"
   }
}
