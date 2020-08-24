local log = require('log')
local fiber = require('fiber')

local clock = require('clock')
local socket = require('socket')

local ffi    = require('ffi')
local ffistr = ffi.string

local ok, new_tab = pcall(require, "table.new")
if not ok then
    new_tab = function (narr, nrec) return {} end
end

if not pcall(function() return ffi.C.PQconnectStart end) then
   ffi.cdef([[
typedef unsigned int Oid;
typedef long int pg_int64;
typedef enum ConnStatusType
{
    CONNECTION_OK,
    CONNECTION_BAD,
    CONNECTION_STARTED,
    CONNECTION_MADE,
    CONNECTION_AWAITING_RESPONSE,
    CONNECTION_AUTH_OK,
    CONNECTION_SETENV,
    CONNECTION_SSL_STARTUP,
    CONNECTION_NEEDED
} ConnStatusType;
typedef enum PostgresPollingStatusType
{
    PGRES_POLLING_FAILED = 0,
    PGRES_POLLING_READING,
    PGRES_POLLING_WRITING,
    PGRES_POLLING_OK,
    PGRES_POLLING_ACTIVE
} PostgresPollingStatusType;
typedef enum ExecStatusType
{
    PGRES_EMPTY_QUERY = 0,
    PGRES_COMMAND_OK,
    PGRES_TUPLES_OK,
    PGRES_COPY_OUT,
    PGRES_COPY_IN,
    PGRES_BAD_RESPONSE,
    PGRES_NONFATAL_ERROR,
    PGRES_FATAL_ERROR,
    PGRES_COPY_BOTH,
    PGRES_SINGLE_TUPLE
} ExecStatusType;
typedef enum PGTransactionStatusType
{
    PQTRANS_IDLE,
    PQTRANS_ACTIVE,
    PQTRANS_INTRANS,
    PQTRANS_INERROR,
    PQTRANS_UNKNOWN
} PGTransactionStatusType;
typedef enum PGVerbosity
{
    PQERRORS_TERSE,
    PQERRORS_DEFAULT,
    PQERRORS_VERBOSE
} PGVerbosity;
typedef enum PGContextVisibility
{
    PQSHOW_CONTEXT_NEVER,
    PQSHOW_CONTEXT_ERRORS,
    PQSHOW_CONTEXT_ALWAYS
} PGContextVisibility;
typedef enum PGPing
{
    PQPING_OK,
    PQPING_REJECT,
    PQPING_NO_RESPONSE,
    PQPING_NO_ATTEMPT
} PGPing;
typedef struct pg_conn PGconn;
typedef struct pg_result PGresult;
typedef struct pg_cancel PGcancel;
typedef struct pgNotify
{
    char       *relname;
    int       be_pid;
    char       *extra;
    struct pgNotify *next;
} PGnotify;
typedef void (*PQnoticeReceiver) (void *arg, const PGresult *res);
typedef void (*PQnoticeProcessor) (void *arg, const char *message);
typedef char pqbool;
typedef struct _PQprintOpt
{
    pqbool    header;
    pqbool    align;
    pqbool    standard;
    pqbool    html3;
    pqbool    expanded;
    pqbool    pager;
    char    *fieldSep;
    char    *tableOpt;
    char    *caption;
    char     **fieldName;
} PQprintOpt;
typedef struct _PQconninfoOption
{
    char    *keyword;
    char    *envvar;
    char    *compiled;
    char    *val;
    char    *label;
    char    *dispchar;
    int    dispsize;
} PQconninfoOption;
typedef struct PQArgBlock
{
    int    len;
    int    isint;
    union
    {
        int     *ptr;
        int    integer;
    }            u;
} PQArgBlock;
typedef struct pgresAttDesc
{
    char    *name;
    Oid    tableid;
    int    columnid;
    int    format;
    Oid    typid;
    int    typlen;
    int    atttypmod;
} PGresAttDesc;
extern PGconn *PQconnectStart(const char *conninfo);
extern PGconn *PQconnectStartParams(const char *const * keywords, const char *const * values, int expand_dbname);
extern PostgresPollingStatusType PQconnectPoll(PGconn *conn);
extern PGconn *PQconnectdb(const char *conninfo);
extern PGconn *PQconnectdbParams(const char *const * keywords, const char *const * values, int expand_dbname);
extern PGconn *PQsetdbLogin(const char *pghost, const char *pgport, const char *pgoptions, const char *pgtty, const char *dbName, const char *login, const char *pwd);
extern void PQfinish(PGconn *conn);
extern PQconninfoOption *PQconndefaults(void);
extern PQconninfoOption *PQconninfoParse(const char *conninfo, char **errmsg);
extern PQconninfoOption *PQconninfo(PGconn *conn);
extern void PQconninfoFree(PQconninfoOption *connOptions);
extern int    PQresetStart(PGconn *conn);
extern PostgresPollingStatusType PQresetPoll(PGconn *conn);
extern void PQreset(PGconn *conn);
extern PGcancel *PQgetCancel(PGconn *conn);
extern void PQfreeCancel(PGcancel *cancel);
extern int    PQcancel(PGcancel *cancel, char *errbuf, int errbufsize);
extern int    PQrequestCancel(PGconn *conn);
extern char *PQdb(const PGconn *conn);
extern char *PQuser(const PGconn *conn);
extern char *PQpass(const PGconn *conn);
extern char *PQhost(const PGconn *conn);
extern char *PQport(const PGconn *conn);
extern char *PQtty(const PGconn *conn);
extern char *PQoptions(const PGconn *conn);
extern ConnStatusType PQstatus(const PGconn *conn);
extern PGTransactionStatusType PQtransactionStatus(const PGconn *conn);
extern const char *PQparameterStatus(const PGconn *conn, const char *paramName);
extern int    PQprotocolVersion(const PGconn *conn);
extern int    PQserverVersion(const PGconn *conn);
extern char *PQerrorMessage(const PGconn *conn);
extern int    PQsocket(const PGconn *conn);
extern int    PQbackendPID(const PGconn *conn);
extern int    PQconnectionNeedsPassword(const PGconn *conn);
extern int    PQconnectionUsedPassword(const PGconn *conn);
extern int    PQclientEncoding(const PGconn *conn);
extern int    PQsetClientEncoding(PGconn *conn, const char *encoding);
extern int    PQsslInUse(PGconn *conn);
extern void *PQsslStruct(PGconn *conn, const char *struct_name);
extern const char *PQsslAttribute(PGconn *conn, const char *attribute_name);
extern const char *const * PQsslAttributeNames(PGconn *conn);
extern void *PQgetssl(PGconn *conn);
extern void PQinitSSL(int do_init);
extern void PQinitOpenSSL(int do_ssl, int do_crypto);
extern PGVerbosity PQsetErrorVerbosity(PGconn *conn, PGVerbosity verbosity);
extern PGContextVisibility PQsetErrorContextVisibility(PGconn *conn, PGContextVisibility show_context);
extern void PQuntrace(PGconn *conn);
extern PQnoticeReceiver PQsetNoticeReceiver(PGconn *conn, PQnoticeReceiver proc, void *arg);
extern PQnoticeProcessor PQsetNoticeProcessor(PGconn *conn, PQnoticeProcessor proc, void *arg);
typedef void (*pgthreadlock_t) (int acquire);
extern pgthreadlock_t PQregisterThreadLock(pgthreadlock_t newhandler);
extern PGresult *PQexec(PGconn *conn, const char *query);
extern PGresult *PQexecParams(PGconn *conn, const char *command, int nParams, const Oid *paramTypes, const char *const * paramValues, const int *paramLengths, const int *paramFormats, int resultFormat);
extern PGresult *PQprepare(PGconn *conn, const char *stmtName, const char *query, int nParams, const Oid *paramTypes);
extern PGresult *PQexecPrepared(PGconn *conn, const char *stmtName, int nParams, const char *const * paramValues, const int *paramLengths, const int *paramFormats, int resultFormat);
extern int    PQsendQuery(PGconn *conn, const char *query);
extern int PQsendQueryParams(PGconn *conn, const char *command, int nParams, const Oid *paramTypes, const char *const * paramValues, const int *paramLengths, const int *paramFormats, int resultFormat);
extern int PQsendPrepare(PGconn *conn, const char *stmtName, const char *query, int nParams, const Oid *paramTypes);
extern int PQsendQueryPrepared(PGconn *conn, const char *stmtName, int nParams, const char *const * paramValues, const int *paramLengths, const int *paramFormats, int resultFormat);
extern int    PQsetSingleRowMode(PGconn *conn);
extern PGresult *PQgetResult(PGconn *conn);
extern int    PQisBusy(PGconn *conn);
extern int    PQconsumeInput(PGconn *conn);
extern PGnotify *PQnotifies(PGconn *conn);
extern int    PQputCopyData(PGconn *conn, const char *buffer, int nbytes);
extern int    PQputCopyEnd(PGconn *conn, const char *errormsg);
extern int    PQgetCopyData(PGconn *conn, char **buffer, int async);
extern int    PQgetline(PGconn *conn, char *string, int length);
extern int    PQputline(PGconn *conn, const char *string);
extern int    PQgetlineAsync(PGconn *conn, char *buffer, int bufsize);
extern int    PQputnbytes(PGconn *conn, const char *buffer, int nbytes);
extern int    PQendcopy(PGconn *conn);
extern int    PQsetnonblocking(PGconn *conn, int arg);
extern int    PQisnonblocking(const PGconn *conn);
extern int    PQisthreadsafe(void);
extern PGPing PQping(const char *conninfo);
extern PGPing PQpingParams(const char *const * keywords, const char *const * values, int expand_dbname);
extern int    PQflush(PGconn *conn);
extern PGresult *PQfn(PGconn *conn, int fnid, int *result_buf, int *result_len, int result_is_int, const PQArgBlock *args, int nargs);
extern ExecStatusType PQresultStatus(const PGresult *res);
extern char *PQresStatus(ExecStatusType status);
extern char *PQresultErrorMessage(const PGresult *res);
extern char *PQresultVerboseErrorMessage(const PGresult *res, PGVerbosity verbosity, PGContextVisibility show_context);
extern char *PQresultErrorField(const PGresult *res, int fieldcode);
extern int    PQntuples(const PGresult *res);
extern int    PQnfields(const PGresult *res);
extern int    PQbinaryTuples(const PGresult *res);
extern char *PQfname(const PGresult *res, int field_num);
extern int    PQfnumber(const PGresult *res, const char *field_name);
extern Oid    PQftable(const PGresult *res, int field_num);
extern int    PQftablecol(const PGresult *res, int field_num);
extern int    PQfformat(const PGresult *res, int field_num);
extern Oid    PQftype(const PGresult *res, int field_num);
extern int    PQfsize(const PGresult *res, int field_num);
extern int    PQfmod(const PGresult *res, int field_num);
extern char *PQcmdStatus(PGresult *res);
extern char *PQoidStatus(const PGresult *res);
extern Oid    PQoidValue(const PGresult *res);
extern char *PQcmdTuples(PGresult *res);
extern char *PQgetvalue(const PGresult *res, int tup_num, int field_num);
extern int    PQgetlength(const PGresult *res, int tup_num, int field_num);
extern int    PQgetisnull(const PGresult *res, int tup_num, int field_num);
extern int    PQnparams(const PGresult *res);
extern Oid    PQparamtype(const PGresult *res, int param_num);
extern PGresult *PQdescribePrepared(PGconn *conn, const char *stmt);
extern PGresult *PQdescribePortal(PGconn *conn, const char *portal);
extern int    PQsendDescribePrepared(PGconn *conn, const char *stmt);
extern int    PQsendDescribePortal(PGconn *conn, const char *portal);
extern void PQclear(PGresult *res);
extern void PQfreemem(void *ptr);
extern PGresult *PQmakeEmptyPGresult(PGconn *conn, ExecStatusType status);
extern PGresult *PQcopyResult(const PGresult *src, int flags);
extern int    PQsetResultAttrs(PGresult *res, int numAttributes, PGresAttDesc *attDescs);
extern void *PQresultAlloc(PGresult *res, size_t nBytes);
extern int    PQsetvalue(PGresult *res, int tup_num, int field_num, char *value, int len);
extern size_t PQescapeStringConn(PGconn *conn, char *to, const char *from, size_t length, int *error);
extern char *PQescapeLiteral(PGconn *conn, const char *str, size_t len);
extern char *PQescapeIdentifier(PGconn *conn, const char *str, size_t len);
extern unsigned char *PQescapeByteaConn(PGconn *conn, const unsigned char *from, size_t from_length, size_t *to_length);
extern unsigned char *PQunescapeBytea(const unsigned char *strtext, size_t *retbuflen);
extern size_t PQescapeString(char *to, const char *from, size_t length);
extern unsigned char *PQescapeBytea(const unsigned char *from, size_t from_length, size_t *to_length);
extern int    lo_open(PGconn *conn, Oid lobjId, int mode);
extern int    lo_close(PGconn *conn, int fd);
extern int    lo_read(PGconn *conn, int fd, char *buf, size_t len);
extern int    lo_write(PGconn *conn, int fd, const char *buf, size_t len);
extern int    lo_lseek(PGconn *conn, int fd, int offset, int whence);
extern pg_int64 lo_lseek64(PGconn *conn, int fd, pg_int64 offset, int whence);
extern Oid    lo_creat(PGconn *conn, int mode);
extern Oid    lo_create(PGconn *conn, Oid lobjId);
extern int    lo_tell(PGconn *conn, int fd);
extern pg_int64 lo_tell64(PGconn *conn, int fd);
extern int    lo_truncate(PGconn *conn, int fd, size_t len);
extern int    lo_truncate64(PGconn *conn, int fd, pg_int64 len);
extern int    lo_unlink(PGconn *conn, Oid lobjId);
extern Oid    lo_import(PGconn *conn, const char *filename);
extern Oid    lo_import_with_oid(PGconn *conn, const char *filename, Oid lobjId);
extern int    lo_export(PGconn *conn, Oid lobjId, const char *filename);
extern int    PQlibVersion(void);
extern int    PQmblen(const char *s, int encoding);
extern int    PQdsplen(const char *s, int encoding);
extern int    PQenv2encoding(void);
extern char     *PQencryptPassword(const char *passwd, const char *user);
extern int    pg_char_to_encoding(const char *name);
extern const char *pg_encoding_to_char(int encoding);
extern int    pg_valid_server_encoding_id(int encoding);
]])
end

local libpq = ffi.load('libpq', true)

local function error_string(conn)
   return ffi.string(libpq['PQerrorMessage'](conn))
end

--[[
SELECT t.oid as "oid", n.nspname as "Schema",
  pg_catalog.format_type(t.oid, NULL) AS "Name",
  pg_catalog.obj_description(t.oid, 'pg_type') as "Description", t.oid
FROM pg_catalog.pg_type t
   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
   AND pg_catalog.pg_type_is_visible(t.oid)
  ORDER BY 1, 2, 3;
]]
local INT2OID=21
local INT4OID=23
local INT8OID=20
local NUMERICOID=1700
local BOOLOID=16
local TEXTOID=25
local REALOID=700
local DOUBLEOID=701

local function fname(self, column)
   return ffi.string(PQfname(self, column))
end

local function ftype(self, column)
   return PQftype(self, column)
end

local PGtrue = string.byte('t')
local PGTrue = string.byte('T')

local PGResultMetatable = {
   __index = function(self, index)
      if index == 'ntuples' then
         return libpq.PQntuples(self)
      elseif index == 'nfields' then
         return libpq.PQnfields(self)
      elseif index == 'fname' then
         return fname
      elseif index == 'ftype' then
         return ftype
      end
   end,
   __call = function(self, row, column)
      assert(row ~= nil, column ~= nil)
      local rows = libpq.PQntuples(self);
      assert(row < rows, "No row in result")
      if type(column) == 'string' then
         column = libpq.PQfnumber(self, column)
         assert(column >= 0, "Field not found: ".. column)
      else
         local columns = libpq.PQnfields(self)
         assert(column < columns, "No column in result")
      end

      if libpq.PQgetisnull(self, row, column) ~= 0 then
         return box.NULL
      end

      local val = libpq.PQgetvalue(self, row, column)
      local len = libpq.PQgetlength(self, row, column)

      local ftype = libpq.PQftype(self, column)
      if ftype == INT2OID then
         return tonumber(ffi.string(val, len))
      elseif ftype == INT4OID then
         return tonumber(ffi.string(val, len))
      elseif ftype == NUMERICOID then
         -- TODO user builtin numeric
         return ffi.string(val, len)
      elseif ftype == INT8OID then
         return tonumber64(ffi.string(val, len))
      elseif ftype == BOOLOID then
         return val[0] == PGtrue or val[0] == PGTrue
      elseif ftype == TEXTOID then
         return ffi.string(val, len)
      elseif ftype == REALOID then
         return tonumber(ffi.string(val, len))
      elseif ftype == DOUBLEOID then
         return tonumber(ffi.string(val, len))
      else
         return ffi.string(val, len)
      end
      return box.NULL
   end
}

local pgresult
local rc, res = pcall(ffi.metatype, ffi.typeof('PGresult'), PGResultMetatable)
if rc then
   pgresult = res
end

local _M = { _VERSION = '0.01' }

local mt = { __index = _M }

function _M.version(self)
    return {lib = libpq.PQlibVersion(), server = libpq.PQserverVersion(self.conn)}
end


function _M.connect(conninfo, timeout)
   local start = clock.time()
   local rest = nil

   local conn = libpq.PQconnectStart(conninfo)
   if conn == nil then
      return nil, 'Cant allocate conn mem'
   end
   ffi.gc(conn, libpq.PQfinish)

   if libpq.PQstatus(conn) == libpq.CONNECTION_BAD then
      local err = error_string(conn)
      return nil, err
   end

   local err = nil
   local sock = libpq.PQsocket(conn)
   while true do
      if timeout then
         rest = timeout - (clock.time() - start)
         if rest < 0 then
            err = 'Connection timeout'
            break
         end
      end
      local rc = socket.iowait(sock, 'RW', rest)
      local status = libpq.PQconnectPoll(conn)
      if status == libpq.PGRES_POLLING_OK then
         -- TODO
         --   libpq.PQsetNoticeProcessor(conn, pgnotice, NULL)
         -- SUCCESS
         local rc = libpq.PQsetnonblocking(conn, 1)
         if rc ~= 0 then
            return nil, error_string(conn)
         end
         -- mutex
         local in_process = fiber.channel(1)
         in_process:put(true)

         local self = setmetatable({
               in_process = in_process,
               conn = conn,},
            mt)

         local weak_ref = setmetatable({
               ref=self}, {__mode='v'})
         local notify_worker = fiber.new(_M._check_for_notifies_loop, weak_ref)
         notify_worker:name('pg:notification')
         self._notify_worker = notify_worker

         return self
      elseif status == libpq.PGRES_POLLING_READING then
         if timeout then
            rest = timeout - (clock.time() - start)
            if rest < 0 then
               err = 'Connection read timeout'
               break
            end
         end
         socket.iowait(sock, 'R', rest)
      elseif status == libpq.PGRES_POLLING_WRITING then
         if timeout then
            rest = timeout - (clock.time() - start)
            if rest < 0 then
               err = 'Connection write timeout'
               break
            end
         end
         socket.iowait(sock, 'W', rest)
      else
         break
      end
   end

   if err == nil then
      err = error_string(conn)
   end

   return nil, err
end

function _M.transaction_status(self)
   return libpq.PQtransactionStatus(self.conn)
end

function _M._wait_for_result(self, timeout)
   local start = clock.time()
   local rest = nil
   local sock = libpq.PQsocket(self.conn)
   while (true) do
      local rc = libpq.PQconsumeInput(self.conn)
      if rc ~= 1 then
         return nil, error_string(self.conn)
      end
      if libpq.PQisBusy(self.conn) == true then
         if timeout ~= nil then
            rest = timeout - (clock.time() - start)
            if rest < 0 then
               self:shutdown()
               return nil, 'Read timeout'
            end
         end
         socket.iowait(sock, 'R', rest)
      else
         break
      end
   end
   return true
end

function _M._unsafe_exec(self, command, args, opts)
   opts = opts or {}
   local start = opts.start
   local timeout = opts.timeout
   local rest = 0

   local trans = libpq.PQtransactionStatus(self.conn)
   if trans == libpq.PQTRANS_ACTIVE then
      log.warn("PG: previous query is not ended")
   elseif trans == libpq.PQTRANS_INTRANS
      or trans == libpq.PQTRANS_INERROR then
      local thisid = fiber.self():id()
      if self.transaction_fiber_id ~= thisid then
         log.warn("PG: another fiber starts transaction, be careful")
      end
      if self.transaction_fiber_id == nil then
         log.warn("PG: unknown fiber starts transaction")
      end
   end

   if args and #args > 0 then
      local args_count = #args
      local types = box.NULL --ffi.new("oid[?]", args_count)
      local strargs = {}
      for i=1,args_count do
         table.insert(strargs, tostring(args[i]))
      end
      local values = ffi.new("const char*[?]", #strargs, strargs)

      local rc = libpq.PQsendQueryParams(self.conn,
                                         command,
                                         args_count,
                                         box.NULL,
                                         values,
                                         NULL,
                                         NULL,
                                         0)
      if rc == -1 then
         return nil, error_string(self.conn)
      end
   else

      local rc = libpq.PQsendQuery(self.conn, command)
      if rc == -1 then
         return nil, error_string(self.conn)
      end
   end

   --[[
      ASYNC WRITE
   ]]
   while true do
      local sock = libpq.PQsocket(self.conn)
      local rc = libpq.PQflush(self.conn)
      if rc == 1 then
         if timeout then
            rest = timeout - (clock.time() - start)
            if rest < 0 then
               self:shutdown()
               return nil, 'Write timeout'
            end
         end
         local state = socket.iowait(sock, 'RW', rest)
         if state == 'R' then
            local rc = libpq.PQconsumeInput(self.conn)
            if rc ~= 1 then
               return nil, error_string(self.conn)
            end
         elseif state == 'RW' then
            local rc = libpq.PQconsumeInput(self.conn)
            if rc ~= 1 then
               return nil, error_string(self.conn)
            end
            -- continue
         end
      elseif rc == 0 then
         break
      elseif rc == -1 then
         return nil, error_string(self.conn)
      end
   end

   --[[
      ASYNC READ
   ]]
   local results = {}
   while true do
      if timeout then
         rest = timeout - (clock.time() - start)
         if rest < 0 then
            self:shutdown()
            return nil, 'Read timeout'
         end
      end
      local rc, err = self:_wait_for_result(rest)
      if err ~= nil then
         return nil, err
      end

      local result = libpq.PQgetResult(self.conn)
      if result == nil then
         break
      end

      ffi.gc(result, libpq.PQclear)

      local status = libpq.PQresultStatus(result)
      if status == libpq.PGRES_TUPLES_OK then
         table.insert(results, result)
      elseif status == libpq.PGRES_COMMAND_OK then
         table.insert(results, 'command ok')
      elseif status == libpq.PGRES_EMPTY_QUERY then
         table.insert(results, 'empty query')
      elseif status == libpq.PGRES_NONFATAL_ERROR then
         table.insert(results, ffi.string(libpq.PQresultErrorMessage(result)))
      elseif status == libpq.PGRES_FATAL_ERROR then
         table.insert(results, ffi.string(libpq.PQresultErrorMessage(result)))
      else
         table.insert(results, 'Unknown sql driver result')
      end
   end
   return results
end

function _M.exec(self, command, args, opts)
   opts = opts or {}

   local timeout = opts.timeout
   local start = clock.time()

   local rc = self.in_process:get(timeout)
   if rc == nil then
      return nil, 'Connection mutex timeout'
   end

   opts = table.copy(opts)
   opts.start = start
   local rc, res, err = pcall(self._unsafe_exec, self, command, args, opts)

   local trans = libpq.PQtransactionStatus(self.conn)
   if trans == libpq.PQTRANS_ACTIVE then
      log.warn("PG: query still active after execution")
   elseif trans == libpq.PQTRANS_INTRANS
          or trans == libpq.PQTRANS_INERROR then
      if self.transaction_fiber_id == nil then
         local thisid = fiber.self():id()
         self.transaction_fiber_id = thisid
      end
   else
      self.transaction_fiber_id = nil
   end
   self.in_process:put(true)
   if rc == false then
      return nil, res
   end
   return res, err
end

function _M.status(self)
   return libpq.PQstatus(self.conn)
end

function _M._check_for_notifies(self)
   local notify = libpq.PQnotifies(self.conn)
   while (notify ~= nil) do
      ffi.gc(notify, libpq.PQfreemem)
      if self.subscriber and type(self.subscriber) == 'function' then
         self.subscriber(self,
                         ffi.string(notify.relname),
                         ffi.string(notify.extra),
                         notify.be_pid)
      end
      notify = libpq.PQnotifies(self.conn)
   end
end

function _M._check_for_notifies_loop(weak_ref)
   while (true) do
      local self = weak_ref.ref
      if self == nil then
         return
      end
      local timer = self.notification_timer or 0.1
      if self.in_process:count() == 1 then
         self.in_process:get()
         local sock = libpq.PQsocket(self.conn)
         local rc, res = pcall(socket.iowait, sock, 'R', 0)
         self.in_process:put(true)

         if not rc then
            -- Seems that connection no more valid
            return
         end
         if res == 'R' then
            libpq.PQconsumeInput(self.conn)
         end
      end

      local rc, res = pcall(_M._check_for_notifies, self)
      if not rc then
         log.info('PG: error while notification loop %q', res)
      end
      self = nil

      fiber.sleep(timer)
   end
end

function _M.shutdown(self, timeout)
   --[==[
      There is way to local disconnect `PQcleanup`, but using `exec` from
      other fiber crashes because `use-after-free`
      Other way to `close(PQsocket())`, but we cant modify PGconn socket
      so socket collision is possible.
      One more another way to `conn:exec([[ select pg_terminate_backed(%pid) ]])`
      but this way fatal error message garbage server logs
      THE WAY HERE is to terminate connection using `close(PQsocket())`
   ]==]
   local sock = libpq.PQsocket(self.conn)
   ffi.C.shutdown(sock, 2)
end

local M = {
   connect = _M.connect,
   libpq = libpq,
}

return M
