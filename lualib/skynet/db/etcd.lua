local skynet = require "skynet"
local queue = require "skynet.queue"
local httpc = require "http.httpc"
local crypt = require "skynet.crypt"
local cjson = require "cjson"

local etcd = {}
local etcd_mt = { __index = etcd }

local TOKEN_TTL_MARGIN = 27000

local base64_encode = crypt.base64encode
local base64_decode = crypt.base64decode

-- Localize frequently used functions for performance
local pairs = pairs
local ipairs = ipairs
local tostring = tostring
local tonumber = tonumber
local table = table
local string = string

local function get_range_end(full_prefix)
    local len = #full_prefix
    for i = len, 1, -1 do
        local b = full_prefix:byte(i)
        if b < 255 then
            return full_prefix:sub(1, i - 1) .. string.char(b + 1) .. string.rep("\0", len - i)
        end
    end
    return string.rep("\0", len)
end

local function make_full_key(prefix, key)
    -- Combine prefix with key, ensuring no double slashes
    if prefix == "" then
        return key
    end
    if prefix:sub(-1) == "/" or key:sub(1, 1) == "/" then
        return prefix .. key
    else
        return prefix .. "/" .. key
    end
end

local function http_request(method, host, path, body, headers, timeout)
    -- NOTE: httpc.timeout is a module-level global, not per-request.
    -- The save/restore pattern below is safe under skynet's cooperative
    -- coroutine model as long as the same etcd instance does not issue
    -- nested requests with different timeouts within a single yield point.
    headers = headers or {}
    headers["Content-Type"] = "application/json"
    local old_timeout = httpc.timeout
    if timeout then
        httpc.timeout = timeout
    end
    local ok, status, resp_body, resp_headers = pcall(httpc.request, method, host, path, nil, headers, body and cjson.encode(body))
    httpc.timeout = old_timeout
    if ok then
        if status >= 200 and status < 300 then
            return cjson.decode(resp_body)
        else
            return nil, status, resp_body
        end
    else
        return nil, -1, status
    end
end

function etcd.new(opts)
    local self = setmetatable({}, etcd_mt)
    if opts.hosts then
        self.hosts = {}
        for _, h in ipairs(opts.hosts) do
            table.insert(self.hosts, h)
        end
    elseif opts.host then
        self.hosts = { opts.host }
    else
        self.hosts = { "http://127.0.0.1:2379" }
    end
    self._current_host_idx = 1
    self.user = opts.user
    self.password = opts.password
    self.timeout = opts.timeout or 5000
    self.prefix = opts.prefix or ""
    self.prefix_len = #self.prefix
    self.retry_count = opts.retry_count or #self.hosts
    self.retry_delay = opts.retry_delay or 100
    self._watches = {}
    self._reg_ttl = opts.reg_ttl or 10
    self._registrations = {}
    self._shared_leases = {}  -- shared lease references: key = lease_id, value = lease_ref
    self._leases_by_ttl = {}  -- index: ttl -> list of lease_ids with this ttl
    self._watch_id = 0
    self._last_revision = opts.start_revision or 0
    self._token = nil
    self._token_expire = 0
    self._watch_etcd_ids = {}
    self._auth_queue = queue()
    self._lease_queue = queue()

    return self
end

function etcd:authenticate()
    if not self.user or not self.password then
        return true
    end

    local body = {
        name = self.user,
        password = self.password,
    }

    local resp, err = self:_request("POST", "/v3/auth/authenticate", body, {}, true)
    if resp and resp.token then
        self._token = resp.token
        self._token_expire = skynet.now() + TOKEN_TTL_MARGIN
        return true
    end
    return nil, err or "authenticate failed"
end

function etcd:_need_auth()
    return self.user and self.password
end

function etcd:_get_token()
    if not self:_need_auth() then
        return nil
    end
    if self._token and skynet.now() <= self._token_expire then
        return self._token
    end
    return self._auth_queue(function()
        if self._token and skynet.now() <= self._token_expire then
            return self._token
        end
        local ok, err = self:authenticate()
        if not ok then
            return nil, err
        end
        return self._token
    end)
end

function etcd:_next_host()
    self._current_host_idx = self._current_host_idx % #self.hosts + 1
end

function etcd:_update_revision(resp)
    if resp and resp.header and resp.header.revision then
        local rev = tonumber(resp.header.revision)
        if rev and rev > self._last_revision then
            self._last_revision = rev
        end
    end
end

function etcd:_request(method, path, body, headers, skip_auth)
    local last_err
    local auth_retried = false
    local i = 1

    while i <= self.retry_count do
        local host = self.hosts[self._current_host_idx]
        local req_headers = {}
        for k, v in pairs(headers or {}) do req_headers[k] = v end

        local skip_this = false
        if not skip_auth and self:_need_auth() then
            local token, err = self:_get_token()
            if not token then
                last_err = err or "auth failed"
                skip_this = true
            else
                req_headers["Authorization"] = token
            end
        end

        if not skip_this then
            local resp, status, err = http_request(method, host, path, body, req_headers, self.timeout)
            if resp then
                self:_update_revision(resp)
                return resp
            end
            last_err = err or ("HTTP " .. tostring(status))

            if status >= 400 and status < 500 and status ~= 401 then
                return nil, status, err
            end

            if not (status == 401 and not auth_retried and not skip_auth) then
                self:_next_host()
                if i < self.retry_count then
                    skynet.sleep(self.retry_delay // 10)
                end
                i = i + 1
            else
                self._token = nil
                auth_retried = true
            end
        else
            self:_next_host()
            if i < self.retry_count then
                skynet.sleep(self.retry_delay // 10)
            end
            i = i + 1
        end
    end
    return nil, last_err
end

function etcd:get_last_revision()
    return self._last_revision
end

function etcd:put(key, value, opts)
    opts = opts or {}
    local body = {
        key = base64_encode(make_full_key(self.prefix, key)),
        value = base64_encode(value),
    }
    if opts.lease then
        body.lease = tostring(opts.lease)
    end
    if opts.prev_kv then
        body.prev_kv = true
    end
    if opts.ignore_lease then
        body.ignore_lease = true
    end
    if opts.ignore_value then
        body.ignore_value = true
    end
    return self:_request("POST", "/v3/kv/put", body)
end

function etcd:get(key, opts)
    opts = opts or {}
    local body = {
        key = base64_encode(make_full_key(self.prefix, key)),
    }
    if opts.range_end then
        local full_range_key = make_full_key(self.prefix, opts.range_end)
        local full_range_end = get_range_end(full_range_key)
        body.range_end = base64_encode(full_range_end)
    end
    if opts.limit then
        body.limit = opts.limit
    end
    if opts.revision then
        body.revision = tostring(opts.revision)
    end
    if opts.sort_order then
        body.sort_order = opts.sort_order
    end
    if opts.sort_target then
        body.sort_target = opts.sort_target
    end
    if opts.serializable then
        body.serializable = true
    end
    if opts.keys_only then
        body.keys_only = true
    end
    if opts.count_only then
        body.count_only = true
    end
    if opts.min_mod_revision then
        body.min_mod_revision = tostring(opts.min_mod_revision)
    end
    if opts.max_mod_revision then
        body.max_mod_revision = tostring(opts.max_mod_revision)
    end

    local resp, err = self:_request("POST", "/v3/kv/range", body)
    if resp and resp.kvs then
        for _, kv in ipairs(resp.kvs) do
            kv.key = base64_decode(kv.key):sub(self.prefix_len + 1)
            if kv.value then
                kv.value = base64_decode(kv.value)
            end
        end
    end
    return resp, err
end

function etcd:get_prefix(prefix, opts)
    opts = opts or {}
    opts.range_end = prefix
    return self:get(prefix, opts)
end

function etcd:delete(key, opts)
    opts = opts or {}
    local body = {
        key = base64_encode(make_full_key(self.prefix, key)),
    }
    if opts.range_end then
        local full_range_key = make_full_key(self.prefix, opts.range_end)
        local full_range_end = get_range_end(full_range_key)
        body.range_end = base64_encode(full_range_end)
    end
    if opts.prev_kv then
        body.prev_kv = true
    end
    return self:_request("POST", "/v3/kv/deleterange", body)
end

function etcd:delete_prefix(prefix)
    return self:delete(prefix, { range_end = prefix })
end

function etcd:txn(compare, success, failure)
    local body = {
        compare = compare or cjson.empty_array,
        success = success or cjson.empty_array,
        failure = failure or cjson.empty_array,
    }
    return self:_request("POST", "/v3/kv/txn", body)
end

function etcd:lease_grant(ttl, id)
    local body = { TTL = ttl }
    if id then
        body.ID = tostring(id)
    end
    return self:_request("POST", "/v3/lease/grant", body)
end

function etcd:lease_revoke(id)
    return self:_request("POST", "/v3/lease/revoke", { ID = tostring(id) })
end

function etcd:lease_ttl(id, keys)
    return self:_request("POST", "/v3/lease/timetolive", { ID = tostring(id), keys = keys and true or false })
end

function etcd:lease_keepalive_once(id)
    local result, err = self:_request("POST", "/v3/lease/keepalive", { ID = tostring(id) })
    if result and result.result then
        return result.result
    end
    return nil, err or "keepalive failed"
end

function etcd:_get_or_create_shared_lease(ttl)
    return self._lease_queue(function()
        local lease_ids = self._leases_by_ttl[ttl]
        if lease_ids then
            for _, lease_id in ipairs(lease_ids) do
                local lease_ref = self._shared_leases[lease_id]
                if lease_ref and lease_ref.ref_count > 0 then
                    lease_ref.ref_count = lease_ref.ref_count + 1
                    return lease_ref
                end
            end
        end

        local lease_resp, err = self:lease_grant(ttl)
        if not lease_resp then
            return nil, err
        end

        local lease_id = tonumber(lease_resp.ID)
        local lease_ref = {
            lease_id = lease_id,
            ttl = ttl,
            ref_count = 1,
            keepalive_running = true,
            errors = 0,
            backoff = 1000,
            registrations = {},
        }

        self._shared_leases[lease_id] = lease_ref
        if not self._leases_by_ttl[ttl] then
            self._leases_by_ttl[ttl] = {}
        end
        table.insert(self._leases_by_ttl[ttl], lease_id)
        self:_start_shared_lease_keepalive(lease_ref)

        return lease_ref
    end)
end

function etcd:_release_shared_lease(lease_ref, key)
    -- Remove registration from lease
    lease_ref.registrations[key] = nil

    -- Decrement reference count
    lease_ref.ref_count = lease_ref.ref_count - 1

    -- If no more references, revoke lease and cleanup
    if lease_ref.ref_count == 0 then
        lease_ref.keepalive_running = false
        local ok, err = pcall(self.lease_revoke, self, lease_ref.lease_id)
        if not ok then
            skynet.error(string.format("etcd release_shared_lease revoke %d failed: %s", lease_ref.lease_id, err))
        end
        self._shared_leases[lease_ref.lease_id] = nil
        local lease_ids = self._leases_by_ttl[lease_ref.ttl]
        if lease_ids then
            for i, lid in ipairs(lease_ids) do
                if lid == lease_ref.lease_id then
                    table.remove(lease_ids, i)
                    break
                end
            end
            if #lease_ids == 0 then
                self._leases_by_ttl[lease_ref.ttl] = nil
            end
        end
        return true
    end

    return false  -- lease still has references
end

function etcd:_rebuild_shared_lease(lease_ref)
    local old_lease_id = lease_ref.lease_id
    -- Grant new lease
    local ok, result = pcall(self.lease_grant, self, lease_ref.ttl)
    if not ok or not result then
        skynet.error(string.format("etcd rebuild_shared_lease lease_grant failed: err=%s", result))
        return nil
    end

    local new_lease_id = tonumber(result.ID)
    if not new_lease_id then
        skynet.error(string.format("etcd rebuild_shared_lease invalid lease id: %s", result.ID))
        pcall(self.lease_revoke, self, new_lease_id)
        return nil
    end
    local total_count = 0
    local failed = false

    -- Process in batches to avoid exceeding etcd request size limit
    local BATCH_SIZE = 100
    local compares = {}
    local successes = {}
    local batch_count = 0

    for key, reg in pairs(lease_ref.registrations) do
        total_count = total_count + 1
        table.insert(compares, {
            key = reg.b64_key,
            result = "EQUAL",
            target = "LEASE",
            lease = tostring(old_lease_id)
        })
        table.insert(successes, {
            request_put = {
                key = reg.b64_key,
                value = reg.b64_value,
                lease = tostring(new_lease_id)
            }
        })
        batch_count = batch_count + 1
        if batch_count >= BATCH_SIZE then
            -- Execute batch
            local ok2, txn_resp = pcall(self.txn, self, compares, successes, cjson.empty_array)
            if not ok2 or not txn_resp or txn_resp.succeeded ~= true then
                failed = true
                break
            end
            compares = {}
            successes = {}
            batch_count = 0
        end
    end

    -- Process remaining
    if not failed and batch_count > 0 then
        local ok2, txn_resp = pcall(self.txn, self, compares, successes, cjson.empty_array)
        if not ok2 or not txn_resp or txn_resp.succeeded ~= true then
            failed = true
        end
    end

    if failed then
        pcall(self.lease_revoke, self, new_lease_id)
        skynet.error("etcd rebuild_shared_lease txn failed on batch")
        return nil
    end

    if total_count == 0 then
        pcall(self.lease_revoke, self, new_lease_id)
        return nil
    end

    -- Update the lease reference with new lease ID and fix indices
    self._shared_leases[old_lease_id] = nil
    lease_ref.lease_id = new_lease_id
    self._shared_leases[new_lease_id] = lease_ref
    local lease_ids = self._leases_by_ttl[lease_ref.ttl]
    if lease_ids then
        for i, lid in ipairs(lease_ids) do
            if lid == old_lease_id then
                lease_ids[i] = new_lease_id
                break
            end
        end
    end
    skynet.error(string.format("etcd shared lease rebuilt: %d registrations, old_lease=%d new_lease=%d",
        total_count, old_lease_id, new_lease_id))

    return new_lease_id
end

function etcd:_start_shared_lease_keepalive(lease_ref)
    skynet.fork(function()
        local interval = (lease_ref.ttl * 1000) // 3
        local current_lease_id = lease_ref.lease_id

        while lease_ref.keepalive_running and lease_ref.ref_count > 0 do
            local ok, res = pcall(self.lease_keepalive_once, self, current_lease_id)
            local ttl = ok and res and tonumber(res.TTL) or 0
            if ttl > 0 then
                lease_ref.errors = 0
                lease_ref.backoff = 1000
                skynet.sleep(interval // 10)
            else
                lease_ref.errors = lease_ref.errors + 1
                if lease_ref.errors >= 3 then
                    local new_id = self:_rebuild_shared_lease(lease_ref)
                    if new_id then
                        current_lease_id = new_id
                        lease_ref.errors = 0
                        lease_ref.backoff = 1000
                    else
                        -- Add jitter to avoid thundering herd
                        local backoff = lease_ref.backoff * 2
                        backoff = backoff * (0.75 + math.random() * 0.5)
                        lease_ref.backoff = math.min(backoff, 30000)
                    end
                end
                skynet.sleep(lease_ref.backoff // 10)
            end
        end
    end)
end

function etcd:get_registry()
    local result = {}
    for key, reg in pairs(self._registrations) do
        table.insert(result, {
            service_name = reg.service_name,
            addr = reg.addr,
            lease_id = reg.lease_ref.lease_id,
            ttl = self._reg_ttl,
        })
    end
    return result
end

local function process_watch_line(line, prefix, callback, current_rev_ref, last_revision_ref, watch_id_ref)
    local prefix_len = #prefix
    local ok, resp = pcall(cjson.decode, line)
    if not ok or not resp or not resp.result then
        return current_rev_ref[1]
    end

    if resp.result.watch_id then
        watch_id_ref[1] = tonumber(resp.result.watch_id)
    end

    if resp.result.events then
        local events = resp.result.events
        for _, ev in ipairs(events) do
            if ev.kv then
                ev.kv.key = base64_decode(ev.kv.key):sub(prefix_len + 1)
                if ev.kv.value then
                    ev.kv.value = base64_decode(ev.kv.value)
                end
                local mod_rev = tonumber(ev.kv.mod_revision)
                if mod_rev and mod_rev > current_rev_ref[1] then
                    current_rev_ref[1] = mod_rev
                end
                if mod_rev and mod_rev > last_revision_ref[1] then
                    last_revision_ref[1] = mod_rev
                end
            end
            if ev.prev_kv then
                ev.prev_kv.key = base64_decode(ev.prev_kv.key):sub(prefix_len + 1)
                if ev.prev_kv.value then
                    ev.prev_kv.value = base64_decode(ev.prev_kv.value)
                end
            end
        end
        callback(events, resp.result)
    end
    return current_rev_ref[1]
end

function etcd:_get_watch_headers()
    local headers = { ["Content-Type"] = "application/json" }
    if self:_need_auth() then
        local token, err = self:_get_token()
        if not token then
            return nil, err
        end
        headers["Authorization"] = token
    end
    return headers
end

function etcd:_process_watch_stream(stream, watch_id, callback, current_rev)
    local current_rev_ref = { current_rev }
    local last_revision_ref = { self._last_revision }
    local etcd_watch_id_ref = { nil }
    local buf = ""

    while self._watches[watch_id] and stream.connected do
        local chunk = stream:padding()
        if not chunk or chunk == "" then break end
        buf = buf .. chunk

        while true do
            local pos = buf:find("\n")
            if not pos then break end
            local line = buf:sub(1, pos - 1)
            buf = buf:sub(pos + 1)
            if line ~= "" then
                current_rev_ref[1] = process_watch_line(line, self.prefix, callback, current_rev_ref, last_revision_ref, etcd_watch_id_ref)
                if etcd_watch_id_ref[1] then
                    self._watch_etcd_ids[watch_id] = etcd_watch_id_ref[1]
                end
            end
        end
    end
    self._last_revision = last_revision_ref[1]
    self._watch_etcd_ids[watch_id] = nil
    return current_rev_ref[1]
end

function etcd:_run_watch_loop(watch_id, opts, base_create_request, callback)
    local max_retry = opts.max_retry or 1000000
    local reconnect_delay = opts.reconnect_delay or 1000
    local current_rev = opts.start_revision or self._last_revision
    local retry = 0

    while retry < max_retry and self._watches[watch_id] do
        local body = { create_request = {} }
        for k, v in pairs(base_create_request) do
            body.create_request[k] = v
        end
        if current_rev > 0 then
            body.create_request.start_revision = tostring(current_rev + 1)
        end

        local headers, err = self:_get_watch_headers()
        if headers then
            local old_timeout = httpc.timeout
            httpc.timeout = nil
            local ok, stream = pcall(httpc.request_stream, "POST", self.hosts[self._current_host_idx], "/v3/watch", nil, headers, cjson.encode(body))
            httpc.timeout = old_timeout
            if ok and stream then
                current_rev = self:_process_watch_stream(stream, watch_id, callback, current_rev)
                local ok_close, err_close = pcall(stream.close, stream)
                if not ok_close then
                    skynet.error("etcd watch close stream error: ", err_close)
                end
            else
                skynet.error("etcd watch connect error (retry ", retry, "):", stream)
            end
        else
            skynet.error("etcd watch auth error (retry ", retry, "):", err)
        end

        if not self._watches[watch_id] then
            break
        end
        self:_next_host()
        retry = retry + 1
        skynet.sleep(reconnect_delay // 10)
    end
    -- Always clean up watch id when loop exits
    self._watch_etcd_ids[watch_id] = nil
end

function etcd:watch(key, callback, opts)
    opts = opts or {}
    local watch_id = self._watch_id + 1
    self._watch_id = watch_id

    local base_create_request = {
        key = base64_encode(make_full_key(self.prefix, key)),
    }
    if opts.range_end then
        local full_range_end = get_range_end(make_full_key(self.prefix, opts.range_end))
        base_create_request.range_end = base64_encode(full_range_end)
    end
    if opts.prev_kv then
        base_create_request.prev_kv = true
    end
    if opts.progress_notify then
        base_create_request.progress_notify = true
    end
    if opts.filters then
        base_create_request.filters = opts.filters
    end

    skynet.fork(function()
        self:_run_watch_loop(watch_id, opts, base_create_request, callback)
        self._watches[watch_id] = nil
    end)

    self._watches[watch_id] = true
    return watch_id
end

function etcd:watch_prefix(prefix, callback, opts)
    opts = opts or {}
    opts.range_end = prefix
    return self:watch(prefix, callback, opts)
end

function etcd:watch_cancel(watch_id)
    local etcd_watch_id = self._watch_etcd_ids[watch_id]
    self._watches[watch_id] = nil
    self._watch_etcd_ids[watch_id] = nil
    if etcd_watch_id then
        local body = { cancel_request = { watch_id = etcd_watch_id } }
        return self:_request("POST", "/v3/watch", body)
    end
    return true
end

function etcd:register(service_name, addr, metadata)
    local key = make_full_key(self.prefix, string.format("%s/%s", service_name, addr))
    if self._registrations[key] then
        return nil, "already registered, deregister first"
    end

    -- Get or create shared lease with configured TTL
    local lease_ref, err = self:_get_or_create_shared_lease(self._reg_ttl)
    if not lease_ref then
        return nil, err or "create shared lease failed"
    end

    local lease_id = lease_ref.lease_id
    local value = cjson.encode({
        addr = addr,
        metadata = metadata or {},
        timestamp = skynet.time(),
    })
    -- key already includes prefix
    local b64_key = base64_encode(key)
    local b64_value = base64_encode(value)

    local txn_resp, err = self:txn(
        { { key = b64_key, result = "EQUAL", target = "VERSION", version = 0 } },
        { { request_put = { key = b64_key, value = b64_value, lease = tostring(lease_id) } } },
        cjson.empty_array
    )
    if not txn_resp then
        self:_release_shared_lease(lease_ref, key)
        return nil, err or "txn failed"
    end

    if txn_resp.succeeded ~= true then
        self:_release_shared_lease(lease_ref, key)
        return nil, "key already exists"
    end

    -- Store registration with reference to shared lease FIRST
    self._registrations[key] = {
        service_name = service_name,
        addr = addr,
        metadata = metadata,
        key = key,
        value = value,
        b64_key = b64_key,
        b64_value = b64_value,
        lease_ref = lease_ref,
    }

    -- THEN add registration to shared lease (now self._registrations[key] is valid)
    lease_ref.registrations[key] = self._registrations[key]

    return lease_id
end

function etcd:deregister(service_name, addr)
    local raw_key = string.format("%s/%s", service_name, addr)
    local full_key = make_full_key(self.prefix, raw_key)
    local reg = self._registrations[full_key]
    if reg and reg.lease_ref then
        self:_release_shared_lease(reg.lease_ref, full_key)
    end
    self._registrations[full_key] = nil
    return self:delete(raw_key)
end

function etcd:discover(service_name)
    local resp = self:get_prefix(service_name)
    local instances = {}
    if resp and resp.kvs then
        for _, kv in ipairs(resp.kvs) do
            local ok, data = pcall(cjson.decode, kv.value)
            if ok then
                instances[data.addr] = data.metadata
            else
                skynet.error(string.format("etcd discover decode failed: key=%s err=%s", kv.key, data))
            end
        end
    end
    return instances
end

function etcd:watch_service(service_name, callback, opts)
    opts = opts or {}
    if opts.full_sync then
        local resp = self:get_prefix(service_name)
        if resp and resp.kvs then
            for _, kv in ipairs(resp.kvs) do
                local addr = kv.key:match("/([^/]+)$")
                if addr then
                    local ok, data = pcall(cjson.decode, kv.value)
                    if ok then
                        callback("add", addr, data and data.metadata)
                    end
                end
            end
        end
    end

    return self:watch_prefix(service_name, function(events)
        for _, ev in ipairs(events) do
            local event_type = ev.type or "PUT"
            local addr = ev.kv.key:match("/([^/]+)$")
            if addr then
                if event_type == "PUT" then
                    local ok, data = pcall(cjson.decode, ev.kv.value)
                    if ok then
                        callback("add", addr, data and data.metadata)
                    end
                elseif event_type == "DELETE" then
                    callback("remove", addr)
                end
            end
        end
    end, opts)
end

function etcd:get_all_services()
    -- When prefix contains the service root path, this gets all services
    -- prefix format: [prefix]/service_name/addr
    -- So get_prefix with empty prefix will get everything under self.prefix
    local resp = self:get_prefix("", { keys_only = true })
    local services = {}
    if resp and resp.kvs then
        for _, kv in ipairs(resp.kvs) do
            local service_name = kv.key:match("^([^/]+)/")
            if service_name then
                services[service_name] = true
            end
        end
    end
    local list = {}
    for name in pairs(services) do
        table.insert(list, name)
    end
    return list
end

function etcd:get_hosts()
    return self.hosts
end

function etcd:get_current_host()
    return self.hosts[self._current_host_idx]
end

function etcd:sync_revision()
    -- Use count_only to get current revision without returning all keys
    self:get("", { count_only = true, limit = 0 })
    return self._last_revision
end

function etcd:set_hosts(hosts)
    self.hosts = hosts
    self._current_host_idx = 1
    self.retry_count = #hosts
end

function etcd:close()
    for watch_id in pairs(self._watches) do
        self._watches[watch_id] = nil
    end
    skynet.sleep(1)

    -- Revoke all shared leases
    for lease_id, lease_ref in pairs(self._shared_leases) do
        lease_ref.keepalive_running = false
        local ok, err = pcall(self.lease_revoke, self, lease_id)
        if not ok then
            skynet.error(string.format("etcd close revoke lease %d failed: %s", lease_id, err))
        end
    end
    self._shared_leases = {}
    self._leases_by_ttl = {}
    self._registrations = {}
end

return etcd
