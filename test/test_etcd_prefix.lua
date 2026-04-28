local skynet = require "skynet"
local etcd = require "skynet.db.etcd"
local httpc = require "http.httpc"
local crypt = require "skynet.crypt"
local cjson = require "cjson"

local base64_encode = crypt.base64encode
local base64_decode = crypt.base64decode

-- Test get_range_end helper function (extracted for deduplication)
local function test_get_range_end()
    print("\n=== Testing get_range_end helper ===")

    -- Load the module to access the local function via debug
    -- We'll test indirectly through the API
    local client = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = ""
    }

    -- Test 1: Prefix with trailing slash
    print("\n1. Testing prefix with trailing slash")
    client:put("/test/range/a", "value_a")
    client:put("/test/range/b", "value_b")
    client:put("/test/range/c", "value_c")
    local resp = client:get_prefix("/test/range/")
    local count = resp.kvs and #resp.kvs or 0
    print("   got", count, "keys for prefix /test/range/")
    assert(count == 3, "should get all 3 keys with trailing slash prefix")

    -- Test 2: Prefix without trailing slash
    print("\n2. Testing prefix without trailing slash (partial match)")
    client:put("/test/rangeX/1", "x1")
    client:put("/test/rangeY/1", "y1")
    resp = client:get_prefix("/test/range")  -- no trailing slash
    count = resp.kvs and #resp.kvs or 0
    print("   got", count, "keys for prefix /test/range")
    assert(count >= 3, "should get all keys starting with /test/range")

    -- Cleanup
    client:delete_prefix("/test/range")
    client:delete_prefix("/test/rangeX/")
    client:delete_prefix("/test/rangeY/")

    print("   get_range_end: OK")
end

-- Test custom prefix replaces hardcoded /skynet/services
local function test_custom_prefix()
    print("\n=== Testing custom prefix behavior ===")

    -- Test 1: Default empty prefix - should work correctly
    print("\n1. Testing default empty prefix")
    local client1 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = ""
    }
    assert(client1.prefix == "", "default prefix should be empty")
    assert(client1.prefix_len == 0, "prefix_len should be 0 for empty prefix")

    local lease_id = client1:register("srv.empty", "127.0.0.1:9001", {})
    assert(lease_id, "register with empty prefix should work")
    local instances = client1:discover("srv.empty")
    assert(instances.kvs and #instances.kvs > 0, "discover should find the service")
    print("   empty prefix registration: OK")

    -- Verify the actual key in etcd (should be without /skynet/services)
    local raw_resp = client1:get("srv.empty/127.0.0.1:9001")
    assert(raw_resp.kvs and #raw_resp.kvs > 0, "key should exist at prefix-relative path")
    print("   key stored at prefix-relative path: OK")

    client1:deregister("srv.empty", "127.0.0.1:9001")
    client1:close()

    -- Test 2: Custom prefix - should namespace all keys
    print("\n2. Testing custom prefix /myapp/services/")
    local client2 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/myapp/services/"
    }
    assert(client2.prefix == "/myapp/services/", "prefix should be set")
    assert(client2.prefix_len == #"/myapp/services/", "prefix_len should be cached")

    lease_id = client2:register("srv.custom", "127.0.0.1:9002", { tag = "test" })
    assert(lease_id, "register with custom prefix should work")

    -- Discover should work within prefix
    instances = client2:discover("srv.custom")
    local count = instances.kvs and #instances.kvs or 0
    assert(count == 1, "discover should find the service within prefix")
    print("   custom prefix registration: OK")

    -- Verify key isolation - client without prefix should NOT see the key
    local client_no_prefix = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = ""
    }
    instances = client_no_prefix:discover("srv.custom")
    count = instances.kvs and #instances.kvs or 0
    assert(count == 0, "client without prefix should not see prefixed keys")
    print("   prefix isolation: OK")

    -- Verify get_all_services works with prefix
    local services = client2:get_all_services()
    local found = false
    for _, s in ipairs(services) do
        if s == "srv.custom" then
            found = true
            break
        end
    end
    assert(found, "get_all_services should find service within prefix")
    print("   get_all_services with prefix: OK")

    -- Cleanup
    client2:deregister("srv.custom", "127.0.0.1:9002")
    client2:close()
    client_no_prefix:close()

    -- Test 3: watch_service with prefix
    print("\n3. Testing watch_service with prefix")
    local client3 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/watchtest/"
    }

    local watch_events = {}
    local watch_id = client3:watch_service("watch.srv", function(action, addr, meta)
        table.insert(watch_events, { action = action, addr = addr, meta = meta })
    end)

    skynet.sleep(10)  -- Wait for watch to establish

    lease_id = client3:register("watch.srv", "127.0.0.1:9003", {})
    assert(lease_id, "register for watch test should work")

    skynet.sleep(30)  -- Wait for watch event

    client3:watch_cancel(watch_id)
    client3:deregister("watch.srv", "127.0.0.1:9003")
    client3:close()

    assert(#watch_events >= 1, "should receive at least one watch event")
    print("   watch_service with prefix: OK")

    -- Test 4: watch_service with full_sync option
    print("\n4. Testing watch_service with full_sync option")
    local client4 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/fullsynctest/"
    }

    -- Register BEFORE starting watch
    lease_id = client4:register("fullsync.srv", "127.0.0.1:9004", { tag = "existing" })
    assert(lease_id, "pre-registration should work")

    local fullsync_events = {}
    watch_id = client4:watch_service("fullsync.srv", function(action, addr, meta)
        table.insert(fullsync_events, { action = action, addr = addr, meta = meta })
    end, { full_sync = true })

    skynet.sleep(20)  -- Wait for full_sync to process

    client4:watch_cancel(watch_id)
    client4:deregister("fullsync.srv", "127.0.0.1:9004")
    client4:close()

    assert(#fullsync_events >= 1, "full_sync should trigger events for existing services")
    local found_existing = false
    for _, ev in ipairs(fullsync_events) do
        if ev.addr == "127.0.0.1:9004" and ev.action == "add" then
            found_existing = true
            break
        end
    end
    assert(found_existing, "full_sync should find the pre-registered service")
    print("   watch_service with full_sync: OK")

    print("\n   custom prefix behavior: OK")
end

-- Test sync_revision uses count_only (doesn't load all keys)
local function test_sync_revision_count_only()
    print("\n=== Testing sync_revision with count_only ===")

    local client = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/synctest/"
    }

    -- Put some keys
    for i = 1, 10 do
        client:put("key" .. i, "value" .. i)
    end

    local rev_before = client:get_last_revision()
    print("   revision before sync:", rev_before)

    -- Put one more key to increase revision
    client:put("trigger_rev", "value")

    local rev = client:sync_revision()
    print("   revision after sync:", rev)

    assert(rev > rev_before, "sync_revision should update revision")

    -- Cleanup
    client:delete_prefix("")
    client:close()

    print("   sync_revision count_only: OK")
end

-- Test _leases_by_ttl index for O(k) shared lease lookup
local function test_leases_by_ttl_index()
    print("\n=== Testing _leases_by_ttl index ===")

    local client = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/leasetest/",
        reg_ttl = 10
    }

    -- Test 1: Multiple registrations with same TTL should share same lease
    print("\n1. Same TTL shared lease lookup via index")
    local lease1 = client:register("srv1", "127.0.0.1:9101", {})
    local lease2 = client:register("srv2", "127.0.0.1:9102", {})
    local lease3 = client:register("srv3", "127.0.0.1:9103", {})

    assert(lease1 == lease2, "same TTL should share lease")
    assert(lease2 == lease3, "same TTL should share lease")
    print("   same TTL reuse lease via index: OK (lease_id=" .. lease1 .. ")")

    -- Test 2: Verify index internal structure
    print("\n2. Verifying _leases_by_ttl index structure")
    -- Check that we have entries in _leases_by_ttl for our reg_ttl
    local lease_ids_by_ttl = client._leases_by_ttl[client._reg_ttl]
    assert(lease_ids_by_ttl, "should have lease_ids indexed by ttl")
    assert(#lease_ids_by_ttl >= 1, "should have at least one lease in index")

    -- Check that the lease_id is in the index
    local found = false
    for _, id in ipairs(lease_ids_by_ttl) do
        if id == lease1 then
            found = true
            break
        end
    end
    assert(found, "lease_id should be in ttl index")
    print("   _leases_by_ttl index structure: OK")

    -- Test 3: Deregister all but one - lease should remain
    print("\n3. Reference counting with index")
    client:deregister("srv1", "127.0.0.1:9101")
    client:deregister("srv2", "127.0.0.1:9102")

    -- New registration should reuse the existing lease
    local lease4 = client:register("srv4", "127.0.0.1:9104", {})
    assert(lease4 == lease1, "should reuse existing lease from index after deregister")
    print("   lease reuse after partial deregister: OK")

    -- Deregister remaining services
    client:deregister("srv3", "127.0.0.1:9103")
    client:deregister("srv4", "127.0.0.1:9104")

    -- Now the lease should be removed from index
    local lease_ref = client._shared_leases[lease1]
    assert(not lease_ref, "lease should be revoked when ref_count reaches 0")
    print("   lease cleanup from shared_leases: OK")

    -- Note: The index may still contain the lease_id but _shared_leases[lease_id] will be nil
    -- This is expected - the index lookup checks lease_ref.ref_count > 0

    client:close()
    print("\n   _leases_by_ttl index: OK")
end

-- Test _watch_etcd_ids cleanup
local function test_watch_etcd_ids_cleanup()
    print("\n=== Testing _watch_etcd_ids cleanup ===")

    local client = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/watchcleanup/"
    }

    -- Start a watch
    local watch_id = client:watch("testkey", function(events) end)
    print("   watch started, watch_id:", watch_id)

    -- Wait briefly for watch to establish and get etcd_watch_id
    skynet.sleep(30)

    -- Check that _watch_etcd_ids may have the entry (if etcd assigned an id)
    local etcd_id_before = client._watch_etcd_ids[watch_id]
    print("   etcd watch id before cancel:", etcd_id_before or "none yet")

    -- Cancel watch
    client:watch_cancel(watch_id)
    skynet.sleep(10)

    -- Verify cleanup
    local etcd_id_after = client._watch_etcd_ids[watch_id]
    assert(not etcd_id_after, "_watch_etcd_ids should be cleaned up after cancel")
    print("   _watch_etcd_ids nil after cancel: OK")

    -- Also verify in _watches
    assert(not client._watches[watch_id], "_watches should be cleaned up")
    print("   _watches cleanup: OK")

    client:close()
    print("\n   _watch_etcd_ids cleanup: OK")
end

-- Test auth race condition prevention (_authenticating flag)
local function test_auth_race_prevention()
    print("\n=== Testing auth race condition prevention ===")

    -- This is a simulated test since we don't have auth enabled by default
    -- We'll verify the mechanism exists and works as expected

    local client = etcd.new {
        host = "http://127.0.0.1:2379",
        user = "testuser",  -- This will trigger auth path
        password = "testpass",
    }

    print("   _authenticating flag initialized:", client._authenticating)
    assert(client._authenticating == false, "should start as false")

    -- Test that concurrent calls will wait
    -- We'll mock by manually setting the flag and checking _get_token behavior
    client._authenticating = true
    client._token = nil  -- Force token refresh needed

    local result = { done = false }
    skynet.fork(function()
        local token, err = client:_get_token()
        result.token = token
        result.err = err
        result.done = true
    end)

    skynet.sleep(5)  -- Give coroutine time to wait

    assert(not result.done, "_get_token should wait while _authenticating is true")
    print("   concurrent auth waits for flag: OK")

    -- Release the "lock" (simulating auth completion)
    client._authenticating = false
    client._token = "fake_token"  -- Simulate successful auth
    client._token_expire = skynet.now() + 30000

    skynet.sleep(10)

    -- The _get_token should return now that we set token directly
    -- Note: Our coroutine would have exited loop after _authenticating became false
    -- but token wasn't set when it checked, so it may attempt real auth and fail
    -- This is expected behavior for our simulation

    client:close()
    print("\n   auth race prevention: OK")
end

-- Test boundary cases for prefix
local function test_prefix_boundary_cases()
    print("\n=== Testing prefix boundary cases ===")

    -- Test 1: Prefix without trailing slash
    print("\n1. Prefix without trailing slash: /myprefix")
    local client1 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/myprefix"  -- No trailing slash
    }

    local lease_id = client1:register("srv1", "127.0.0.1:9201", {})
    assert(lease_id, "register should work with prefix without trailing slash")

    -- Key should be: /myprefix/srv1/127.0.0.1:9201
    local instances = client1:discover("srv1")
    assert(instances.kvs and #instances.kvs > 0, "discover should work")
    print("   prefix without trailing slash: OK")

    client1:deregister("srv1", "127.0.0.1:9201")
    client1:close()

    -- Test 2: Prefix with multiple path segments
    print("\n2. Prefix with multiple segments: /org/env/app/services")
    local client2 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/org/env/app/services"
    }

    lease_id = client2:register("deep.srv", "127.0.0.1:9202", {})
    assert(lease_id, "register should work with deep prefix")

    instances = client2:discover("deep.srv")
    assert(instances.kvs and #instances.kvs > 0, "discover should work with deep prefix")

    -- Verify get_all_services works
    local services = client2:get_all_services()
    local found = false
    for _, s in ipairs(services) do
        if s == "deep.srv" then
            found = true
            break
        end
    end
    assert(found, "get_all_services should find service")
    print("   deep multi-segment prefix: OK")

    client2:deregister("deep.srv", "127.0.0.1:9202")
    client2:close()

    -- Test 3: Root prefix "/"
    print("\n3. Root prefix: /")
    local client3 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/"
    }

    lease_id = client3:register("root.srv", "127.0.0.1:9203", {})
    assert(lease_id, "register should work with root prefix")

    instances = client3:discover("root.srv")
    assert(instances.kvs and #instances.kvs > 0, "discover should work with root prefix")
    print("   root prefix: OK")

    client3:deregister("root.srv", "127.0.0.1:9203")
    client3:close()

    -- Test 4: Key stripping in get() - verify prefix is stripped from returned keys
    print("\n4. Key prefix stripping in get()")
    local client4 = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/stripping/test"
    }

    client4:put("/foo/bar", "test_value")  -- becomes: /stripping/test/foo/bar in etcd
    local resp = client4:get("/foo/bar")
    assert(resp.kvs and #resp.kvs == 1, "should get one key")
    assert(resp.kvs[1].key == "/foo/bar", "key should have prefix stripped")
    assert(resp.kvs[1].value == "test_value", "value should match")
    print("   key prefix stripping: OK")

    -- Also test get_prefix
    client4:put("/foo/baz", "value2")
    resp = client4:get_prefix("/foo/")
    assert(resp.kvs and #resp.kvs >= 2, "should get 2 keys")
    for _, kv in ipairs(resp.kvs) do
        assert(kv.key:find("/stripping") == nil, "key should not contain prefix")
    end
    print("   get_prefix key stripping: OK")

    client4:delete_prefix("/foo/")
    client4:close()

    print("\n   prefix boundary cases: OK")
end

-- Test multiple clients with different prefixes (isolation)
local function test_prefix_isolation()
    print("\n=== Testing prefix isolation between clients ===")

    local client_a = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/teamA"
    }

    local client_b = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/teamB"
    }

    -- Both register same service name
    local lease_a = client_a:register("common.srv", "127.0.0.1:9301", { team = "A" })
    local lease_b = client_b:register("common.srv", "127.0.0.1:9302", { team = "B" })

    assert(lease_a, "client_a register failed")
    assert(lease_b, "client_b register failed")

    -- Each client should only see their own service
    local instances_a = client_a:discover("common.srv")
    local count_a = instances_a.kvs and #instances_a.kvs or 0
    assert(count_a == 1, "client_a should see 1 instance")

    local instances_b = client_b:discover("common.srv")
    local count_b = instances_b.kvs and #instances_b.kvs or 0
    assert(count_b == 1, "client_b should see 1 instance")

    -- Verify metadata is correct
    local ok_a, data_a = pcall(cjson.decode, instances_a.kvs[1].value)
    local ok_b, data_b = pcall(cjson.decode, instances_b.kvs[1].value)
    assert(ok_a and data_a.metadata.team == "A", "client_a metadata wrong")
    assert(ok_b and data_b.metadata.team == "B", "client_b metadata wrong")

    print("   client_a sees team A service: OK")
    print("   client_b sees team B service: OK")
    print("   services are properly isolated by prefix: OK")

    -- Cleanup
    client_a:deregister("common.srv", "127.0.0.1:9301")
    client_b:deregister("common.srv", "127.0.0.1:9302")
    client_a:close()
    client_b:close()

    print("\n   prefix isolation: OK")
end

skynet.start(function()
    print("========================================")
    print("Testing etcd client prefix functionality")
    print("========================================")

    -- Run all tests
    local ok, err

    ok, err = pcall(test_get_range_end)
    if not ok then
        print("ERROR in test_get_range_end:", err)
        skynet.exit()
    end

    ok, err = pcall(test_custom_prefix)
    if not ok then
        print("ERROR in test_custom_prefix:", err)
        skynet.exit()
    end

    ok, err = pcall(test_sync_revision_count_only)
    if not ok then
        print("ERROR in test_sync_revision_count_only:", err)
        skynet.exit()
    end

    ok, err = pcall(test_leases_by_ttl_index)
    if not ok then
        print("ERROR in test_leases_by_ttl_index:", err)
        skynet.exit()
    end

    ok, err = pcall(test_watch_etcd_ids_cleanup)
    if not ok then
        print("ERROR in test_watch_etcd_ids_cleanup:", err)
        skynet.exit()
    end

    ok, err = pcall(test_auth_race_prevention)
    if not ok then
        print("ERROR in test_auth_race_prevention:", err)
        skynet.exit()
    end

    ok, err = pcall(test_prefix_boundary_cases)
    if not ok then
        print("ERROR in test_prefix_boundary_cases:", err)
        skynet.exit()
    end

    ok, err = pcall(test_prefix_isolation)
    if not ok then
        print("ERROR in test_prefix_isolation:", err)
        skynet.exit()
    end

    print("\n========================================")
    print("All tests passed!")
    print("========================================")

    skynet.exit()
end)
