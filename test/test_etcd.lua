local skynet = require "skynet"
local etcd = require "skynet.db.etcd"

skynet.start(function()
    print("Testing etcd client with new prefix functionality...")

    -- Test backward compatibility: old /skynet/services pattern as custom prefix
    print("\n0. Test backward compatibility with /skynet/services prefix")
    local client_legacy = etcd.new {
        host = "http://127.0.0.1:2379",
        prefix = "/skynet/services"
    }
    local legacy_lease = client_legacy:register("legacy.srv", "127.0.0.1:7777", { legacy = true })
    assert(legacy_lease, "legacy prefix register should work")
    local legacy_instances = client_legacy:discover("legacy.srv")
    assert(legacy_instances.kvs and #legacy_instances.kvs > 0, "legacy prefix discover should work")
    client_legacy:deregister("legacy.srv", "127.0.0.1:7777")
    client_legacy:close()
    print("  backward compatibility with /skynet/services prefix: OK")

    local client = etcd.new {
        host = "http://127.0.0.1:2379"
    }

    print("\n1. Test put/get")
    client:put("/skynet/test/key1", "hello world")
    local resp = client:get("/skynet/test/key1")
    print("  got value:", resp.kvs[1].value)
    assert(resp.kvs and resp.kvs[1].value == "hello world", "get failed")

    print("\n2. Test prefix")
    client:put("/skynet/test/prefix/a", "value a")
    client:put("/skynet/test/prefix/b", "value b")
    resp = client:get_prefix("/skynet/test/prefix/")
    print("  prefix count:", resp.count)
    assert(tonumber(resp.count) == 2, "prefix get failed")

    print("\n3. Test delete")
    client:delete("/skynet/test/prefix/a")
    resp = client:get_prefix("/skynet/test/prefix/")
    print("  after delete count:", resp.count)
    assert(tonumber(resp.count) == 1, "delete failed")

    print("\n4. Test lease")
    local lease = client:lease_grant(5)
    print("  lease granted, id:", lease.ID)
    client:put("/skynet/test/lease_key", "lease_value", { lease = tonumber(lease.ID) })
    resp = client:get("/skynet/test/lease_key")
    print("  lease key exists:", resp.kvs and #resp.kvs > 0)

    print("\n5. Test service registration")
    local lease_id = client:register("test.service", "127.0.0.1:8080", { region = "test" })
    print("  registered with lease_id:", lease_id)
    local instances = client:discover("test.service")
    local count = instances.kvs and #instances.kvs or 0
    print("  discover found instances:", count)
    assert(count == 1, "service discovery failed - should find 1 instance")

    local registry = client:get_registry()
    print("  local registry entries:", #registry)
    assert(#registry == 1, "registry should have 1 entry")

    print("\n5b. Test shared lease - multiple registrations share same lease")
    local lease2 = client:register("test.service2", "127.0.0.1:8081", { region = "test2" })
    print("  second service lease_id:", lease2)
    print("  first service lease_id:", lease_id)
    assert(lease_id == lease2, "multiple registrations should share the same lease (shared mode)")
    registry = client:get_registry()
    print("  registry now has", #registry, "entries sharing one lease")
    assert(#registry == 2, "registry should have 2 entries")

    print("\n5c. Test reference counting - deregister one, lease still exists")
    client:deregister("test.service", "127.0.0.1:8080")
    local lease3 = client:register("test.service3", "127.0.0.1:8082", { region = "test3" })
    print("  new registration after deregister, lease_id:", lease3)
    assert(lease3 == lease2, "should reuse existing shared lease after deregister")
    registry = client:get_registry()
    assert(#registry == 2, "registry should have 2 entries")
    -- Re-register first service for cleanup
    lease_id = client:register("test.service", "127.0.0.1:8080", { region = "test" })

    print("\n6. Test watch (watch will run 3 seconds)")
    local watch_finished = false
    local watch_id = client:watch("/skynet/test/watch_key", function(events)
        for _, ev in ipairs(events) do
            print(string.format("  watch event: type=%s key=%s value=%s",
                ev.type or "PUT", ev.kv.key, ev.kv.value or ""))
        end
    end)

    skynet.timeout(10, function()
        client:put("/skynet/test/watch_key", "test watch value")
    end)

    skynet.timeout(100, function()
        client:watch_cancel(watch_id)
        watch_finished = true
    end)

    skynet.timeout(150, function()
        client:deregister("test.service", "127.0.0.1:8080")
        client:deregister("test.service2", "127.0.0.1:8081")
        print("\n7. Test deregistration")
        instances = client:discover("test.service")
        local count2 = instances.kvs and #instances.kvs or 0
        print("  instances after deregister:", count2)
        assert(count2 == 0, "deregister failed")

        instances = client:discover("test.service2")
        count2 = instances.kvs and #instances.kvs or 0
            count2 = count2 + 1
        end
        print("  service2 instances after deregister:", count2)
        assert(count2 == 0, "service2 deregister failed")

        registry = client:get_registry()
        print("  local registry after deregister:", #registry)

        print("\n8. Test conflict detection (simulated)")
        local lease1 = client:register("conflict.test", "127.0.0.1:9000", {})
        print("  registered with lease1:", lease1)

        local fake_lease = client:lease_grant(10)
        local fake_lease_id = tonumber(fake_lease.ID)
        print("  created fake lease2:", fake_lease_id)

        -- Key uses client.prefix (empty by default), not hardcoded /skynet/services
        local reg_key = "conflict.test/127.0.0.1:9000"
        client:put(reg_key, "fake_value", { lease = fake_lease_id })
        print("  overwrote key with different lease (simulating conflict)")

        skynet.sleep(30)

        instances = client:discover("conflict.test")
        print("  service still discoverable:", next(instances) ~= nil)

        local current_registry = client:get_registry()
        print("  current registry lease:", current_registry[1] and current_registry[1].lease_id)
        print("  lease changed due to conflict resolution:", lease1 ~= (current_registry[1] and current_registry[1].lease_id))

        client:deregister("conflict.test", "127.0.0.1:9000")
        client:delete_prefix("/skynet/test/")
        print("\n9. Cleanup complete")
        client:close()
        skynet.exit()
    end)
end)
