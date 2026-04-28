local skynet = require "skynet"
local etcd = require "skynet.db.etcd"
local arg = {...}

skynet.start(function()
    local client = etcd.new {
        hosts = {"http://127.0.0.1:2370", "http://127.0.0.1:2379"}
    }

    local service_name = arg[1] or "test.service"
    local service_addr = arg[2] or "127.0.0.1:8080"

    print("Registering service:", service_name, "at", service_addr)

    local lease_id, err = client:register(service_name, service_addr, {
        protocol = "http",
        weight = 100,
        region = "us-west",
    })
    if lease_id then
        print("Service registered successfully, lease_id:", lease_id)
    else
        print("Service registered failed, err:", err)
    end

    skynet.timeout(300, function()
        print("Deregistering service...")
        client:deregister(service_name, service_addr)
        client:close()
        skynet.exit()
    end)
end)
