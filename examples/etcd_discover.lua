local skynet = require "skynet"
local etcd = require "skynet.db.etcd"
local arg = {...}

skynet.start(function()
    local client = etcd.new {
        host = "http://127.0.0.1:2379"
    }

    local service_name = arg[1] or "test.service"

    print("Discovering service:", service_name)

    local instances = client:discover(service_name)
    for addr, meta in pairs(instances) do
        print(string.format("  - %s (protocol=%s weight=%s)", addr, meta.protocol, meta.weight))
    end

    print("\nWatching for service changes...")

    client:watch_service(service_name, function(action, addr, meta)
        if action == "add" then
            print(string.format("  + Service UP: %s (protocol=%s weight=%s)", addr, meta.protocol, meta.weight))
        elseif action == "remove" then
            print(string.format("  - Service DOWN: %s", addr))
        end
    end)
end)
