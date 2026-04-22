-- Test for fswatch (inotify-based) C service in Skynet
-- Usage: ./skynet test/test_fswatch.lua <path-to-monitor> [-r]

local skynet = require "skynet"
local fswatch = require "skynet.fswatch"

local function main(...)
	local path = ...
	if not path then
		path = "./tmp_test"
		os.execute("mkdir -p " .. path)
	end

	local recursive = true

	print(string.format("Starting fswatch test, monitoring: %s recursive=%s", path, tostring(recursive)))

	local mon = fswatch.new()

	mon:set_callback(function(wd, mask, name, path)
		local events = fswatch.decode_mask(mask)
		print(string.format("Event: wd=%d mask=%08x events={%s} name=%s path=%s",
			wd, mask, table.concat(events, ", "), name, path))
	end)

	mon:add(path, recursive)
	print(string.format("Added watch on %s recursive=%s", path, tostring(recursive)))
end

skynet.start(function()
	main()
end)
