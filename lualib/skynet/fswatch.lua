-- fswatch file monitoring (via inotify) helper for Skynet
-- Written by Claude Code

local skynet = require "skynet"
local coroutine = require "skynet.coroutine"

local fswatch = {}
local fswatch_mt = { __index = fswatch }

-- Event mask constants (same as inotify API)
fswatch.EVENT = {
	ACCESS = 0x00000001,
	MODIFY = 0x00000002,
	ATTRIB = 0x00000004,
	CLOSE_WRITE = 0x00000008,
	CLOSE_NOWRITE = 0x00000010,
	OPEN = 0x00000020,
	MOVED_FROM = 0x00000040,
	MOVED_TO = 0x00000080,
	CREATE = 0x00000100,
	DELETE = 0x00000200,
	DELETE_SELF = 0x00000400,
	MOVE_SELF = 0x00000800,
	UNMOUNT = 0x00002000,
	Q_OVERFLOW = 0x00004000,
	IGNORED = 0x00008000,
	ISDIR = 0x40000000,
}

-- Decode event mask to table of event names
function fswatch.decode_mask(mask)
	local events = {}
	for name, bit in pairs(fswatch.EVENT) do
		if mask & bit ~= 0 then
			table.insert(events, name)
		end
	end
	return events
end

-- Create a new inotify monitor
function fswatch.new()
	require "skynet.manager"
	local handle = skynet.launch("fswatch")
	local self = setmetatable({
		handle = handle,
		callback = nil,
	}, fswatch_mt)

	-- Subscribe to events
	skynet.send(handle, "text", "subscribe")

	-- Register response handler
	skynet.dispatch("text", function(_, _, msg)
		self:dispatch(msg)
	end)

	return self
end

-- Dispatch incoming events
function fswatch:dispatch(msg)
	local words = {}
	for word in string.gmatch(msg, "%S+") do
		table.insert(words, word)
	end
	
	if words[1] == "event" then
		local wd = tonumber(words[2])
		local mask = tonumber(words[3], 16)
		local name = words[4]
		if self.callback then
			self.callback(wd, mask, name)
		end
	elseif words[1] == "added" then
		-- added wd response
		-- ignored here since we use synchronous call
	end
end

-- Add a watch
function fswatch:add(path, recursive)
	if recursive then
		skynet.send(self.handle, "text", "add -r " .. path)
	else
		skynet.send(self.handle, "text", "add " .. path)
	end
	-- The response comes back as "added wd", but we don't wait for it in this API
end

-- Remove a watch
function fswatch:remove(wd)
	skynet.send(self.handle, "text", "remove " .. tostring(wd))
end

-- Set callback for events
function fswatch:set_callback(callback)
	self.callback = callback
end

-- Exit the inotify service
function fswatch:exit()
	skynet.exit(self.handle)
end

skynet.register_protocol {
	name = "text",
	id = skynet.PTYPE_TEXT,
	pack = function(...) return ... end,
	unpack = skynet.tostring,
}

return fswatch
