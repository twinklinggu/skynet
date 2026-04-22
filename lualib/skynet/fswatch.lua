-- fswatch file monitoring (via inotify) helper for Skynet
-- Written by Claude Code

local skynet = require "skynet"
local coroutine = require "skynet.coroutine"
require "skynet.manager"

local fswatch = {}
local fswatch_mt = { __index = fswatch }

local DEDUP_WINDOW = 10		-- 10ms de-duplication window
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
	local handle = skynet.launch("fswatch")
	local self = setmetatable({
		handle = handle,
		callback = nil,
		dedup = true,
		_last_event = {},
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
	local fields = {}
	for field in string.gmatch(msg, "[^\t]+") do
		table.insert(fields, field)
	end
	
	if fields[1] == "event" then
		local wd = tonumber(fields[2])
		local mask = tonumber(fields[3], 16)
		local path = fields[4]
		local name = fields[5] or ""
		
		if self.dedup then
			local key = wd .. ":" .. mask .. ":" .. path
			local now = skynet.now()
			if self._last_event[key] and now - self._last_event[key] < DEDUP_WINDOW then
				return
			end
			self._last_event[key] = now
		end
		
		if self.callback then
			self.callback(wd, mask, name, path)
		end
	elseif fields[1] == "added" then
		if self.add_cb then
			self.add_cb(tonumber(fields[2]))
			self.add_cb = nil
		end
	end
end

-- Add a watch (async, returns immediately)
function fswatch:add(path, recursive)
	if recursive then
		skynet.send(self.handle, "text", "add -r " .. path)
	else
		skynet.send(self.handle, "text", "add " .. path)
	end
end

-- Add a watch (sync, returns watch descriptor)
function fswatch:add_sync(path, recursive)
	local co = coroutine.running()
	self.add_cb = function(wd)
		skynet.wakeup(co)
	end
	self:add(path, recursive)
	skynet.wait()
end

-- Remove a watch by path
function fswatch:remove(path)
	skynet.send(self.handle, "text", "remove " .. path)
end

-- Set callback for events
function fswatch:set_callback(callback)
	self.callback = callback
end

-- Enable/disable event de-duplication (default: enabled)
function fswatch:set_dedup(enable)
	self.dedup = enable
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
