local skynet = require "skynet"


skynet.register_protocol {
	name = "text",
	id = skynet.PTYPE_TEXT,
	pack = function(...) return ... end,
	unpack = skynet.tostring,
}

skynet.dispatch("text", function(session, source, msg)
	local strList = {}
	string.gsub(msg, "[^,]+" , function(w) table.insert(strList, w) end)
	print("path", strList[1], "event", strList[2])
end)

local fsw_event_flag = {
	NoOp = 0,                     --< No event has occurred. */
	PlatformSpecific = (1 << 0),  --< Platform-specific placeholder for event type that cannot currently be mapped. */
	Created = (1 << 1),           --< An object was created. */
	Updated = (1 << 2),           --< An object was updated. */
	Removed = (1 << 3),           --< An object was removed. */
	Renamed = (1 << 4),           --< An object was renamed. */
	OwnerModified = (1 << 5),     --< The owner of an object was modified. */
	AttributeModified = (1 << 6), --< The attributes of an object were modified. */
	MovedFrom = (1 << 7),         --< An object was moved from this location. */
	MovedTo = (1 << 8),           --< An object was moved to this location. */
	IsFile = (1 << 9),            --< The object is a file. */
	IsDir = (1 << 10),            --< The object is a directory. */
	IsSymLink = (1 << 11),        --< The object is a symbolic link. */
	Link = (1 << 12),             --< The link count of an object has changed. */
	Overflow = (1 << 13)          --< The event queue has overflowed. */
};

skynet.start(function()
	local fswatch = require "fswatch"
	fswatch.init_library()
	fswatch.set_verbose(true)
	local session = fswatch.new_session(skynet.self())
	session:set_recursive(true)
	session:set_directory_only(true)
	session:set_follow_symlinks(false)
	session:set_allow_overflow(true)
	session:set_latency(0.01)
	session:add_event_type_filter(fsw_event_flag.Created)
	session:add_event_type_filter(fsw_event_flag.Updated)
	session:add_event_type_filter(fsw_event_flag.AttributeModified)

	session:add_path("./test")


	session:start_monitor()
	_G.session = session
end)
