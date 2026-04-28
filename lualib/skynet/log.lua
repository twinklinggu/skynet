local skynet = require "skynet"

local M = {
    TRACE = 0,
    DEBUG = 1,
    INFO  = 2,
    WARN  = 3,
    ERROR = 4,
    FATAL = 5,
}

local level_names = {
    [M.TRACE] = "[TRACE]",
    [M.DEBUG] = "[DEBUG]",
    [M.INFO]  = "[INFO]",
    [M.WARN]  = "[WARN]",
    [M.ERROR] = "[ERROR]",
    [M.FATAL] = "[FATAL]",
}

local current_level = M.INFO

local logger_service

local function format_msg(level, ...)
    local args = {...}
    local n = #args
    if n == 0 then
        return level_names[level] .. " "
    end
    
    local parts = { level_names[level] }
    for i = 1, n do
        local v = args[i]
        if type(v) == "nil" then
            parts[#parts + 1] = "nil"
        elseif type(v) == "table" then
            local ok, str = pcall(require("cjson").encode, v)
            if ok then
                parts[#parts + 1] = str
            else
                parts[#parts + 1] = tostring(v)
            end
        else
            parts[#parts + 1] = tostring(v)
        end
    end
    return table.concat(parts, " ")
end

local function send_log(msg)
    if logger_service then
        skynet.send(logger_service, "text", msg)
    else
        skynet.error(msg)
    end
end

function M.trace(...)
    if current_level <= M.TRACE then
        send_log(format_msg(M.TRACE, ...))
    end
end

function M.debug(...)
    if current_level <= M.DEBUG then
        send_log(format_msg(M.DEBUG, ...))
    end
end

function M.info(...)
    if current_level <= M.INFO then
        send_log(format_msg(M.INFO, ...))
    end
end

function M.warn(...)
    if current_level <= M.WARN then
        send_log(format_msg(M.WARN, ...))
    end
end

function M.error(...)
    if current_level <= M.ERROR then
        send_log(format_msg(M.ERROR, ...))
    end
end

function M.fatal(...)
    if current_level <= M.FATAL then
        send_log(format_msg(M.FATAL, ...))
    end
end

function M.set_logger(addr)
    logger_service = addr
end

function M.set_level(level)
    current_level = level
end

function M.level_name(level)
    return level_names[level] or "[UNKNOWN]"
end

return M
