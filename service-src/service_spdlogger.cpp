#include "skynet_spdlogger.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>

#include "spdlog/async.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#define MAX_LOG_SIZE (50 * 1024 * 1024)
#define MAX_LOG_FILES 3

struct spdlogger *
spdlogger_create(void) {
    struct spdlogger *inst = new struct spdlogger(); 
    inst->filename = nullptr;
    return inst;
}

void
spdlogger_release(struct spdlogger *inst) {
    if (inst->logger) {
        inst->logger->flush();
    }
    if (inst->filename) {
        free((void *)inst->filename);
    }
    delete inst;
}

static int
spdlogger_cb(struct skynet_context * context, void *ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
    struct spdlogger *inst = (struct spdlogger *)ud;

    const char* m = (const char*)msg;
    if (type == PTYPE_TEXT && inst->logger) {
        if (sz >= 7 && strncmp(m, "[DEBUG]", 7) == 0) {
            inst->logger->debug(std::string(m, sz));
        } else if (sz >= 6 && strncmp(m, "[INFO]", 6) == 0) {
            inst->logger->info(std::string(m, sz));
        } else if (sz >= 6 && strncmp(m, "[WARN]", 6) == 0) {
            inst->logger->warn(std::string(m, sz));
        } else if (sz >= 7 && strncmp(m, "[ERROR]", 7) == 0) {
            inst->logger->error(std::string(m, sz));
        } else {
            inst->logger->info(std::string(m, sz));
        }
    }
    return 0;
}

int spdlogger_init(struct spdlogger * inst, struct skynet_context *ctx, const char * parm) {
    try {
        spdlog::init_thread_pool(8192, 1);

        std::vector<spdlog::sink_ptr> sinks;
        auto console_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
        sinks.push_back(console_sink);

        if (parm && parm[0] != '\0') {
            auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                parm, MAX_LOG_SIZE, MAX_LOG_FILES);
            sinks.push_back(rotating_sink);
        }

        inst->logger = std::make_shared<spdlog::async_logger>(
            "skynet", 
            sinks.begin(), 
            sinks.end(), 
            spdlog::thread_pool(), // 使用刚才初始化的全局线程池
            spdlog::async_overflow_policy::block // 当队列满时的策略：阻塞或丢弃
        );

        inst->logger->set_level(spdlog::level::trace);
        inst->logger->flush_on(spdlog::level::trace);
        skynet_callback(ctx, inst, spdlogger_cb);
        return 0;

    } catch (const spdlog::spdlog_ex &ex) {
        skynet_error(ctx, "spdlogger async init failed: %s", ex.what());
        return 1;
    }
}
