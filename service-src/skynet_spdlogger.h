#ifndef SKYNET_SPDLOGGER_H
#define SKYNET_SPDLOGGER_H

#include <stddef.h>
#include <stdint.h>

#define PTYPE_TEXT 0
#define PTYPE_SYSTEM 4

extern "C" {
    struct skynet_context;
    void skynet_error(struct skynet_context * context, const char *msg, ...);
    typedef int (*skynet_cb)(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz);
    void skynet_callback(struct skynet_context * context, void *ud, skynet_cb cb);
}



#ifdef __cplusplus
#include <memory>
namespace spdlog {
    class logger;
}
struct spdlogger {
    std::shared_ptr<spdlog::logger> logger;
    const char *filename;
};
#else
struct spdlogger;
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct spdlogger * spdlogger_create(void);
void spdlogger_release(struct spdlogger *);
int spdlogger_init(struct spdlogger *, struct skynet_context *, const char *);

#ifdef __cplusplus
}
#endif

#endif
