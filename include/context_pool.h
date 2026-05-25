#ifndef CONTEXT_POOL_H
#define CONTEXT_POOL_H

#include"connection.h"

// Context Table(Contains fd to ConnectionContext mappings)
extern _Atomic(ConnectionContext*) context_table[];

// Atomic Request ID Counter
extern _Atomic uint64_t global_req_id;

extern void init_context_pool(void);
extern void destroy_context_pool(void);
extern ConnectionContext* create_context(int fd);
extern void free_context(ConnectionContext* ctx);

#endif




