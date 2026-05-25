#ifndef CACHE_H
#define CACHE_H


#include"connection.h"

typedef struct {
    time_t expires_at;
    time_t cached_at;
    long content_length;
    int upstream_header_len;
    char url[512];
    char etag[256];
}CacheHeader;

// Cache Lifecycle
extern void init_cache();
extern void destroy_cache();
extern void rehydrate_cache();

// Lookup / insertion — called from http_handler.c
extern int  check_cache(ConnectionContext* ctx);
extern int  add_to_cache_ram(ConnectionContext* ctx, char* url, time_t expires_at, time_t cached_at,
                      char* upstream_headers, int upstream_header_len,
                      char* body, long body_size);

// Waiter management — called from http_handler.c
extern void bypass_cache_for_waiters(char* url,void* node);
extern void abort_cache_download(char* url, void* node_ref);
extern void cache_not_modified(ConnectionContext* ctx);

// Reference counting — called from connection.c and http_handler.c
extern void acquire_cache_ref(void* ref);
extern void release_cache_ref(void* ref);

// PURGE API
extern int purge_cache_entry(char* url);

// Utility
extern void get_cache_filename(char*,char*);

//Cache Memory Tracking
extern _Atomic long total_cache_memory;

#endif