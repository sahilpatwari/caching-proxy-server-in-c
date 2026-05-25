#ifndef CONFIG_H
#define CONFIG_H

#include"proxy_log.h"

typedef struct {
    int port;
    int backlog;
    int buffer;
    int max_fds;
    int epoll_batch_size;
    int pool_buckets;
    int pool_max_connections;
    int idle_timeout_sec;
    int latency_upstream_threshold;
    int cooldown_sec;
    int max_consecutive_errors;
    int max_cache_mem;
    int large_file_threshold;
    int default_ttl;
    int cache_buckets;
    int max_waiters;
    int expiry_interval;
    int prefetch_threshold;
    int prefetch_window;
    int rate_limiter_capacity;
    int refill_rate;
    int client_timeout;
    int cleanup_interval;
    int hash_table_size;
    LogLevel log_level;
    int log_queue_size;
    int max_log_msg;
    int log_batch_size;
} ProxyConfig;

extern ProxyConfig global_config;
void load_config(const char* filename);

#endif