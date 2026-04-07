#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stddef.h>
#include "proxy.h"

ProxyConfig global_config;

typedef enum { TYPE_INT, TYPE_LOGLEVEL } ConfigType;

// The lookup map structure
typedef struct {
    const char* key;
    size_t offset;
    ConfigType type;
} ConfigMap;

// Macros to make the lookup table perfectly clean and readable
#define CFG_INT(k) { #k, offsetof(ProxyConfig, k), TYPE_INT }
#define CFG_LOG(k) { #k, offsetof(ProxyConfig, k), TYPE_LOGLEVEL }

// The Lookup Table
static const ConfigMap config_table[] = {
    CFG_INT(port),
    CFG_INT(backlog),
    CFG_INT(buffer),
    CFG_INT(max_fds),
    CFG_INT(epoll_batch_size),
    CFG_INT(pool_buckets),
    CFG_INT(pool_max_connections),
    CFG_INT(idle_timeout_sec),
    CFG_INT(latency_upstream_threshold),
    CFG_INT(cooldown_sec),
    CFG_INT(max_consecutive_errors),
    CFG_INT(max_cache_mem),
    CFG_INT(large_file_threshold),
    CFG_INT(default_ttl),
    CFG_INT(cache_buckets),
    CFG_INT(max_waiters),
    CFG_INT(expiry_interval),
    CFG_INT(prefetch_threshold),
    CFG_INT(prefetch_window),
    CFG_INT(rate_limiter_capacity),
    CFG_INT(refill_rate),
    CFG_INT(client_timeout),
    CFG_INT(cleanup_interval),
    CFG_INT(hash_table_size),
    CFG_LOG(log_level),
    CFG_INT(log_queue_size),
    CFG_INT(max_log_msg),
    CFG_INT(log_batch_size),
    {NULL, 0, TYPE_INT} // Array terminator
};

// Helper: Trim leading & trailing whitespace
static char* trim_whitespace(char* str) {
    char* end;
    while(isspace((unsigned char)*str)) str++;
    if(*str == 0) return str;
    end = str + strlen(str) - 1;
    while(end > str && isspace((unsigned char)*end)) end--;
    end[1] = '\0';
    return str;
}

void load_config(const char* filename) {
    // 1. Set System Defaults (in case proxy.conf is missing or incomplete)
    global_config.port = 3490;
    global_config.backlog = 4096;
    global_config.buffer = 8192;
    global_config.max_fds = 65536;
    global_config.epoll_batch_size = 512;
    global_config.pool_buckets = 1024;
    global_config.pool_max_connections = 12000;
    global_config.idle_timeout_sec = 30;
    global_config.latency_upstream_threshold = 1500000;
    global_config.cooldown_sec = 5;
    global_config.max_consecutive_errors = 20;
    global_config.max_cache_mem = 52428800;
    global_config.large_file_threshold = 1048576;
    global_config.default_ttl = 300;
    global_config.cache_buckets = 8192;
    global_config.max_waiters = 8192;
    global_config.expiry_interval = 60;
    global_config.prefetch_threshold = 5;
    global_config.prefetch_window = 30;
    global_config.rate_limiter_capacity = 10;
    global_config.refill_rate = 1;
    global_config.client_timeout = 300;
    global_config.cleanup_interval = 120;
    global_config.hash_table_size = 4096;
    global_config.log_level = LEVEL_ERROR; // Default to proxy_log.h enum
    global_config.log_queue_size = 8192;
    global_config.max_log_msg = 256;
    global_config.log_batch_size = 1024;

    // 2. Open File
    FILE* file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "[System] No %s found. Proceeding with safe defaults.\n", filename);
        return;
    }

    // 3. Parse Configuration using the Lookup Table
    char line[512];
    while (fgets(line, sizeof(line), file)) {
        char* trimmed = trim_whitespace(line);
        if (trimmed[0] == '#' || trimmed[0] == '\0') {
            continue; // Skip comments and empty lines
        }

        char* delimiter = strchr(trimmed, '=');
        if (!delimiter) continue;

        *delimiter = '\0';
        char* key = trim_whitespace(trimmed);
        char* value = trim_whitespace(delimiter + 1);

        int found = 0;
        
        // Loop purely through our configuration definitions
        for (int i = 0; config_table[i].key != NULL; i++) {
            if (strcmp(key, config_table[i].key) == 0) {
                found = 1;
                
                // Write dynamically mapped data directly into the struct's memory
                if (config_table[i].type == TYPE_INT) {
                    int* field_ptr = (int*)((char*)&global_config + config_table[i].offset);
                    *field_ptr = atoi(value);
                } else if (config_table[i].type == TYPE_LOGLEVEL) {
                    LogLevel* field_ptr = (LogLevel*)((char*)&global_config + config_table[i].offset);
                    if (strcmp(value, "LEVEL_DEBUG") == 0) *field_ptr = LEVEL_DEBUG;
                    else if (strcmp(value, "LEVEL_INFO") == 0)  *field_ptr = LEVEL_INFO;
                    else if (strcmp(value, "LEVEL_WARN") == 0)  *field_ptr = LEVEL_WARN;
                    else if (strcmp(value, "LEVEL_ERROR") == 0) *field_ptr = LEVEL_ERROR;
                    else {
                        fprintf(stderr, "proxy.conf warning: Invalid log_level '%s', defaulting to LEVEL_ERROR\n", value);
                        *field_ptr = LEVEL_ERROR;
                    }
                }
                break;
            }
        }

        if (!found) {
            fprintf(stderr, "proxy.conf warning: Ignored unknown config key '%s'\n", key);
        }
    }
    fclose(file);
}
