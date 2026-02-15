#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<pthread.h>
#include<string.h>
#include<stdint.h>
#include <inttypes.h>
#include"proxy_log.h"

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static FILE* log_file = NULL;
static LogLevel current_level = LEVEL_INFO; // Default to INFO

void init_log(LogLevel level) {
    pthread_mutex_lock(&log_lock);
    log_file = fopen("proxy.log","a");
    if(!log_file) {
        perror("Warning! Failed to open log file! Logging Disabled");
    }
    current_level = level;
    pthread_mutex_unlock(&log_lock);
}

void log_event(LogLevel level, uint64_t req_id, const char *client_ip,const char *event_msg) {
    if(!log_file) return;
    
    if (level < current_level) return;

    time_t now = time(NULL);
    struct tm* t = localtime(&now);
    char time_str[64];
    strftime(time_str,sizeof(time_str),"%Y-%m-%d %H:%M:%S",t);
    
    const char* safe_ip = client_ip ? client_ip : "-";

    const char *level_str = "UNK";
    switch(level) {
        case LEVEL_DEBUG: level_str = "DBG"; break;
        case LEVEL_INFO:  level_str = "INF"; break;
        case LEVEL_WARN:  level_str = "WRN"; break;
        case LEVEL_ERROR: level_str = "ERR"; break;
    }

    pthread_mutex_lock(&log_lock);
    
    // Format: [Time] [ReqID] [IP] [Level] Message
    fprintf(log_file, "[%s] [Req:%" PRIu64 "] [%s] [%s] %s\n", time_str, req_id, safe_ip, level_str, event_msg);
    fflush(log_file);

    pthread_mutex_unlock(&log_lock);
}

void close_log() {
    pthread_mutex_lock(&log_lock);
    if(log_file) {
        fclose(log_file);
        log_file = NULL;
    }
    pthread_mutex_unlock(&log_lock);
}