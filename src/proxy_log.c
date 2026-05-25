#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<pthread.h>
#include<string.h>
#include<stdint.h>
#include <inttypes.h>
#include"proxy_log.h"

#define LOG_QUEUE_SIZE 8192
#define MAX_LOG_MSG 256
#define LOG_BATCH_SIZE 1024

typedef struct LogEntry {
    char msg[MAX_LOG_MSG];
}LogEntry;

static LogEntry LogQueue[LOG_QUEUE_SIZE];
static int head = 0;
static int tail = 0;
static int count = 0;
static int shutdown_flag = 0;

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t log_notify = PTHREAD_COND_INITIALIZER;
static pthread_cond_t log_space = PTHREAD_COND_INITIALIZER;
static pthread_t logger_worker_thread;

static FILE* log_file = NULL;
static LogLevel current_level = LEVEL_INFO; // Default to INFO

static void* logger_worker(void* args) {
    while(1) {
        pthread_mutex_lock(&log_lock);

        while(count == 0 && !shutdown_flag) {
            pthread_cond_wait(&log_notify,&log_lock);
        }
        
        if(shutdown_flag && count == 0) {
            pthread_mutex_unlock(&log_lock);
            return NULL;
        }

        LogEntry temp_buf[LOG_BATCH_SIZE];
        int batch_size = 0;

        while(count > 0 && batch_size < LOG_BATCH_SIZE) {
            memcpy(&temp_buf[batch_size],&LogQueue[head],sizeof(LogEntry));
            head = (head + 1) % LOG_QUEUE_SIZE;
            count--;
            batch_size++;
        }

        pthread_cond_broadcast(&log_space);
        pthread_mutex_unlock(&log_lock);
        
        if(log_file) {
            for(int i = 0; i < batch_size; i++) {
                fputs(temp_buf[i].msg,log_file);
            }
            fflush(log_file);
        }
    }
    return NULL;
}
void init_log(LogLevel level) {
    log_file = fopen("proxy.log","a");
    if(!log_file) {
        perror("Warning! Failed to open log file! Logging Disabled");
    }
    current_level = level;
    shutdown_flag = 0;

    if(pthread_create(&logger_worker_thread,NULL,logger_worker,NULL) != 0) {
        perror("Failed to create logger worker thread");
    }
}

void log_event(LogLevel level, uint64_t req_id, const char *client_ip,const char *event_msg) {
    if(!log_file) return;
    
    if (level < current_level) return;

    time_t now = time(NULL);
    struct tm t_storage;
    struct tm* t = localtime_r(&now,&t_storage);
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

    // Format: [Time] [ReqID] [IP] [Level] Message
    char formatted_message[MAX_LOG_MSG];
    snprintf(formatted_message, sizeof(formatted_message),"[%s] [Req:%" PRIu64 "] [%s] [%s] %s\n", time_str, req_id, safe_ip, level_str, event_msg);

    pthread_mutex_lock(&log_lock);
    
    while(count >= LOG_QUEUE_SIZE && !shutdown_flag) {
        pthread_cond_wait(&log_space,&log_lock);
    }
    
    if(!shutdown_flag) {
        strncpy(LogQueue[tail].msg,formatted_message,MAX_LOG_MSG - 1);
        LogQueue[tail].msg[MAX_LOG_MSG - 1] = '\0';
        tail = (tail + 1) % LOG_QUEUE_SIZE;
        count++;
        
        if(count >= 128) {
            pthread_cond_broadcast(&log_notify);
        }
    }

    pthread_mutex_unlock(&log_lock);
}

void close_log() {
    pthread_mutex_lock(&log_lock);

    shutdown_flag = 1;
    pthread_cond_broadcast(&log_notify);
    pthread_cond_broadcast(&log_space);
    pthread_mutex_unlock(&log_lock);

    pthread_join(logger_worker_thread,NULL);

    if(log_file) {
        fclose(log_file);
        log_file = NULL;
    }
}