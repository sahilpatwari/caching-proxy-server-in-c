#include<stdio.h>
#include<stdlib.h>
#include<time.h>
#include<pthread.h>
#include<string.h>
#include"proxy_log.h"

static pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
static FILE* log_file = NULL;

void init_log() {
    pthread_mutex_lock(&log_lock);
    log_file = fopen("proxy.log","a");
    if(!log_file) {
        perror("Warning! Failed to open log file! Logging Disabled");
    }
    pthread_mutex_unlock(&log_lock);
}

void log_message(const char* client_ip,const char* request_url,int status_code,long size) {
    if(!log_file) return;

    time_t now = time(NULL);
    struct tm* t = localtime(&now);
    char time_str[64];
    strftime(time_str,sizeof(time_str),"%Y-%m-%d %H:%M:%S",t);
    
    const char* safe_ip = client_ip ? client_ip : "-";
    const char* safe_url = request_url ? request_url : "-";
    pthread_mutex_lock(&log_lock);

    fprintf(log_file,"[%s] [%s] %s %d %ld bytes\n",time_str,safe_ip,safe_url,status_code,size);

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