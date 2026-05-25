
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<time.h>
#include<sys/time.h>

#include"proxy.h"
#include"proxy_log.h"
#include"config.h"
#include"context_pool.h"
#include"reactor.h"
#include"upstream.h"

// TELEMETRY GLOBALS
_Atomic uint64_t metric_cache_hits = 0;
_Atomic uint64_t metric_cache_misses = 0;
_Atomic uint64_t metric_total_requests = 0;
_Atomic uint64_t metric_current_rps = 0;
_Atomic uint64_t metric_cpu_usage = 0;
_Atomic long metric_memory_rss_kb = 0;
time_t server_start_time;

void* telemetry_worker(void* arg) {
    uint64_t last_total_requests = 0;
    double last_cpu_time = 0.0;
    long page_size = sysconf(_SC_PAGESIZE); // Usually 4096 bytes

    while(server_running) { 
        // 1. HARDWARE-ACCURATE CPU USAGE (Nanosecond Precision)
        struct timespec ts;
        if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) == 0) {
            // Convert seconds and nanoseconds into a single double
            double current_cpu = (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
            
            if (last_cpu_time > 0.0) {
                // If it consumed 0.5 seconds of CPU time in the last 1.0 wall-clock seconds, that is 50% CPU.
                // Across 12 cores, max is 1200%
                metric_cpu_usage = (uint64_t)((current_cpu - last_cpu_time) * 10000.0);
            }
            last_cpu_time = current_cpu;
        }

        // 2. BULLETPROOF RAM USAGE (Page Table Math)
        FILE* fp = fopen("/proc/self/statm", "r");
        if (fp) {
            long size, resident;
            // statm format: size resident shared text lib data dt
            if (fscanf(fp, "%ld %ld", &size, &resident) == 2) {
                // resident is in Pages. Multiply by page_size to get bytes, divide by 1024 for KB
                metric_memory_rss_kb = (resident * page_size) / 1024;
            }
            fclose(fp);
        }

        uint64_t current_total = metric_total_requests;
        metric_current_rps = current_total - last_total_requests;
        last_total_requests = current_total;

        sleep(1);
    }

    return NULL;
}

void* connection_reaper_worker(void* arg) {
    while(server_running) {
        sleep(5);
        time_t now = time(NULL);
        int reaped_per_core = MAX_FDS / NUM_REACTORS;
        ConnectionContext* reaper[NUM_REACTORS][reaped_per_core];
        int reaper_count[NUM_REACTORS];
        memset(reaper_count,0,sizeof(reaper_count));
        for(int i = 0; i < MAX_FDS; i++) {
            ConnectionContext* ctx = atomic_load(&context_table[i]);
            if(ctx != NULL) {
                time_t last_act = atomic_load(&ctx->last_active);
                if(last_act > 0 && (now - last_act) > global_config.idle_timeout_sec) {
                    pthread_mutex_lock(&ctx->state_lock);
                    if(ctx->state != STATE_CLOSE) {
                        if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM || ctx->state == STATE_FETCH_UPSTREAM) {
                            int current_errors = atomic_fetch_add(&consecutive_upstream_errors,1) + 1;
                            char log_buf[128];
                            snprintf(log_buf,sizeof(log_buf),"Reaper. Origin Timed Out. Strike: %d/%d %d",current_errors,global_config.max_consecutive_errors,(int)ctx->state);
                            log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
                        } else {
                            char log_buf[128];
                            snprintf(log_buf,sizeof(log_buf),"Reaper snipered idle connection on FD %d %d",ctx->client_fd,(int)ctx->state);
                            log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
                        }

                        
                        ctx->state = STATE_CLOSE;
                        if(ctx->client_fd != -1) {
                            int r_id = ctx->reactor_id;
                            reaper[r_id][reaper_count[r_id]++] = ctx;
                        }
                    }
                    pthread_mutex_unlock(&ctx->state_lock);
                }
            }
        }

        for(int i = 0; i < NUM_REACTORS; i++) {
            if(reaper_count[i] > 0) {
                signal_reactor_task_bulk(i,reaper[i],reaper_count[i]);
            }
        }
    }
    return NULL;
}
