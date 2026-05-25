#ifndef METRICS_H
#define METRICS_H

#include <stdint.h>
#include <stdatomic.h>
#include <time.h>

// Telemetry Globals
extern _Atomic uint64_t metric_cache_hits;
extern _Atomic uint64_t metric_cache_misses;
extern _Atomic uint64_t metric_total_requests;
extern _Atomic uint64_t metric_current_rps;
extern _Atomic uint64_t metric_cpu_usage;
extern _Atomic long metric_memory_rss_kb;
extern time_t server_start_time;

extern void* telemetry_worker(void* arg);
void* connection_reaper_worker(void* arg);
#endif