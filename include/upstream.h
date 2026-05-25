#ifndef UPSTREAM_H
#define UPSTREAM_H

#include"connection.h"

// Upstream health globals (defined in upstream.c)
extern _Atomic int      active_upstream_connections;
extern _Atomic uint64_t global_upstream_latency_us;
extern _Atomic time_t   circuit_cooldown_until;
extern _Atomic int      consecutive_upstream_errors;

extern void init_connection_pool(void);
extern int get_pool_connection(char* hostname,int port);
extern void stash_connection(int fd, char* hostname, int port);
extern void flush_connection_pool(char* hostname,int port);

extern int resolve_origin_dns(const char* hostname,const char* port_str);
extern void* dns_refresh_worker(void* arg);
extern void handle_connect_upstream(ConnectionContext* ctx);
extern void handle_wait_connect(ConnectionContext *ctx);
extern void get_upstream_info(char* hostname_out, int size, int* port_out);

#endif