#ifndef CACHE_H
#define CACHE_H

#include"proxy.h"
#include"proxy_log.h"
#include"errors.h"

void init_cache();
void init_registry();
void destroy_cache();
void rehydrate_cache();
void get_cache_filename(char*,char*);
void enforce_cache_capacity();
void handle_check_cache(ConnectionContext *ctx);
void handle_send_cache(ConnectionContext *ctx);
void handle_fetch_upstream(ConnectionContext *ctx);
void remove_from_cache_ram(char* url);
#endif