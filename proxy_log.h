#ifndef PROXY_LOG_H
#define PROXY_LOG_H

void init_log();

void log_message(const char*,const char*,int,long);

void close_log();
#endif