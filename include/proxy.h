#ifndef PROXY_H
#define PROXY_H

#include<time.h>
#include<netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include<stdint.h>
#include<signal.h>

#define BUFFER           8192
#define MAX_FDS          65536
#define EPOLL_BATCH_SIZE 128

#define NUM_REACTORS sysconf(_SC_NPROCESSORS_ONLN)

extern void init_upstream_dns_lock(void);

// Shutdown flag (defined in main.c)
extern volatile sig_atomic_t server_running;

#endif