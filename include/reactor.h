#ifndef REACTOR_H
#define REACTOR_H

typedef struct {
    int reactor_id;
    int epoll_fd;
    int wakeup_fd;         // eventfd used for signalling
    ConnectionContext* task_head;
    ConnectionContext* task_tail;
    pthread_mutex_t task_lock;
    _Atomic int wakeup_pending;
} Reactor;

// Reactor globals (defined in reactor.c)
extern Reactor* global_reactors;
extern __thread int epoll_fd;

extern void signal_reactor_task_bulk(int reactor_id, ConnectionContext** waiters, int count);
extern int make_socket_non_blocking(int fd);
extern void* worker_reactor_loop(void* args);
#endif