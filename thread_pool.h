#ifndef THREAD_POOL_H
#define THREAD_POOL_H


void init_thread_pool(int,int);

int submit_task(void* (*function)(void*),void*);

void destroy_thread_pool();
#endif