#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<unistd.h>
#include"thread_pool.h"

typedef struct Task{
    void* (*function)(void*);
    void* args;
    struct Task* next;
}Task;

static struct {
    Task* head;
    Task* tail;
    pthread_mutex_t lock;
    pthread_cond_t notify;
    pthread_t* threads;
    int thread_count;
    int shutdown;
}pool;

static void* worker_thread(void* args) {
    while(1) {
        pthread_mutex_lock(&pool.lock);

        while(pool.head == NULL && !pool.shutdown) {
            pthread_cond_wait(&pool.notify,&pool.lock);
        }

        if(pool.shutdown && pool.head == NULL) {
            pthread_mutex_unlock(&pool.lock);
            break;
        }

        Task* task = pool.head;
        pool.head = pool.head->next;
        if(pool.head == NULL) {
            pool.tail = NULL;
        }
        
        pthread_mutex_unlock(&pool.lock);

        if(task != NULL) {
            (*(task->function))(task->args);
            free(task);
        }
    }
    return NULL;
}

void init_thread_pool(int numThreads) {
    pool.thread_count = numThreads;
    pool.shutdown = 0;
    pool.head = NULL;
    pool.tail = NULL;
    pthread_mutex_init(&pool.lock,NULL);
    pthread_cond_init(&pool.notify,NULL);

    pool.threads = malloc(sizeof(pthread_t) * pool.thread_count);

    for(int i = 0; i < pool.thread_count; i++) {
       if(pthread_create(&pool.threads[i],NULL,worker_thread,NULL) != 0) {
           printf("Failed to create worker thread!\n");
       }
    }
}

void submit_task(void* (*function)(void*),void* args) {
    Task* task = malloc(sizeof(Task));
    task->function = function;
    task->args = args;
    task->next = NULL;

    pthread_mutex_lock(&pool.lock);
    if(pool.tail == NULL) {
        pool.head = task;
        pool.tail = task;
    } else {
        pool.tail->next = task;
        pool.tail = pool.tail->next;
    }
    pthread_cond_signal(&pool.notify);
    pthread_mutex_unlock(&pool.lock);
}

void destroy_thread_pool() {
    pthread_mutex_lock(&pool.lock);
    pool.shutdown = 1;
    pthread_cond_broadcast(&pool.notify);
    pthread_mutex_unlock(&pool.lock);

    for(int i = 0; i < pool.thread_count; i++) {
        pthread_join(pool.threads[i],NULL);
    }
    
    free(pool.threads);
    pthread_mutex_destroy(&pool.lock);
    pthread_cond_destroy(&pool.notify);
}