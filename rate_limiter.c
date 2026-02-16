#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<time.h>
#include<pthread.h>
#include<unistd.h>
#include<netinet/in.h>
#include"rate_limiter.h"

#define RATE_LIMITER_CAPACITY 10
#define REFILL_RATE 1
#define CLIENT_TIMEOUT 60      // Remove client if inactive for 60s
#define CLEANUP_INTERVAL 30    // Run cleanup every 30s

typedef struct ClientNode{
    char ip[INET6_ADDRSTRLEN];
    double tokens;
    time_t last_refill;
    struct ClientNode* next;
}ClientNode;

static pthread_mutex_t limit_lock = PTHREAD_MUTEX_INITIALIZER;
static ClientNode* head = NULL;
pthread_t cleaner_thread;
static int running =1;

void* prune_stale_clients(void* args) {
    while(running) {
       sleep(CLEANUP_INTERVAL);

        time_t now = time(NULL);
        pthread_mutex_lock(&limit_lock);

        ClientNode* current = head;
        ClientNode* prev = NULL;
        int pruned_count = 0;

        while(current != NULL) {
            if(difftime(now,current->last_refill) > CLIENT_TIMEOUT) {
                ClientNode* to_free = current;

                if(prev == NULL) {
                    head = current->next;
                    current = head;
                } else {
                    prev->next = current->next;
                    current = current->next;
                }

                free(to_free);
                pruned_count++;
            } else {
                prev = current;
                current = current->next;
            }
        }

        pthread_mutex_unlock(&limit_lock);

        if(pruned_count > 0) {
            printf("[RateLimit] Pruned %d stale clients.\n", pruned_count);
        }
    }
    return NULL;
}
void init_rate_limiter() {
    if (pthread_create(&cleaner_thread, NULL, prune_stale_clients, NULL) != 0) {
        perror("Failed to start rate limiter cleanup thread");
    }
}

int check_rate_limit(char* client_ip) {
    pthread_mutex_lock(&limit_lock);
    ClientNode* target = head;
    while(target != NULL) {
        if(strcmp(target->ip,client_ip) == 0) {
            break;
        }
        target = target->next;
    }

    if(target == NULL) {
        target = malloc(sizeof(ClientNode));
        if(!target) {
            pthread_mutex_unlock(&limit_lock);
            return 1;
        } else {
            strncpy(target->ip,client_ip,INET6_ADDRSTRLEN - 1);
            target->ip[INET6_ADDRSTRLEN] = '\0';
            target->tokens = RATE_LIMITER_CAPACITY;
            target->last_refill = time(NULL);
            target->next = head;
            head = target;
        }
    }

    //Refill Logic
    time_t now = time(NULL);
    int seconds_passed = difftime(now,target->last_refill);
    if(seconds_passed > 0) {
        target->tokens += seconds_passed * REFILL_RATE;
        if(target->tokens > RATE_LIMITER_CAPACITY) {
            target->tokens = RATE_LIMITER_CAPACITY;
        }
        target->last_refill = now;
    }
    int allowed = 0;
    if(target->tokens > 1.0) {
        target->tokens -= 1.0;
        allowed = 1;
    } else {
        allowed = 0;
    }
    pthread_mutex_unlock(&limit_lock);
    return allowed;
    
}

void destroy_rate_limiter() {
    running = 0;
    pthread_cancel(cleaner_thread);
    pthread_join(cleaner_thread,NULL);
    pthread_mutex_lock(&limit_lock);
    while(head != NULL) {
        ClientNode* del = head;
        head = head->next;
        free(del);
    }
    pthread_mutex_unlock(&limit_lock);
}