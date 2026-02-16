#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<time.h>
#include<pthread.h>
#include<netinet/in.h>
#include"rate_limiter.h"

#define RATE_LIMITER_CAPACITY 10
#define refill_rate 1

typedef struct ClientNode{
    char ip[INET6_ADDRSTRLEN];
    double tokens;
    time_t last_refill;
    struct ClientNode* next;
}ClientNode;

static pthread_mutex_t limit_lock = PTHREAD_MUTEX_INITIALIZER;
static ClientNode* head = NULL;

void init_rate_limiter() {
    //Maybe required later for setup
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
        target->tokens += seconds_passed * refill_rate;
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
    pthread_mutex_lock(&limit_lock);
    while(head != NULL) {
        ClientNode* del = head;
        head = head->next;
        free(del);
    }
    pthread_mutex_unlock(&limit_lock);
}