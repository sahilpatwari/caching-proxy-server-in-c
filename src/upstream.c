#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<sys/epoll.h>
#include<errno.h>
#include<fcntl.h>

#include"proxy.h"
#include"proxy_log.h"
#include"config.h"
#include"context_pool.h"
#include"workers.h"
#include"reactor.h"
#include"http_handler.h"

#define POOL_BUCKETS 1024

// Circuit Breaker Globals
_Atomic uint64_t global_upstream_latency_us = 50000;
_Atomic time_t circuit_cooldown_until = 0;
_Atomic int consecutive_upstream_errors = 0;

_Atomic int active_upstream_connections = 0;

typedef struct ConnectionNode {
    int fd;
    struct ConnectionNode* next;
} ConnectionNode;

typedef struct HostEntry{
    char hostname[256];
    int port;
    ConnectionNode* fd_head;   
    struct HostEntry* next_host;      
} HostEntry;

typedef struct HostBucket{
    HostEntry* head;
    pthread_mutex_t bucket_lock;
}HostBucket;


typedef struct UpstreamConfig {
    char hostname[256];
    int port;
    struct sockaddr_storage resolved_addr;
    socklen_t resolved_addr_len;
    int is_resolved;
    pthread_rwlock_t dns_lock;
}UpstreamConfig;

UpstreamConfig upstream_config;

void init_upstream_dns_lock(void) {
    pthread_rwlock_init(&upstream_config.dns_lock, NULL);
}

void get_upstream_info(char* hostname_out, int size, int* port_out) {
    pthread_rwlock_rdlock(&upstream_config.dns_lock);
    strncpy(hostname_out, upstream_config.hostname, size - 1);
    hostname_out[size - 1] = '\0';
    *port_out = upstream_config.port;
    pthread_rwlock_unlock(&upstream_config.dns_lock);
}

static HostBucket connection_map[POOL_BUCKETS];

static unsigned long hash_host(const char* hostname,int port) {
    unsigned long hash = 5381;
    int c;
    while((c = *hostname++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    hash = ((hash << 5) + hash) + port;
    return hash % POOL_BUCKETS;
}

void init_connection_pool() {
    for (int i = 0; i < POOL_BUCKETS; i++) {
        pthread_mutex_init(&connection_map[i].bucket_lock, NULL);
        connection_map[i].head = NULL;
    }
}

int get_pool_connection(char* hostname,int port) {
    int idx = hash_host(hostname,port);
    HostBucket* bucket = &connection_map[idx];
    
    int pool_fd = -1;
    pthread_mutex_lock(&bucket->bucket_lock);

    HostEntry* curr_host = bucket->head;
    while(curr_host != NULL) {
        if(strcmp(curr_host->hostname,hostname) == 0 && curr_host->port == port) {
            if(curr_host->fd_head != NULL) {
                 ConnectionNode* node = curr_host->fd_head;
                 pool_fd = node->fd;

                 curr_host->fd_head = node->next;
                 free(node);

                 char peek_buf[1];
                 int res = recv(pool_fd,peek_buf,sizeof(peek_buf),MSG_PEEK | MSG_DONTWAIT);
                 if(res >= 0 || (res < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
                    close(pool_fd);
                    atomic_fetch_sub(&active_upstream_connections,1);
                    pool_fd = -1;
                    continue;
                 }
            }
            break;
        }
        curr_host = curr_host->next_host;
    }
    pthread_mutex_unlock(&bucket->bucket_lock);
    return pool_fd;
}

void stash_connection(int fd, char* hostname, int port) {
    int idx = hash_host(hostname,port);
    HostBucket* bucket = &connection_map[idx];

    pthread_mutex_lock(&bucket->bucket_lock);

    HostEntry* curr_host = bucket->head;
    HostEntry* target_host = NULL;
    while(curr_host != NULL) {
        if(strcmp(curr_host->hostname,hostname) == 0 && curr_host->port == port) {
            target_host = curr_host;
            break;
        }
        curr_host = curr_host->next_host;
    }

    if(target_host == NULL) {
        target_host = malloc(sizeof(HostEntry));
        if(!target_host) {
            perror("malloc");
            atomic_fetch_sub(&active_upstream_connections, 1);
            close(fd);
            pthread_mutex_unlock(&bucket->bucket_lock);
            return;
        }

        strncpy(target_host->hostname,hostname,sizeof(target_host->hostname) - 1);
        target_host->port = port;
        target_host->fd_head = NULL;
        target_host->next_host = bucket->head;
        bucket->head = target_host;
    }

    ConnectionNode* node = malloc(sizeof(ConnectionNode));
    if(!node) {
        perror("malloc");
        close(fd);
        pthread_mutex_unlock(&bucket->bucket_lock);
        return;
    }

    node->fd = fd;
    node->next = target_host->fd_head;
    target_host->fd_head = node;

    pthread_mutex_unlock(&bucket->bucket_lock);
    return;
}

void flush_connection_pool(char* hostname,int port) {
    int idx = hash_host(hostname,port);
    HostBucket* bucket = &connection_map[idx];

    pthread_mutex_lock(&bucket->bucket_lock);
    HostEntry* curr_host = bucket->head;
    while(curr_host != NULL) {
        if(strcmp(curr_host->hostname,hostname) == 0 && curr_host->port == port) {
            ConnectionNode* node = curr_host->fd_head;
            int closed_count = 0;

            while(node != NULL) {
                close(node->fd);
                atomic_fetch_sub(&active_upstream_connections,1);

                ConnectionNode* temp = node;
                node = node->next;
                free(temp);
                closed_count++;
            }
            
            curr_host->fd_head = NULL;
            if(closed_count > 0) {
                char log_buf[128];
                snprintf(log_buf,sizeof(log_buf),"Pool Flush: Closed %d staled sockets for %s",closed_count,hostname);
                log_event(LEVEL_INFO,0,"[system]",log_buf);
            }

            break;
        }
        curr_host = curr_host->next_host;
    }
    pthread_mutex_unlock(&bucket->bucket_lock);
}

int resolve_origin_dns(const char* hostname,const char* port_str) {
    strncpy(upstream_config.hostname,hostname,sizeof(upstream_config.hostname) - 1);
    upstream_config.hostname[sizeof(upstream_config.hostname) - 1] = '\0';

    upstream_config.port = atoi(port_str);
    upstream_config.is_resolved = 0;

    struct addrinfo hints,*res;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    
    int max_retries = 5;
    int retry_delay = 1;
    for(int i = 0; i < max_retries; i++) {
        int status = getaddrinfo(hostname,port_str,&hints,&res);
        if(status == 0) {
            memcpy(&upstream_config.resolved_addr,res->ai_addr,res->ai_addrlen);
            upstream_config.resolved_addr_len = res->ai_addrlen;
            upstream_config.is_resolved = 1;

            freeaddrinfo(res);
            printf("Origin Server DNS resolved. Hostname %s Port %s\n",hostname,port_str);
            return 0;
        }

        fprintf(stderr,"[Startup] DNS Resolution Failed (%s). Retrying in %d seconds........",gai_strerror(status),retry_delay);
        sleep(retry_delay);
        retry_delay *= 2;
    }

    fprintf(stderr,"Failed to resolve origin server DNS even after %d tries. Shutting Down\n",max_retries);
    return -1;
}

void* dns_refresh_worker(void* arg) {
    char port_str[16];
    snprintf(port_str,sizeof(port_str),"%d",upstream_config.port);
    
    while(server_running) {
        sleep(60);

        struct addrinfo hints,*res;
        memset(&hints,0,sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        int status = getaddrinfo(upstream_config.hostname,port_str,&hints,&res);
        if(status == 0) {
            int ip_changed = 0;
            
            pthread_rwlock_rdlock(&upstream_config.dns_lock);
            if(upstream_config.resolved_addr.ss_family != res->ai_family) {
                ip_changed = 1;
            } else {
                if(res->ai_family == AF_INET) {
                    struct sockaddr_in* old_addr = (struct sockaddr_in*)&upstream_config.resolved_addr;
                    struct sockaddr_in* new_addr = (struct sockaddr_in*)res->ai_addr;

                    if(old_addr->sin_addr.s_addr != new_addr->sin_addr.s_addr) {
                        ip_changed = 1;
                    }
                } else if(res->ai_family == AF_INET6) {
                    struct sockaddr_in6* old_addr = (struct sockaddr_in6*)&upstream_config.resolved_addr;
                    struct sockaddr_in6* new_addr = (struct sockaddr_in6*)res->ai_addr;

                    if(memcmp(&old_addr->sin6_addr,&new_addr->sin6_addr,sizeof(struct in6_addr)) != 0) {
                        ip_changed = 1;
                    }
                }
            }
            pthread_rwlock_unlock(&upstream_config.dns_lock);

            if(ip_changed) {
                pthread_rwlock_wrlock(&upstream_config.dns_lock);
                memcpy(&upstream_config.resolved_addr,res->ai_addr,res->ai_addrlen);
                upstream_config.resolved_addr_len = res->ai_addrlen;
                pthread_rwlock_unlock(&upstream_config.dns_lock);

                log_event(LEVEL_WARN,0,"[system]","DNS Shift Detected! Flushing stale connections in the connection pool");

                flush_connection_pool(upstream_config.hostname,upstream_config.port);
            }
    
        } else {
            fprintf(stderr,"Failed to resolve dns of origin server");
        }
    }
    return NULL;
}

void handle_connect_upstream(ConnectionContext* ctx) {
    
    uint64_t current_latency_us = atomic_load(&global_upstream_latency_us);
    int current_errors = atomic_load(&consecutive_upstream_errors);
    time_t now = time(NULL);
    time_t cooldown = atomic_load(&circuit_cooldown_until);

    if(now < cooldown) {
        format_error_response(ctx, 503, "Origin server is experiencing high latency. Please retry later.", "Retry-After: 5\r\n");
        return;
    }
    
    if(current_latency_us > global_config.latency_upstream_threshold || current_errors > global_config.max_consecutive_errors) {
        atomic_store(&circuit_cooldown_until,now + global_config.cooldown_sec);

        char log_buf[256];
        snprintf(log_buf, sizeof(log_buf), "Circuit Breaker: HALF-OPEN Probe. (EMA: %.2f ms | Errors: %d)", current_latency_us / 1000.0, current_errors);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, log_buf);
    }

    strncpy((ctx->req).hostname,upstream_config.hostname,sizeof((ctx->req).hostname) - 1);
    (ctx->req).hostname[sizeof((ctx->req).hostname) - 1] = '\0';
    (ctx->req).port = upstream_config.port;
    strncpy((ctx->req).path, ctx->url, sizeof((ctx->req).path) - 1);
    (ctx->req).path[sizeof((ctx->req).path) - 1] = '\0';

    int warmfd = get_pool_connection((ctx->req).hostname,(ctx->req).port);
    if(warmfd != -1) {
        ctx->upstream_fd = warmfd;
        ctx->is_reused_upstream = 1;
        atomic_store(&context_table[warmfd],ctx);

        struct epoll_event event;
        event.data.fd = warmfd;
        ctx->upstream_interests = EPOLLOUT | EPOLLET;
        event.events = ctx->upstream_interests;

        if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,warmfd,&event) == -1) {
             // If ADD fails because it was already there (Ghost registration), MOD it
             if(errno == EEXIST) {
                 if(epoll_ctl(epoll_fd, EPOLL_CTL_MOD, warmfd, &event) == -1) {
                     perror("epoll_ctl mod upstream (warm pool)");
                     ctx->state = STATE_CLOSE;
                     return;
                 }
             } else {
                 perror("epoll_ctl add upstream (warm pool)");
                 ctx->state = STATE_CLOSE;
                 return;
             }
        }

        ctx->state = STATE_SEND_UPSTREAM;
        return;
    }
    
    pthread_rwlock_rdlock(&upstream_config.dns_lock);
    if(!upstream_config.is_resolved) {
        fprintf(stderr, "Proxy Error: Upstream DNS never resolved at startup!\n");
        format_error_response(ctx, 502, "Bad Gateway (DNS)", NULL);
        pthread_rwlock_unlock(&upstream_config.dns_lock);
        return;
    }
    

    int sockfd,connect_res = -1;
    if((sockfd = socket(upstream_config.resolved_addr.ss_family,SOCK_STREAM,0)) == -1) {
        format_error_response(ctx, 502, "Bad Gateway (Socket)", NULL);
        pthread_rwlock_unlock(&upstream_config.dns_lock);
        return;
    }
    atomic_fetch_add(&active_upstream_connections,1);
    make_socket_non_blocking(sockfd);
   
    connect_res = connect(sockfd,(struct sockaddr*)&upstream_config.resolved_addr,upstream_config.resolved_addr_len);
    
    pthread_rwlock_unlock(&upstream_config.dns_lock);
    if(connect_res < 0 && errno != EINPROGRESS) {
        close(sockfd);
        sockfd = -1;
        fprintf(stderr,"The proxy couldn't connect to host %s\n",(ctx->req).hostname);
        format_error_response(ctx, 502, "Bad Gateway", NULL);
        return;
    }
    
    ctx->upstream_fd = sockfd;
    ctx->is_reused_upstream = 0;
    atomic_store(&context_table[sockfd],ctx);
    
    if(connect_res == 0) {
        ctx->state = STATE_SEND_UPSTREAM;
    } else {
        ctx->state = STATE_WAIT_CONNECT;
    }
    return;
}


void handle_wait_connect(ConnectionContext *ctx) {
    int error = 0;
    socklen_t len = sizeof(error);
    
    if (getsockopt(ctx->upstream_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
        perror("getsockopt failed");
        format_error_response(ctx, 502, "Bad Gateway", NULL);
        return;
    }

    if (error != 0) {
        int current_errors = atomic_fetch_add(&consecutive_upstream_errors,1) + 1;
        char log_buf[128];
        snprintf(log_buf,sizeof(log_buf),"Upstream Connection Failed (%s). Strike: %d / %d",strerror(error),current_errors,global_config.max_consecutive_errors);
        log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
        format_error_response(ctx, 502, "Bad Gateway", NULL);
        return;
    }

    ctx->state = STATE_SEND_UPSTREAM;
}
