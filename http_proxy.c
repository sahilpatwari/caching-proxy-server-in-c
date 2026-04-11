#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<unistd.h>
#include<sys/stat.h>
#include<string.h>
#include<signal.h>
#include <time.h>
#include <stdint.h> 
#include <inttypes.h>
#include <sys/time.h>
#include <netinet/tcp.h>
#include<errno.h>
#include<fcntl.h>
#include<sys/epoll.h>
#include <sys/resource.h> 

#include"cache.h"
#include"rate_limiter.h"
#include"thread_pool.h"

#define BACKLOG SOMAXCONN
#define BUFFER 8192
#define MAX_FDS 65536
#define EPOLL_BATCH_SIZE 512
#define POOL_BUCKETS 1024
_Atomic int active_upstream_connections = 0;

_Atomic uint64_t global_upstream_latency_us = 50000;
_Atomic time_t circuit_cooldown_until = 0;
_Atomic int consecutive_upstream_errors = 0;


extern void abort_cache_download(char* url,void* node_ref);
extern int purge_cache_entry(char* url);
extern void release_cache_ref(void* ref);
// ATOMIC REQUEST ID COUNTER
static _Atomic uint64_t global_req_id = 0;

// SHUTDOWN FLAG
volatile sig_atomic_t server_running = 1;

// SIGNAL HANDLER
void handle_shutdown_signal(int sig) {
    (void)sig; // Suppress unused warning
    server_running = 0; 
}

// TELEMETRY GLOBALS
_Atomic uint64_t metric_cache_hits = 0;
_Atomic uint64_t metric_cache_misses = 0;
_Atomic uint64_t metric_total_requests = 0;
_Atomic uint64_t metric_current_rps = 0;

// System Level: Hardware Metrics
_Atomic uint64_t metric_cpu_usage = 0;
_Atomic long metric_memory_rss_kb = 0;
extern long total_cache_memory;
time_t server_start_time;

static struct upstream_config {
    char hostname[256];
    int port;
    struct sockaddr_storage resolved_addr;
    socklen_t resolved_addr_len;
    int is_resolved;
    pthread_rwlock_t dns_lock;
}upstream_config;

//EPOLL INSTANCE
__thread int epoll_fd;
HostBucket connection_map[POOL_BUCKETS];
void handle_connect_upstream(ConnectionContext* ctx);
static unsigned long hash_host(const char* hostname,int port) {
    unsigned long hash = 5381;
    int c;
    while((c = *hostname++)) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    hash = ((hash << 5) + hash) + port;
    return hash % POOL_BUCKETS;
}

void* telemetry_worker(void* arg) {
    uint64_t last_total_requests = 0;
    double last_cpu_time = 0.0;
    long page_size = sysconf(_SC_PAGESIZE); // Usually 4096 bytes

    while(1) { 
        // 1. HARDWARE-ACCURATE CPU USAGE (Nanosecond Precision)
        struct timespec ts;
        if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) == 0) {
            // Convert seconds and nanoseconds into a single double
            double current_cpu = (double)ts.tv_sec + ((double)ts.tv_nsec / 1000000000.0);
            
            if (last_cpu_time > 0.0) {
                // If it consumed 0.5 seconds of CPU time in the last 1.0 wall-clock seconds, that is 50% CPU.
                // Across 12 cores, max is 1200%
                metric_cpu_usage = (uint64_t)(current_cpu - last_cpu_time) * 10000.0;
            }
            last_cpu_time = current_cpu;
        }

        // 2. BULLETPROOF RAM USAGE (Page Table Math)
        FILE* fp = fopen("/proc/self/statm", "r");
        if (fp) {
            long size, resident;
            // statm format: size resident shared text lib data dt
            if (fscanf(fp, "%ld %ld", &size, &resident) == 2) {
                // resident is in Pages. Multiply by page_size to get bytes, divide by 1024 for KB
                metric_memory_rss_kb = (resident * page_size) / 1024;
            }
            fclose(fp);
        }

        uint64_t current_total = metric_total_requests;
        metric_current_rps = current_total - last_total_requests;
        last_total_requests = current_total;

        sleep(1);
    }

    return NULL;
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

_Atomic(ConnectionContext*) context_table[MAX_FDS];
static ConnectionContext* context_pool_memory;
static ConnectionContext** free_context_stack;
int free_context_stack_top = -1;
static pthread_mutex_t context_pool_lock = PTHREAD_MUTEX_INITIALIZER;

void init_context_pool() {
    context_pool_memory = calloc(global_config.pool_max_connections,sizeof(ConnectionContext));
    free_context_stack = malloc(global_config.pool_max_connections * sizeof(ConnectionContext*));

    if(!context_pool_memory || !free_context_stack) {
        printf("Failed to pre-allocate RAM");
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < global_config.pool_max_connections; i++) {
        free_context_stack[i] = &context_pool_memory[i];
    }
    free_context_stack_top = global_config.pool_max_connections - 1;
}

void destroy_context_pool() {
    if(context_pool_memory) free(context_pool_memory);
    if(free_context_stack)  free(free_context_stack);

    context_pool_memory = NULL;
    free_context_stack = NULL;
    free_context_stack_top = -1;
}

ConnectionContext* create_context(int fd) {
    if(fd >= MAX_FDS) return NULL;
    
    ConnectionContext* ctx = NULL;
    pthread_mutex_lock(&context_pool_lock);
    if(free_context_stack_top >= 0) {
        ctx = free_context_stack[free_context_stack_top--];
    }
    pthread_mutex_unlock(&context_pool_lock);
    
    if(!ctx) {
        ctx = calloc(1,sizeof(ConnectionContext));
        if(!ctx) return NULL;
    }

    ctx->send_mem_buf = NULL;
    ctx->send_mem_len = 0;
    ctx->send_mem_offset = 0;

    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    if(getpeername(fd,(struct sockaddr*)&addr,&addr_size) == 0) {
        if (addr.ss_family == AF_INET) {
            inet_ntop(AF_INET, &((struct sockaddr_in*)&addr)->sin_addr,
                      ctx->client_ip, sizeof(ctx->client_ip));
        } else {
            inet_ntop(AF_INET6, &((struct sockaddr_in6*)&addr)->sin6_addr,
                      ctx->client_ip, sizeof(ctx->client_ip));
        }
    } else {
        strcpy(ctx->client_ip,"UNKNOWN");
    }
    
    ctx->client_fd = fd;
    ctx->upstream_fd = -1;
    ctx->file_fd = -1;
    ctx->active_threads = 0;
    pthread_mutex_init(&ctx->state_lock, NULL);
    atomic_store(&ctx->last_active,time(NULL));
    ctx->req_id = atomic_fetch_add(&global_req_id,1);
    ctx->state = STATE_READ_REQUEST;
    
    ctx->cached_at = 0;
    ctx->bytes_read = 0;
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->is_designated_downloader = 0;
    ctx->bytes_remaining = 0;
    ctx->file_offset = 0;
    ctx->keep_alive = 1;
    ctx->upstream_header_len = 0;
    ctx->upstream_headers_parsed = 0;
    ctx->cache_ttl = 0;
    ctx->checkCache = 0;
    ctx->upstream_content_length = 0;
    ctx->upstream_body_downloaded = 0;
    ctx->cache_ref = NULL;

    ctx->is_chunked = 0;
    ctx->chunk_state = 0;
    ctx->current_chunk_size = 0;
    ctx->current_chunk_bytes_read = 0;
    ctx->hex_idx = 0;
    ctx->header_overshoot_len = 0;
    
    ctx->read_buf[0] = '\0';
    ctx->method[0] = '\0';
    ctx->url[0] = '\0';
    ctx->protocol[0] = '\0';
    ctx->req.hostname[0] = '\0';
    ctx->req.port = 0;
    ctx->req.path[0] = '\0';
    atomic_store(&context_table[fd],ctx);
    return ctx; 
}

void free_context(ConnectionContext* ctx) {
    if (ctx == NULL) return;
     
    if(ctx->is_designated_downloader) {
        abort_cache_download(ctx->url,ctx->cache_ref);
        ctx->is_designated_downloader = 0;
    }
    
    release_cache_ref(ctx->cache_ref);
    ctx->cache_ref = NULL;

    if (ctx->upstream_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ctx->upstream_fd, NULL);
        
        atomic_store(&context_table[ctx->upstream_fd],NULL); 
        atomic_fetch_sub(&active_upstream_connections,1);
        close(ctx->upstream_fd);
        ctx->upstream_fd = -1;
    }
    
    if (ctx->file_fd != -1) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
    }

    if (ctx->client_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ctx->client_fd, NULL);
        atomic_store(&context_table[ctx->client_fd],NULL);
        close(ctx->client_fd);
        ctx->client_fd = -1;
    }
    
    pthread_mutex_lock(&context_pool_lock);
    if(free_context_stack_top < global_config.pool_max_connections - 1) {
        free_context_stack[++free_context_stack_top] = ctx;
        pthread_mutex_unlock(&context_pool_lock);
        return;
    } 
    pthread_mutex_unlock(&context_pool_lock);

    free(ctx);
}

void* connection_reaper_worker(void* arg) {
    while(server_running) {
        sleep(5);
        time_t now = time(NULL);
        for(int i = 0; i < MAX_FDS; i++) {
            ConnectionContext* ctx = atomic_load(&context_table[i]);
            if(ctx != NULL) {
                time_t last_act = atomic_load(&ctx->last_active);
                if(last_act > 0 && (now - last_act) > global_config.idle_timeout_sec) {
                    pthread_mutex_lock(&ctx->state_lock);
                    if(ctx->state != STATE_CLOSE) {
                        if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM || ctx->state == STATE_FETCH_UPSTREAM) {
                            int current_errors = atomic_fetch_add(&consecutive_upstream_errors,1) + 1;
                            char log_buf[128];
                            snprintf(log_buf,sizeof(log_buf),"Reaper. Origin Timed Out. Strike: %d/%d",current_errors,global_config.max_consecutive_errors);
                            log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
                        } else {
                            char log_buf[128];
                            snprintf(log_buf,sizeof(log_buf),"Reaper snipered idle connection on FD %d",ctx->client_fd);
                            log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
                        }

                        
                        ctx->state = STATE_CLOSE;

                        struct epoll_event event;
                        memset(&event,0,sizeof(event));
                        event.data.fd = ctx->client_fd;
                        event.events = EPOLLOUT | EPOLLONESHOT;
                        if(ctx->client_fd != -1) {
                            epoll_ctl(ctx->thread_epoll_fd,EPOLL_CTL_MOD,ctx->client_fd,&event);
                        }
                    }
                    pthread_mutex_unlock(&ctx->state_lock);
                }
            }
        }
    }
    return NULL;
}

int make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return -1;
    }
    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        perror("fcntl F_SETFL O_NONBLOCK");
        return -1;
    }
    return 0;
}

void handle_send_response_headers(ConnectionContext* ctx) {
    while(ctx->write_offset < ctx->write_len) {
        int bytes_sent = send(ctx->client_fd,ctx->write_buf + ctx->write_offset,ctx->write_len - ctx->write_offset,MSG_NOSIGNAL);

        if(bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) return;
            perror("Client send error");
            ctx->state = STATE_CLOSE;
            return;
        } else if (bytes_sent == 0) {
            ctx->state = STATE_CLOSE; // Client hung up early
            return;
        }
        atomic_store(&ctx->last_active,time(NULL));
        ctx->write_offset += bytes_sent;
    }
    ctx->state = STATE_SEND_CACHE;

    handle_send_cache(ctx);
}

void handle_send_upstream(ConnectionContext* ctx) {
    
    if(ctx->write_len == 0) {
        ctx->write_len = snprintf(ctx->write_buf,sizeof(ctx->write_buf),
        "%s %s %s\r\nHost: %s\r\nConnection: keep-alive\r\n"
        ,ctx->method,(ctx->req).path,ctx->protocol,(ctx->req).hostname);

        ctx->write_offset = 0;
        char *buffer_ptr;
        char headers_copy[BUFFER + 1];
        char* end_of_headers = strstr(ctx->read_buf,"\r\n\r\n");
        if(end_of_headers) {
            int header_len = end_of_headers - ctx->read_buf;
            memcpy(headers_copy,ctx->read_buf,header_len);
            headers_copy[header_len] = '\0';
        } else {
            strncpy(headers_copy,ctx->read_buf,BUFFER);
            headers_copy[BUFFER] ='\0';
        }

        char *token = strtok_r(headers_copy,"\r\n",&buffer_ptr);
        while(token != NULL) {
            if(strncmp(token,ctx->method,strlen(ctx->method)) == 0 || strncmp(token,"Host",4) == 0 || strncmp(token,"Connection",10) == 0) {
                token = strtok_r(NULL,"\r\n",&buffer_ptr);
                continue;
            }
            ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,"%s\r\n",token);
            token = strtok_r(NULL,"\r\n",&buffer_ptr);
        }
        ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,"\r\n");
    }

    while(ctx->write_offset < ctx->write_len) {
        int bytes_sent = send(ctx->upstream_fd,ctx->write_buf + ctx->write_offset,ctx->write_len - ctx->write_offset,MSG_NOSIGNAL);

        if(bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            if(errno == EPIPE || errno == ECONNRESET) {

                int current_errors = atomic_fetch_add(&consecutive_upstream_errors, 1) + 1;
                char log_buf[128];
                snprintf(log_buf, sizeof(log_buf), "Origin dropped connection during send(). Strike: %d/%d", current_errors, global_config.max_consecutive_errors);
                log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, log_buf);

                epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->upstream_fd,NULL);
                atomic_store(&context_table[ctx->upstream_fd],NULL);
                close(ctx->upstream_fd);
                ctx->upstream_fd = -1;
                atomic_fetch_sub(&active_upstream_connections, 1);

                ctx->state = STATE_CONNECT_UPSTREAM;
                handle_connect_upstream(ctx);
                return;
            }
            perror("upstream send error");
            ctx->state = STATE_CLOSE;
            return;
        } else if(bytes_sent == 0) {
            ctx->state = STATE_CLOSE;
            return;
        }

        ctx->write_offset += bytes_sent;
    }
    gettimeofday(&ctx->upstream_send_time,NULL);

    ctx->state = STATE_FETCH_UPSTREAM;
    return;
}

void handle_connect_upstream(ConnectionContext* ctx) {
    
    uint64_t current_latency_us = atomic_load(&global_upstream_latency_us);
    int current_errors = atomic_load(&consecutive_upstream_errors);
    time_t now = time(NULL);
    time_t cooldown = atomic_load(&circuit_cooldown_until);

    if(now < cooldown) {
        ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
            "HTTP/1.1 503 Service Unavailable\r\n"
            "Content-Type: text/plain\r\n"
            "Retry-After: 5\r\n"
            "Connection: close\r\n"
            "Content-Length: 64\r\n"
            "\r\n"
            "Origin server is experiencing high latency. Please retry later.");
            
        ctx->write_offset = 0;
        ctx->keep_alive = 0;
        ctx->state = STATE_SEND_RESPONSE_HEADERS;
        return;
    }
    
    if(current_latency_us > global_config.latency_upstream_threshold || current_errors > global_config.max_consecutive_errors) {
        atomic_store(&circuit_cooldown_until,now + global_config.cooldown_sec);

        char log_buf[256];
        snprintf(log_buf, sizeof(log_buf), "Circuit Breaker: HALF-OPEN Probe. (EMA: %.2f ms | Errors: %d)", current_latency_us / 1000.0, current_errors);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, log_buf);
    }

    strncpy((ctx->req).hostname,upstream_config.hostname,sizeof((ctx->req).hostname) - 1);
    (ctx->req).port = upstream_config.port;
    strncpy((ctx->req).path, ctx->url, sizeof((ctx->req).path) - 1);
    (ctx->req).path[sizeof((ctx->req).path) - 1] = '\0';

    int warmfd = get_pool_connection((ctx->req).hostname,(ctx->req).port);
    if(warmfd != -1) {
        ctx->upstream_fd = warmfd;
        atomic_store(&context_table[warmfd],ctx);

        struct epoll_event event;
        event.data.fd = warmfd;
        event.events = EPOLLOUT | EPOLLONESHOT;

        if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,warmfd,&event) == -1) {
             perror("epoll_ctl add upstream");
             ctx->state = STATE_CLOSE;
             return;
        }

        ctx->state = STATE_SEND_UPSTREAM;
        handle_send_upstream(ctx);
        return;
    }
    
    pthread_rwlock_rdlock(&upstream_config.dns_lock);
    if(!upstream_config.is_resolved) {
        fprintf(stderr, "Proxy Error: Upstream DNS never resolved at startup!\n");
        send_error_response(ctx->client_fd, 502, "Bad Gateway (DNS)", NULL);
        ctx->state = STATE_CLOSE;
        return;
    }
    

    int sockfd,connect_res = -1;
    if((sockfd = socket(upstream_config.resolved_addr.ss_family,SOCK_STREAM,0)) == -1) {
        send_error_response(ctx->client_fd, 502, "Bad Gateway (Socket)", NULL);
        ctx->state = STATE_CLOSE;
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
        send_error_response(ctx->client_fd, 502, "Bad Gateway", NULL);
        ctx->state = STATE_CLOSE;
        return;
    }
    
    ctx->upstream_fd = sockfd;
    atomic_store(&context_table[sockfd],ctx);
    
    struct epoll_event event;
    memset(&event,0,sizeof(event));
    event.data.fd = sockfd;
    event.events = EPOLLOUT | EPOLLONESHOT;
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sockfd, &event) == -1) {
        perror("epoll_ctl add upstream");
        ctx->state = STATE_CLOSE;
        return;
    }

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
        ctx->state = STATE_CLOSE;
        return;
    }

    if (error != 0) {
        int current_errors = atomic_fetch_add(&consecutive_upstream_errors,1) + 1;
        char log_buf[128];
        snprintf(log_buf,sizeof(log_buf),"Upstream Connection Failed (%s). Strike: %d / %d",strerror(error),current_errors,global_config.max_consecutive_errors);
        log_event(LEVEL_WARN,ctx->req_id,ctx->client_ip,log_buf);
        send_error_response(ctx->client_fd, 502, "Bad Gateway", NULL);
        ctx->state = STATE_CLOSE;
        return;
    }

    ctx->state = STATE_SEND_UPSTREAM;
    handle_send_upstream(ctx); 
}

void handle_parse_request(ConnectionContext* ctx) {
    //RATE LIMIT
    /*if(!check_rate_limit(ctx->client_ip)) {
        printf("[Rate Limit] Denied: %s\n", ctx->client_ip);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Rate Limit Exceeded (429)");
        send_error_response(ctx->client_fd, 429, "Too Many Requests. Slow down.",NULL);
        ctx->state = STATE_CLOSE;
        return;
    }*/

    ctx->read_buf[ctx->bytes_read] = '\0';
    int count = sscanf(ctx->read_buf,"%15s %511s %15s",ctx->method,ctx->url,ctx->protocol);
    if(count != 3) {
        send_error_response(ctx->client_fd, 400, "Invalid HTTP Request Format.",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Malformed Request");
        ctx->state = STATE_CLOSE;
        return;
    }
    
    if(strstr(ctx->protocol,"HTTP/1.0") != NULL && strstr(ctx->read_buf,"Connection: Keep-Alive") == NULL) {
        ctx->keep_alive = 0;
    }
    else if(strstr(ctx->read_buf,"Connection:close") || strstr(ctx->read_buf,"Connection: close")) {
        ctx->keep_alive = 0;
    } else {
        ctx->keep_alive = 1;
    }

    //printf("[Proxy] Request: %s %s\n", method, url);
    char log_buf[1536];
    snprintf(log_buf, sizeof(log_buf), "Request: %s %s:%d", ctx->method, (ctx->req).hostname, (ctx->req).port);
    log_event(LEVEL_INFO, ctx->req_id, ctx->client_ip, log_buf);
    
    atomic_fetch_add(&metric_total_requests,1);
    
    if(strcmp(ctx->method,"PURGE") == 0) {
        int purge_status = purge_cache_entry(ctx->url);
        
        if (purge_status == 200) {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"status\": \"purged\", \"path\": \"%s\"}", ctx->url);
        } 
        else if (purge_status == 409) {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"error\": \"File is actively being fetched. Try again.\"}");
        } 
        else {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"error\": \"Not in cache\", \"path\": \"%s\"}", ctx->url);
        }

        ctx->bytes_remaining = 0;
        ctx->file_fd = -1;
        ctx->keep_alive = 0;
        ctx->state = STATE_SEND_RESPONSE_HEADERS;
        return;
    }

    if(strncmp(ctx->method,"GET",3) == 0) {
        if(strcmp(ctx->url,"/stats") == 0) {
            uint64_t hits = metric_cache_hits;
            uint64_t misses = metric_cache_misses;
            uint64_t total = hits + misses;
            double hit_ratio = total > 0 ? ((double)hits / total)* 100.0 : 0.0;
            long uptime = time(NULL) - server_start_time;
            uint64_t cpu = atomic_load(&metric_cpu_usage);

            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                "Connection: close\r\n\r\n"
                "{\n"
                "  \"server_uptime_seconds\": %ld,\n"
                "  \"traffic\": {\n"
                "    \"current_rps\": %lu,\n"
                "    \"total_requests\": %lu\n"
                "  },\n"
                "  \"cache\": {\n"
                "    \"hit_ratio_percent\": %.2f,\n"
                "    \"total_hits\": %lu,\n"
                "    \"total_misses\": %lu,\n"
                "    \"tracked_ram_bytes\": %ld\n"
                "  },\n"
                "  \"hardware\": {\n"
                "    \"cpu_usage_percent\": %.2f,\n"
                "    \"memory_rss_kb\": %ld\n"
                "  }\n"
                "}", 
                uptime, metric_current_rps, metric_total_requests, 
                hit_ratio, hits, misses, total_cache_memory,
                cpu / 100.0, metric_memory_rss_kb);
                
                ctx->bytes_remaining = 0;
                ctx->file_fd = -1;
                ctx->keep_alive = 0;
                ctx->state = STATE_SEND_RESPONSE_HEADERS;
                return;
        } 
        ctx->state = STATE_CHECK_CACHE;
        ctx->checkCache = 1;
        handle_check_cache(ctx);
    } else {
        send_error_response(ctx->client_fd,501,"Not Implemented",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Unsupported Method");
    }
    return;
}

void handle_read_request(ConnectionContext* ctx) {
    while(1) {
        int n = recv(ctx->client_fd,ctx->read_buf + ctx->bytes_read,BUFFER - 1 - ctx->bytes_read,MSG_NOSIGNAL);

        if(n < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            if (errno != ECONNRESET) perror("recv error");
            ctx->state = STATE_CLOSE;
            return;
        } else if(n == 0) {
            //Client disconnected prematurely
            ctx->state = STATE_CLOSE;
            return;
        }
        
        atomic_store(&ctx->last_active,time(NULL));
        ctx->bytes_read += n;
        ctx->read_buf[ctx->bytes_read] = '\0';

        if(strstr(ctx->read_buf,"\r\n\r\n") != NULL) {
            ctx->state = STATE_PARSE_REQUEST;
            handle_parse_request(ctx);
            return;
        }
    }
}
void* handle_state_machine(void* args) {
    ConnectionContext* ctx = (ConnectionContext*)args;

    if(ctx == NULL) return NULL;


    pthread_mutex_lock(&ctx->state_lock);
    if(ctx->state != STATE_CLOSE) {
        switch(ctx->state) {
            case STATE_READ_REQUEST:
                handle_read_request(ctx);
                break;
            case STATE_PARSE_REQUEST:
                handle_parse_request(ctx);
                break;
            case STATE_CHECK_CACHE:
                handle_check_cache(ctx);
                break;
            case STATE_SEND_CACHE:
                handle_send_cache(ctx);
                break;
            case STATE_CONNECT_UPSTREAM:
                handle_connect_upstream(ctx);
                break;
            case STATE_WAIT_CONNECT:
                handle_wait_connect(ctx);
                break;
            case STATE_SEND_UPSTREAM:
                handle_send_upstream(ctx);
                break;
            case STATE_FETCH_UPSTREAM:
                handle_fetch_upstream(ctx);
                break;
            case STATE_SEND_RESPONSE_HEADERS:
                handle_send_response_headers(ctx);
                break;
            /*case STATE_TUNNELING:
                handle_tunnel_request(ctx);
                break;*/ 
        }
    }
    
    if(ctx->state != STATE_CLOSE && ctx->state != STATE_WAIT_CACHE) {
        struct epoll_event event;
        memset(&event,0,sizeof(event));
        if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM || ctx->state == STATE_FETCH_UPSTREAM) {
            event.data.fd = ctx->upstream_fd;
            if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM) {
                event.events = EPOLLOUT | EPOLLONESHOT;
            } else {
                event.events = EPOLLIN | EPOLLONESHOT;
            }
            
            if(ctx->upstream_fd != -1) {
                if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ctx->upstream_fd, &event) == -1) {
                    perror("epoll_ctl mod upstream failed");
                    ctx->state = STATE_CLOSE; 
                }
            }
        } else if(ctx->state == STATE_READ_REQUEST ||  ctx->state == STATE_SEND_RESPONSE_HEADERS || ctx->state == STATE_SEND_CACHE) {
            event.data.fd = ctx->client_fd;
            if(ctx->state == STATE_SEND_CACHE || ctx->state == STATE_SEND_RESPONSE_HEADERS) {
                event.events = EPOLLOUT | EPOLLONESHOT;
            } else {
                event.events = EPOLLIN | EPOLLONESHOT;
            }

            if(ctx->client_fd != -1) {
                if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ctx->client_fd, &event) == -1) {
                    perror("epoll_ctl mod client failed");
                    ctx->state = STATE_CLOSE; 
                }
            }
        }
    }
    
    if(ctx->state == STATE_CLOSE) {
        if(ctx->client_fd != -1) {
            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->client_fd,NULL);
        }

        if(ctx->upstream_fd != -1) {
            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->upstream_fd,NULL);
        }
    }

    int is_closed = (ctx->state == STATE_CLOSE) ? 1 : 0;
    pthread_mutex_unlock(&ctx->state_lock);

    int remaining_threads = atomic_fetch_sub(&ctx->active_threads, 1) - 1;

    if(is_closed && remaining_threads == 0) {
        pthread_mutex_destroy(&ctx->state_lock);
        free_context(ctx);
    }
    return NULL;
}

/*void handle_tunnel_request(int client_fd,struct ProxyRequest *req,char* initial_buffer,int initial_len,char* method,char* url,char* protocol,char* client_ip,uint64_t req_id) {
    
    char log_buf[1536];
    snprintf(log_buf, sizeof(log_buf), "Tunnel Start: %s:%d", req->hostname, req->port);
    log_event(LEVEL_INFO, req_id, client_ip, log_buf);

    long content_length = 0;
    char* cl_ptr = strstr(initial_buffer,"Content-Length:");
    if(cl_ptr) {
        content_length = strtol(cl_ptr + 15,NULL,10);
        snprintf(log_buf, sizeof(log_buf), "POST Body: %ld bytes", content_length);
        log_event(LEVEL_DEBUG, req_id, client_ip, log_buf);
    } else {
        send_error_response(client_fd,411, "Content-Length Required",NULL);
        log_event(LEVEL_WARN, req_id, client_ip,"Content-Length Required");
        return;
    }

    int serverfd = connect_to_host(req->hostname,req->port);
    if(serverfd == -1) {
        send_error_response(client_fd, 502, "Bad Gateway: Could not connect to remote server",NULL);
        snprintf(log_buf, sizeof(log_buf), "Upstream connection failed: %s", req->hostname);
        log_event(LEVEL_ERROR, req_id, client_ip, log_buf);
        return;
    }
    log_event(LEVEL_DEBUG, req_id, client_ip, "Upstream connected");

    char* body_start = strstr(initial_buffer,"\r\n\r\n");
    int header_end_idx;
    if(body_start) {
        body_start += 4;
        header_end_idx = body_start - initial_buffer;
    } else {
        header_end_idx = initial_len;
        body_start = initial_buffer + initial_len;
    }
    char header_buffer[BUFFER];
    char* headers_copy = malloc(header_end_idx + 1);
    if(!headers_copy) {
        perror("malloc");
        send_error_response(client_fd,500,"Internal Server Error",NULL);
        log_event(LEVEL_ERROR, req_id, client_ip, "Malloc failed for headers");
        close(serverfd);
        return;
    }
    snprintf(header_buffer,sizeof(header_buffer),
    "%s %s %s\r\nHost: %s\r\nConnection: close\r\n",method,req->path,protocol,req->hostname);

    memcpy(headers_copy,initial_buffer,header_end_idx);
    headers_copy[header_end_idx] = '\0';
    char* save_ptr;
    char* token = strtok_r(headers_copy,"\r\n",&save_ptr);
    while(token != NULL) {
        if(strncmp(token,method,strlen(method)) == 0 || strncmp(token,"Host",4) == 0 || strncmp(token,"Connection",10) == 0) {
            token = strtok_r(NULL,"\r\n",&save_ptr);
            continue;
        }
        snprintf(header_buffer + strlen(header_buffer),sizeof(header_buffer),"%s\r\n",token);
        token = strtok_r(NULL,"\r\n",&save_ptr);
    }
    snprintf(header_buffer + strlen(header_buffer),sizeof(header_buffer),"\r\n");
    free(headers_copy);
    //Send Headers first
    int header_len = strlen(header_buffer);
    while(header_len > 0) {
        int n;
        if((n = send(serverfd,header_buffer,strlen(header_buffer),MSG_NOSIGNAL)) == -1) {
            send_error_response(client_fd, 502, "Bad Gateway",NULL);
            log_event(LEVEL_ERROR, req_id, client_ip, "Failed to send headers to upstream");
            close(serverfd);
            return;
        }
        header_len -= n;
    }
    log_event(LEVEL_DEBUG,req_id,client_ip, "Headers forwarded to upstream");

    //Send remaining body in buffer
    int body_in_buffer = initial_len - (body_start - initial_buffer);
    int body_len = 0;
    while(body_len < body_in_buffer) {
        int n;
        if((n =send(serverfd,body_start + body_len,body_in_buffer - body_len,MSG_NOSIGNAL)) == -1) {
            perror("Failed to send remaining body");
            log_event(LEVEL_WARN, req_id, client_ip, "Upstream closed during initial body send");
            close(serverfd);
            return;
        }
        content_length -= n;
        body_len += n;
    }
    char buffer[BUFFER];
    while(content_length > 0) {
        int bytes,len = 0;
        if((bytes = recv(client_fd,buffer,BUFFER - 1,MSG_NOSIGNAL)) == -1) {
            log_event(LEVEL_WARN,req_id,client_ip,"Client disconnected during body upload");
            close(serverfd);
            return;
        }
        buffer[bytes] ='\0';
        while(len < bytes) {
            int n = 0;
            if((n = send(serverfd,buffer + len,bytes - len,MSG_NOSIGNAL)) == -1) {
                perror("Failed to send body");
                log_event(LEVEL_WARN, req_id, client_ip, "Upstream closed during initial body send");
                close(serverfd);
                return;
            }
            len += n;
        }
        content_length -= len;
    }
    long total_bytes = 0,recv_bytes,recv_len = 0;
    char recv_buffer[BUFFER];
    while((recv_bytes = recv(serverfd,recv_buffer,BUFFER - 1,0)) > 0) {
        while(recv_len < recv_bytes) {
            int n;
            if((n = send(client_fd,recv_buffer + recv_len,recv_bytes - recv_len,0)) == -1) {
                perror("Failed to send to client");
                log_event(LEVEL_WARN, req_id, client_ip, "Client disconnected during responding");
                close(serverfd);
                return;
            }
            recv_len += n;
            total_bytes += n;
        }
    }
    if(recv_bytes == -1) {
        perror("Failed to receive data from upstream");
        log_event(LEVEL_WARN, req_id, client_ip, "Upstream closed during response download");
        close(serverfd);
        return;
    }
    snprintf(log_buf, sizeof(log_buf), "Tunnel Closed. Relayed: %ld bytes", total_bytes);
    log_event(LEVEL_INFO, req_id, client_ip, log_buf);
    printf("Total Bytes relayed: %ld bytes\n",total_bytes);
    close(serverfd);
}*/

int resolve_origin_dns(const char* hostname,const char* port_str) {
    strncpy(upstream_config.hostname,hostname,sizeof(upstream_config.hostname) - 1);

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

void* worker_reactor_loop(void* args) {
    int status, sockfd = -1,yes = 1;
    struct addrinfo hints,*res,*p;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", global_config.port);

    if((status = getaddrinfo(NULL,port_str,&hints,&res)) == -1) {
        fprintf(stderr,"worker: getaddrinfo: %s\n",gai_strerror(status));
        return NULL;
    }

    for(p = res;p !=  NULL; p = p->ai_next) {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1) {
            perror("worker: socket allocation thread failed");
            continue;
        }
        
        if(setsockopt(sockfd,SOL_SOCKET, SO_REUSEADDR,&yes,sizeof(int)) == -1) {
            perror("setsocketopt");
            exit(1);
        }

        if(setsockopt(sockfd,SOL_SOCKET, SO_REUSEPORT,&yes,sizeof(int)) == -1) {
            perror("setsocketopt");
            exit(1);
        }
        
        if(bind(sockfd,p->ai_addr,p->ai_addrlen) == -1) {
            perror("worker: bind failed");
            close(sockfd);
            continue;
        }

        break;
    }
    
    freeaddrinfo(res);

    if(p == NULL) {
        fprintf(stderr,"worker: Couldn't bind a to a port");
    }

    if(listen(sockfd,BACKLOG) == -1) {
        perror("worker: listen failed");
        exit(1);
    }

    make_socket_non_blocking(sockfd); //make the main listener non-blocking

    epoll_fd = epoll_create1(0);
    if(epoll_fd == -1) {
        perror("worker: epoll instance creation failed");
        exit(1);
    }

    struct epoll_event event;
    memset(&event,0,sizeof(event));
    struct epoll_event events[EPOLL_BATCH_SIZE];

    event.data.fd = sockfd;
    event.events = EPOLLIN;
    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,sockfd,&event) == -1) {
        perror("worker: epoll ctl: sockfd failed");
        exit(1);
    }
    
    while(server_running) {

        int n_ready = epoll_wait(epoll_fd,events,EPOLL_BATCH_SIZE,1000);
        if(n_ready == -1) {
            if(errno == EINTR) {
                continue;
            }

            perror("worker: epoll_wait failed");
            break;
        }

        for(int i = 0; i < n_ready; i++) {
            int currentfd = events[i].data.fd;
            if(currentfd == sockfd) {
                while(1) {
                    struct sockaddr_storage their_addr;
                    socklen_t sin_size;
                    sin_size = sizeof their_addr;

                    int newfd = accept(sockfd,(struct sockaddr*)&their_addr,&sin_size);
                    if(newfd == -1) {
                        if(errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; //We have accepted all incoming connections
                        }
                        perror("worker: accept");
                        break;
                    }

                    make_socket_non_blocking(newfd);
                    int flag = 1;
                    if(setsockopt(newfd,IPPROTO_TCP,TCP_NODELAY,&flag,sizeof(flag)) == -1) {
                        perror("worker: setsockopt TCP_NODELAY Failed(Non-Fatal)");
                    }

                    ConnectionContext* ctx = create_context(newfd);
                    if(ctx == NULL) {
                        fprintf(stderr,"worker: create_context failed");
                        close(newfd);
                        continue;
                    }

                    ctx->thread_epoll_fd = epoll_fd;
                    event.data.fd = newfd;
                    event.events = EPOLLIN | EPOLLONESHOT;
                    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,newfd,&event) == -1) {
                        perror("worker: epoll ctl failed adding client descriptor");
                        ctx->state = STATE_CLOSE;
                        free_context(ctx);
                        close(newfd);
                        continue;
                    }
                }
            } else {
                ConnectionContext* ctx = atomic_load(&context_table[currentfd]);
                if(ctx != NULL) {
                    atomic_store(&ctx->last_active,time(NULL));
                    atomic_fetch_add(&ctx->active_threads,1);
                    handle_state_machine(ctx);
                } else {
                    fprintf(stderr,"Ghost file descriptor event triggered on fd %d\n",currentfd);
                }
            }
        }
    }
    close(epoll_fd);
    close(sockfd);
    return NULL;  
}

int main(int argc,char* argv[]) {
    
    char hostname[256] = "127.0.0.1";
    char port_str[16]  = "8080";

    if(argc == 1) {
        fprintf(stderr,"use --help for usage details");
        return 0;
    }

    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i],"--help") == 0) {
            fprintf(stderr,"Usage Details: ./http_proxy.exe --port <port> --origin <hostname>\n");
            fprintf(stderr,"Example: ./http_proxy.exe --port 8080 --origin 127.0.0.1");
            return 0;
        } else{
            if(argc < 5) {
                fprintf(stderr,"Wrong Usage. use --help for usage details");
                return 0;
            }
            if(strcmp(argv[i],"--port") == 0 && i + 1 < argc) {
                strncpy(port_str,argv[++i],sizeof(port_str) - 1);
                port_str[sizeof(port_str) - 1] = '\0';

            } else if(strcmp(argv[i],"--origin") == 0 && i + 1 < argc) {
                strncpy(hostname,argv[++i],sizeof(hostname) - 1);
                hostname[sizeof(hostname) - 1] = '\0';

            }
        }
    }

    if(resolve_origin_dns(hostname,port_str) == -1) {
        return 0;
    }

    signal(SIGPIPE, SIG_IGN); 

    struct sigaction sa;
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);  // Catch Ctrl+C
    sigaction(SIGTERM, &sa, NULL); // Catch kill commands

    mkdir("cache", 0777);
    
    // Load config before initializing systems
    load_config("proxy.conf");
    
    init_log(global_config.log_level);
    init_rate_limiter();
    init_connection_pool();
    init_cache();
    init_context_pool();
    init_registry();
    rehydrate_cache();

    printf("Server is listening\n");
    global_req_id = (uint64_t)time(NULL) << 16;
    printf("Proxy Server started. Initial Req ID: %" PRIu64 "\n", global_req_id);
    
    server_start_time = time(NULL);

    int NUM_REACTORS = 8;
    pthread_t reactors[NUM_REACTORS];
    for(int i = 0; i < NUM_REACTORS; i++) {
        if(pthread_create(&reactors[i],NULL,worker_reactor_loop,NULL) != 0) {
            perror("Failed to start worker reactor thread");
        }
    }

    pthread_t telemetry_thread;
    if (pthread_create(&telemetry_thread, NULL, telemetry_worker, NULL) != 0) {
        perror("Failed to start telemetry daemon");
    }
    pthread_detach(telemetry_thread);
    printf("[System] Telemetry daemon started.\n");

    pthread_t reaper_thread;
    if(pthread_create(&reaper_thread,NULL,connection_reaper_worker,NULL) != 0) {
        perror("Failed to start the reaper daemon");
    }
    pthread_detach(reaper_thread);
    printf("[System] Idle Reaper daemon started.\n");
    
    pthread_rwlock_init(&upstream_config.dns_lock,NULL);
    pthread_t dns_thread;
    if(pthread_create(&dns_thread,NULL,dns_refresh_worker,NULL) != 0) {
        perror("Failed to start the dns thread");
    }
    pthread_detach(dns_thread);
    printf("[System] DNS Thread started.\n");
    for(int i = 0; i < NUM_REACTORS; i++) {
        pthread_join(reactors[i],NULL);
    }

    printf("\nInitiating graceful shutdown sequence...\n");
    //destroy_rate_limiter(); 
    destroy_cache();
    destroy_context_pool();
    close_log(); 

    printf("Proxy shut down successfully. All resources freed.\n");
    return 0;
}