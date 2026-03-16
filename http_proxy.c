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


#include"cache.h"
#include"rate_limiter.h"
#include"thread_pool.h"

#define PORT "3490"
#define BACKLOG SOMAXCONN
#define BUFFER 8192
#define MAX_FDS 10000 
#define POOL_BUCKETS 1024

// ATOMIC REQUEST ID COUNTER
static _Atomic uint64_t global_req_id = 0;

// SHUTDOWN FLAG
volatile sig_atomic_t server_running = 1;

// SIGNAL HANDLER
void handle_shutdown_signal(int sig) {
    (void)sig; // Suppress unused warning
    server_running = 0; 
}

//EPOLL INSTANCE
int epoll_fd;
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

ConnectionContext* context_table[MAX_FDS];
ConnectionContext* create_context(int fd) {
    if(fd >= MAX_FDS) return NULL;
    ConnectionContext* ctx = calloc(1,sizeof(ConnectionContext));
    if(!ctx) return NULL;
    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    if(getpeername(fd,(struct sockaddr*)&addr,&addr_size) == 0) {
          if(getnameinfo((struct sockaddr*)&addr,addr_size,ctx->client_ip,sizeof(ctx->client_ip),NULL,0,NI_NUMERICHOST) == -1) {
            strcpy(ctx->client_ip,"UNKNOWN");
        }
    } else {
        strcpy(ctx->client_ip,"UNKNOWN");
    }
    
    ctx->client_fd = fd;
    ctx->upstream_fd = -1;
    ctx->file_fd = -1;
    // GENERATE ID
    ctx->active_threads = 0;
    pthread_mutex_init(&ctx->state_lock, NULL);
    ctx->req_id = global_req_id++;
    ctx->state = STATE_READ_REQUEST;
    ctx->bytes_read = 0;
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->bytes_remaining = 0;
    ctx->keep_alive = 1;
    context_table[fd] = ctx;
    return ctx; 
}

void free_context(ConnectionContext* ctx) {
     if (ctx == NULL) return;

    if (ctx->upstream_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ctx->upstream_fd, NULL);
        
        context_table[ctx->upstream_fd] = NULL; 
        
        close(ctx->upstream_fd);
        ctx->upstream_fd = -1;
    }

    if (ctx->client_fd != -1) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ctx->client_fd, NULL);
        context_table[ctx->client_fd] = NULL;
        close(ctx->client_fd);
        ctx->client_fd = -1;
    }

    if (ctx->file_fd != -1) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
    }
    free(ctx);
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
        char* headers_copy = malloc(BUFFER + 1);
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
        free(headers_copy);
    }

    while(ctx->write_offset < ctx->write_len) {
        int bytes_sent = send(ctx->upstream_fd,ctx->write_buf + ctx->write_offset,ctx->write_len - ctx->write_offset,MSG_NOSIGNAL);

        if(bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            if(errno == EPIPE || errno == ECONNRESET) {
                epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->upstream_fd,NULL);
                context_table[ctx->upstream_fd] = NULL;
                close(ctx->upstream_fd);
                ctx->upstream_fd = -1;

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
    ctx->state = STATE_FETCH_UPSTREAM;
    return;
}

void handle_connect_upstream(ConnectionContext* ctx) {

    int warmfd = get_pool_connection((ctx->req).hostname,(ctx->req).port);
    if(warmfd != -1) {
        ctx->upstream_fd = warmfd;
        context_table[warmfd] = ctx;

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

    struct addrinfo hints, *res, *p;
    int sockfd,status,connect_res = -1;

    char port_str[16];
    snprintf(port_str,sizeof(port_str),"%d",(ctx->req).port);
    
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if((status = getaddrinfo((ctx->req).hostname,port_str,&hints,&res)) == -1) {
        fprintf(stderr,"getaddrinfo: %s\n",gai_strerror(status));
        send_error_response(ctx->client_fd, 502, "Bad Gateway", NULL);
        ctx->state = STATE_CLOSE;
        return;
    }

    for(p = res;p != NULL;p = p->ai_next) {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1) {
            continue;
        }
        
        make_socket_non_blocking(sockfd);

        connect_res = connect(sockfd,p->ai_addr,p->ai_addrlen);

        if(connect_res == 0) {
            break;
        } else if(connect_res < 0 && errno == EINPROGRESS){
            break;
        }

        close(sockfd);
        sockfd = -1;
    }
    freeaddrinfo(res);
    if(p == NULL) {
        fprintf(stderr,"The proxy couldn't connect to host %s\n",(ctx->req).hostname);
        send_error_response(ctx->client_fd, 502, "Bad Gateway", NULL);
        ctx->state = STATE_CLOSE;
        return;
    }
    
    ctx->upstream_fd = sockfd;
    context_table[sockfd] = ctx;
    
    struct epoll_event event;
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
        fprintf(stderr, "Upstream connection failed: %s\n", strerror(error));
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

    if(strstr(ctx->read_buf,"Connection:close") || strstr(ctx->read_buf,"Connection: close")) {
        ctx->keep_alive = 0;
    } else {
        ctx->keep_alive = 1;
    }
    
    ctx->read_buf[ctx->bytes_read] = '\0';
    int count = sscanf(ctx->read_buf,"%15s %5119s %15s",ctx->method,ctx->url,ctx->protocol);
    if(count != 3) {
        send_error_response(ctx->client_fd, 400, "Invalid HTTP Request Format.",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Malformed Request");
        ctx->state = STATE_CLOSE;
        return;
    }

    if(parse_url(ctx->url,&(ctx->req)) != 0) {
        send_error_response(ctx->client_fd, 400, "Invalid URL Format.",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Invalid URL");
        ctx->state = STATE_CLOSE;
        return;
    }
    //printf("[Proxy] Request: %s %s\n", method, url);
    char log_buf[1536];
    snprintf(log_buf, sizeof(log_buf), "Request: %s %s:%d", ctx->method, (ctx->req).hostname, (ctx->req).port);
    log_event(LEVEL_INFO, ctx->req_id, ctx->client_ip, log_buf);

    if(strncmp(ctx->method,"GET",3) == 0) {
        ctx->state = STATE_CHECK_CACHE;
        ctx->checkCache = 1;
        handle_check_cache(ctx);
    } else if(strncmp(ctx->method,"POST",4) == 0) {
        /*ctx->state = STATE_TUNNELING;
        handle_tunnel_request(ctx);*/
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

    atomic_fetch_add(&ctx->active_threads, 1);

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
    
    if(ctx->state != STATE_CLOSE) {
        struct epoll_event event;
        if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM || ctx->state == STATE_FETCH_UPSTREAM) {
            event.data.fd = ctx->upstream_fd;
            if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM) {
                event.events = EPOLLOUT | EPOLLONESHOT;
            } else {
                event.events = EPOLLIN | EPOLLONESHOT;
            }
            
            if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ctx->upstream_fd, &event) == -1) {
                perror("epoll_ctl mod upstream failed");
                ctx->state = STATE_CLOSE; 
                free_context(ctx);
            }
        } else if(ctx->state == STATE_READ_REQUEST ||  ctx->state == STATE_SEND_RESPONSE_HEADERS || ctx->state == STATE_SEND_CACHE) {
            event.data.fd = ctx->client_fd;
            if(ctx->state == STATE_SEND_CACHE || ctx->state == STATE_SEND_RESPONSE_HEADERS) {
                event.events = EPOLLOUT | EPOLLONESHOT;
            } else {
                event.events = EPOLLIN | EPOLLONESHOT;
            }
            if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, ctx->client_fd, &event) == -1) {
                perror("epoll_ctl mod client failed");
                ctx->state = STATE_CLOSE; 
                free_context(ctx);
            }
        }
    }

    pthread_mutex_unlock(&ctx->state_lock);

    int remaining_threads = atomic_fetch_sub(&ctx->active_threads, 1) - 1;

    if(ctx->state == STATE_CLOSE && remaining_threads == 0) {
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

int main() {
    struct sockaddr_storage their_addr;
    struct addrinfo hints, *res, *p;
    socklen_t sin_size;
    int status, sockfd,newfd,yes = 1;
    
    signal(SIGPIPE, SIG_IGN); 

    struct sigaction sa;
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);  // Catch Ctrl+C
    sigaction(SIGTERM, &sa, NULL); // Catch kill commands

    mkdir("cache", 0777);
    init_log(LEVEL_ERROR);
    init_rate_limiter();
    init_connection_pool();
    memset(&hints,0,sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if((status = getaddrinfo(NULL,PORT,&hints,&res)) == -1) {
        fprintf(stderr,"getaddrinfo: %s\n",gai_strerror(status));
        return 1;
    }

    for(p = res;p != NULL;p = p->ai_next) {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1) {
              perror("socket");
              continue;
        }

         if(setsockopt(sockfd,SOL_SOCKET, SO_REUSEADDR,&yes,sizeof(int)) == -1) {
            perror("setsocketopt");
            exit(1);
        }

        if(bind(sockfd,p->ai_addr,p->ai_addrlen) == -1) {
            perror("bind");
            continue;
        }
        break;
    }
    freeaddrinfo(res);
    if(p == NULL) {
        fprintf(stderr,"Server couldn't bind to a specific port");
        exit(1);
    }
   
    if(listen(sockfd,BACKLOG) == -1) {
        perror("listen");
        exit(1);
    }
    printf("Server is listening\n");
    global_req_id = (uint64_t)time(NULL) << 16;
    printf("Proxy Server started. Initial Req ID: %" PRIu64 "\n", global_req_id);
    make_socket_non_blocking(sockfd); // Make the main listener non-blocking
    
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1 failed");
        exit(1);
    }

    struct epoll_event event;
    struct epoll_event events[MAX_FDS];

    event.data.fd = sockfd;
    event.events = EPOLLIN;
    
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sockfd, &event) == -1) {
        perror("epoll_ctl: sockfd");
        exit(1);
    }

    init_thread_pool(8,4096);
    while(server_running) {

        int n_ready = epoll_wait(epoll_fd,events,MAX_FDS,-1);

        if(n_ready == -1) {
           if (errno == EINTR) {
                continue; 
            }
            perror("epoll_wait");
            break;
        }

        for(int i = 0;i < n_ready;i++) {
            int currentfd = events[i].data.fd;
            if(currentfd == sockfd) {
                while(1) {
                    sin_size = sizeof their_addr;
                    newfd = accept(sockfd,(struct sockaddr*)&their_addr,&sin_size);
                    if(newfd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; // We accepted all incoming connections
                        }
                        perror("accept");
                        continue;
                    }
                    make_socket_non_blocking(newfd);

                    ConnectionContext* ctx = create_context(newfd);
                    event.data.fd = newfd;
                    event.events = EPOLLIN | EPOLLONESHOT;
                    epoll_ctl(epoll_fd,EPOLL_CTL_ADD,newfd,&event);
                }
            } else {
                ConnectionContext* ctx = context_table[currentfd];
                submit_task(handle_state_machine,(void*)ctx);
            }
        }   
    } 

    printf("\nInitiating graceful shutdown sequence...\n");
    destroy_thread_pool(); 
    //destroy_rate_limiter(); 
    close_log(); 
    close(epoll_fd);
    close(sockfd);

    printf("Proxy shut down successfully. All resources freed.\n");
    return 0;
}