#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<pthread.h>
#include<unistd.h>
#include<sys/stat.h>
#include<string.h>
#include<signal.h>
#include <stdatomic.h>
#include <time.h>
#include <stdint.h> 
#include <inttypes.h>

#include"proxy.h"
#include"cache.h"
#include"proxy_log.h"
#include"errors.h"
#include"rate_limiter.h"

#define PORT "3490"
#define BACKLOG 10
#define BUFFER 8192

// ATOMIC REQUEST ID COUNTER
static _Atomic uint64_t global_req_id = 0;

typedef struct {
    int client_fd;
}thread_args_t;

void handle_tunnel_request(int client_fd,struct ProxyRequest *req,char* initial_buffer,int initial_len,char* method,char* url,char* protocol,char* client_ip,uint64_t req_id) {
    
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
        send_error_response(client_fd,411, "Content-Length Required");
        log_event(LEVEL_WARN, req_id, client_ip,"Content-Length Required");
        return;
    }

    int serverfd = connect_to_host(req->hostname,req->port);
    if(serverfd == -1) {
        send_error_response(client_fd, 502, "Bad Gateway: Could not connect to remote server");
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
        send_error_response(client_fd,500,"Internal Server Error");
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
            send_error_response(client_fd, 502, "Bad Gateway");
            log_event(LEVEL_ERROR, req_id, client_ip, "Failed to send headers to upstream");
            close(serverfd);
            return;
        }
        header_len -= n;
    }
    log_event(LEVEL_DEBUG,req_id,client_ip, "Headers forwarded to upstream");

    //Send remaining body in buffer
    int body_in_buffer = initial_len - (body_start - initial_buffer);
    while(body_in_buffer > 0) {
        int n;
        if((n =send(serverfd,body_start,body_in_buffer,MSG_NOSIGNAL)) == -1) {
            perror("Failed to send remaining body");
            log_event(LEVEL_WARN, req_id, client_ip, "Upstream closed during initial body send");
            close(serverfd);
            return;
        }
        content_length -= n;
        body_in_buffer -= n;
    }
    char buffer[BUFFER];
    while(content_length > 0) {
        int bytes,len;
        if((bytes = recv(client_fd,buffer,BUFFER - 1,MSG_NOSIGNAL)) == -1) {
            log_event(LEVEL_WARN,req_id,client_ip,"Client disconnected during body upload");
            close(serverfd);
            return;
        }
        buffer[bytes] ='\0';
        len = bytes;
        while(bytes > 0) {
            int n;
            if((n = send(serverfd,buffer,bytes,MSG_NOSIGNAL)) == -1) {
                perror("Failed to send body");
                log_event(LEVEL_WARN, req_id, client_ip, "Upstream closed during initial body send");
                close(serverfd);
                return;
            }
            bytes -= n;
        }
        content_length -= len;
    }
    long total_bytes = 0,recv_bytes;
    char recv_buffer[BUFFER];
    while((recv_bytes = recv(serverfd,recv_buffer,BUFFER - 1,0)) > 0) {
        while(recv_bytes > 0) {
            int n;
            if((n = send(client_fd,recv_buffer,recv_bytes,0)) == -1) {
                perror("Failed to send to client");
                log_event(LEVEL_WARN, req_id, client_ip, "Client disconnected during responding");
                close(serverfd);
                return;
            }
            recv_bytes -= n;
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
}

void* handle_client(void* args) {
    thread_args_t *thread_args = (thread_args_t*)args;
    int newfd = thread_args->client_fd;
    free(thread_args);
    
    // GENERATE ID
    uint64_t req_id = global_req_id++;

    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    char client_ip[INET6_ADDRSTRLEN];
    if(getpeername(newfd,(struct sockaddr*)&addr,&addr_size) == 0) {
          if(getnameinfo((struct sockaddr*)&addr,addr_size,client_ip,sizeof(client_ip),NULL,0,NI_NUMERICHOST) != 0) {
            strcpy(client_ip,"UNKNOWN");
        }
    } else {
        strcpy(client_ip,"UNKNOWN");
    }
    
    //RATE LIMIT
    if(!check_rate_limit(client_ip)) {
        printf("[Rate Limit] Denied: %s\n", client_ip);
        log_event(LEVEL_WARN, req_id, client_ip, "Rate Limit Exceeded (429)");
        send_error_response(newfd, 429, "Too Many Requests. Slow down.");
        close(newfd);
        return NULL;
    }
    char buffer[BUFFER];
    printf("Client Connected\n");
    int bytes_received = recv(newfd,buffer,BUFFER - 1,0);
    if(bytes_received < 0) {
        perror("recv");
        send_error_response(newfd, 400, "Failed to read request.");
        close(newfd);
        return NULL;
    } else if(bytes_received == 0) {
        close(newfd);
        return NULL;
    }

    buffer[bytes_received] = '\0';
    const char* response;
    char method[16],url[5120],protocol[16];
    int count = sscanf(buffer,"%15s %5119s %15s",method,url,protocol);
    if(count != 3) {
       send_error_response(newfd, 400, "Invalid HTTP Request Format.");
       log_event(LEVEL_WARN, req_id, client_ip, "Malformed Request");
       close(newfd);
       return NULL;
    }
    
    struct ProxyRequest req;
    if(parse_url(url,&req) != 0) {
        send_error_response(newfd, 400, "Invalid URL Format.");
        log_event(LEVEL_WARN, req_id, client_ip, "Invalid URL");
        close(newfd);
        return NULL;
    }
    printf("[Proxy] Request: %s %s\n", method, url);
    char log_buf[1536];
    snprintf(log_buf, sizeof(log_buf), "Request: %s %s:%d", method, req.hostname, req.port);
    log_event(LEVEL_INFO, req_id, client_ip, log_buf);


    if(strncmp(method,"GET",3) == 0) {
        char cache_file[256];
        get_cache_filename(url,cache_file);
        printf("Checking cache: %s\n",cache_file);

        if(check_cache(cache_file,url)) {
            printf("[Cache] HIT: %s\n", url);
            serve_from_cache(newfd,url,client_ip,cache_file,req_id);
            close(newfd);
            return NULL;
        }

        printf("[Cache] MISS: Fetching %s\n", url);

        int serverfd = connect_to_host(req.hostname,req.port);
        if(serverfd == -1) {
            send_error_response(newfd, 502, "Bad Gateway: Could not connect to remote server");
            snprintf(log_buf, sizeof(log_buf), "Upstream connection failed: %s", req.hostname);
            log_event(LEVEL_ERROR, req_id, client_ip, log_buf);
            close(newfd);
            return NULL;
        }
        char new_req[BUFFER];
        snprintf(new_req,sizeof(new_req),
        "%s %s %s\r\nHost: %s\r\nConnection: close\r\n"
        ,method,req.path,protocol,req.hostname);
        
        char *buffer_ptr;
        char *token = strtok_r(buffer,"\r\n",&buffer_ptr);
        while(token != NULL) {
            if(strncmp(token,method,strlen(method)) == 0 || strncmp(token,"Host",4) == 0 || strncmp(token,"Connection",10) == 0) {
                token = strtok_r(NULL,"\r\n",&buffer_ptr);
                continue;
            }
            snprintf(new_req + strlen(new_req),sizeof(new_req),"%s\r\n",token);
            token = strtok_r(NULL,"\r\n",&buffer_ptr);
        }
        snprintf(new_req + strlen(new_req),sizeof(new_req),"\r\n");
        printf("%s",new_req);
        printf("Forwarding Request\n");

        if(send(serverfd,new_req,strlen(new_req),0) == -1) {
            perror("Upstream send failed");
            send_error_response(newfd, 503, "Service Unavailable");
            snprintf(log_buf, sizeof(log_buf), "Failed to send headers to: %s", req.hostname);
            log_event(LEVEL_ERROR, req_id, client_ip, log_buf);
            close(serverfd);
            close(newfd);
            return NULL;
        }
        
        fetch_and_cache(newfd,serverfd,url,client_ip,cache_file,req_id);
        close(serverfd);
    } else if(strncmp(method,"POST",4) == 0) {
        printf("[Proxy] Tunneling POST Request\n");
        handle_tunnel_request(newfd, &req, buffer, bytes_received,method,url,protocol,client_ip,req_id);
    } else {
        send_error_response(newfd,501,"Not Implemented");
        log_event(LEVEL_WARN, req_id, client_ip, "Unsupported Method");
    }
    close(newfd);
    return NULL;
}
int main() {
    struct sockaddr_storage their_addr;
    struct addrinfo hints, *res, *p;
    socklen_t sin_size;
    int status, sockfd,newfd,yes = 1;
    
    signal(SIGPIPE, SIG_IGN); 
    mkdir("cache", 0777);
    init_log(LEVEL_DEBUG);
    init_rate_limiter();
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
    while(1) {
        sin_size = sizeof their_addr;
        newfd = accept(sockfd,(struct sockaddr*)&their_addr,&sin_size);
        if(newfd == -1) {
            perror("accept");
            continue;
        }
        thread_args_t *args = malloc(sizeof(thread_args_t));
        if(!args) {
            perror("malloc");
            close(newfd);
            continue;
        }

        args->client_fd = newfd;
        pthread_t pid;
        if(pthread_create(&pid,NULL,handle_client,(void*)args) != 0) {
            free(args);
            close(newfd);
        } else {
            pthread_detach(pid);
        }

    }
    return 0;
}