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

#include"proxy.h"
#include"cache.h"
#include"proxy_log.h"
#include"errors.h"

#define PORT "3490"
#define BACKLOG 10
#define BUFFER 8192

typedef struct {
    int client_fd;
}thread_args_t;


void* handle_client(void* args) {
    thread_args_t *thread_args = (thread_args_t*)args;
    int newfd = thread_args->client_fd;
    free(thread_args);

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
       log_message(client_ip, "MALFORMED_REQUEST", 400, 0);
       close(newfd);
       return NULL;
    }
    
    struct ProxyRequest req;
    if(parse_url(url,&req) != 0) {
        send_error_response(newfd, 400, "Invalid URL Format.");
        log_message(client_ip, url, 400, 0);
        close(newfd);
        return NULL;
    }
    printf("[Proxy] Request: %s %s\n", method, url);

    char cache_file[256];
    get_cache_filename(url,cache_file);
    printf("Checking cache: %s\n",cache_file);

    if(check_cache(cache_file)) {
        printf("[Cache] HIT: %s\n", url);
        serve_from_cache(newfd,url,client_ip,cache_file);
        close(newfd);
        return NULL;
    }

    printf("[Cache] MISS: Fetching %s\n", url);

    int serverfd = connect_to_host(req.hostname,req.port);
    if(serverfd == -1) {
        send_error_response(newfd, 502, "Bad Gateway: Could not connect to remote server");
        log_message(client_ip, url, 502, 0);
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
        log_message(client_ip, url, 503, 0);
        close(serverfd);
        close(newfd);
        return NULL;
    }
    
    fetch_and_cache(newfd,serverfd,url,client_ip,cache_file);

    close(serverfd);
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
    init_log();
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