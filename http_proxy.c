#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<pthread.h>
#include<unistd.h>
#include<string.h>

#include"proxy.h"
#include"cache.h"
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
    char buffer[BUFFER];
    printf("Client Connected\n");
    int bytes_received = recv(newfd,buffer,BUFFER - 1,0);
    if(bytes_received < 0) {
        perror("recieve");
        close(newfd);
        return NULL;
    }
    buffer[bytes_received] = '\0';
    const char* response;
    char method[16],url[5120],protocol[16];
    int count = sscanf(buffer,"%15s %5119s %15s",method,url,protocol);
    if(count != 3) {
        fprintf(stderr,"Unknown Error in Parsing the Request");
        response = "HTTP/1.1 500 Internal Server Error\r\n"
                "\r\n"
                "Internal Server Error";
        
        if(send(newfd,response,strlen(response),0) == -1) {
            perror("send");
            close(newfd);
            return NULL;
        }
    }
    struct ProxyRequest req;
    if(parse_url(url,&req) == 0) {
         printf("Proxy Request Detected\n");
         printf("Target Host: %s\n",req.hostname);
         printf("Port: %d\n",req.port);
         printf("Path: %s\n",req.path);
         
         char cache_file[224];
         get_cache_filename(url,cache_file);
         printf("Checking cache: %s\n",cache_file);
         if(check_cache(cache_file)) {
            printf("CACHE HIT Serving file from the disk\n");
            FILE *fp = fopen(cache_file,"rb");
            char file_buffer[8192];
            int bytes_read;
            while((bytes_read = fread(file_buffer,1,sizeof(file_buffer),fp)) > 0) {
                send(newfd,file_buffer,bytes_read,0);
            }
            fclose(fp);
            close(newfd);
            return NULL;
         }
         printf("CACHE MISS Fetching from the Network\n");
         int serverfd = connect_to_host(req.hostname,req.port);
         if(serverfd == -1) {
            char* err_msg = "HTTP 502 Bad Gateway\r\n\r\nConnection Failed";
            send(newfd,err_msg,strlen(err_msg),0);
         } else {
            char new_req[8192];
            snprintf(new_req,sizeof(new_req),
            "%s %s %s\r\nHost: %s\r\nConnection: close\r\n\r\n"
            ,method,req.path,protocol,req.hostname);
            
            printf("Forwarding Request\n");

            if(send(serverfd,new_req,strlen(new_req),0) == -1) {
                perror("send to server");
                close(serverfd);
                return NULL;
            }
            char temp_file[256];
            snprintf(temp_file,sizeof(temp_file),"%s.%ld.tmp",cache_file,pthread_self());

            FILE *cache_fp = fopen(temp_file,"wb");
            if(!cache_fp) {
                perror("Warning: Couldn't open the temp file to write in it");
            }
            char remote_buffer[8192];
            int n, total_bytes = 0;
            while((n =  recv(serverfd,remote_buffer,sizeof(remote_buffer) - 1,0)) > 0) {
                remote_buffer[n] = '\0';
                if(send(newfd,remote_buffer,n,0) == -1) {
                    perror("send to client");
                    break;
                }
                
                if(cache_fp) {
                    fwrite(remote_buffer,1,n,cache_fp);
                }

                total_bytes += n;
            }
            
            if(cache_fp) {
                fclose(cache_fp);
                if(total_bytes > 0) {
                    if(rename(temp_file,cache_file) == 0) {
                        printf("Cache Committed %s\n",cache_file);
                        printf("Cached %d bytes to %s\n",total_bytes,cache_file);
                    } else {
                        printf("Cache Commit Failed\n");
                    }
                } else {
                    remove(temp_file);
                }
            }
            printf("Total Number of Bytes relayed back to the client: %d\n",total_bytes);

            close(serverfd);

         }


    } else {
        printf("Invalid URL Format\n");
    }
    return NULL;
}
int main() {
    struct sockaddr_storage their_addr;
    struct addrinfo hints, *res, *p;
    socklen_t sin_size;
    int status, sockfd,newfd,yes = 1;

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