#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<unistd.h>
#include<string.h>

int connect_to_host(char* hostname,int port) {
    struct addrinfo hints, *res, *p;
    int sockfd,status;

    char port_str[6];
    snprintf(port_str,sizeof(port_str),"%d",port);
    
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if((status = getaddrinfo(hostname,port_str,&hints,&res)) == -1) {
        fprintf(stderr,"getaddrinfo: %s\n",gai_strerror(status));
        return -1;
    }

    for(p = res;p != NULL;p = p->ai_next) {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1) {
            perror("proxy: socket");
            return -1;
        }

        if(connect(sockfd,p->ai_addr,p->ai_addrlen) == -1) {
            perror("proxy: connect");
            return -1;
        }
        break;
    }
    freeaddrinfo(res);
    if(p == NULL) {
        fprintf(stderr,"The proxy couldn't connect to host %s\n",hostname);
        return -1;
    }
    
    return sockfd;
}