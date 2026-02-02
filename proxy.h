#ifndef PROXY_H
#define PROXY_H

struct ProxyRequest {
    char hostname[1024];
    int port;
    char path[4096];
};

int parse_url(char*,struct ProxyRequest*);
int connect_to_host(char*,int);

#endif