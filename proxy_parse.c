#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include"proxy.h"

int parse_url(char* url , struct ProxyRequest *req) {
    req->port = 80;
    req->path[0] = '\0';
    char *host_start = url,*path_start;
    if(strncmp(url,"http://",7) == 0) {
        host_start += 7;
    }

    int host_len = 0;
    path_start = strpbrk(host_start,"/:");
    if(!path_start) {
         host_len = strlen(host_start);
    } else {
        host_len = path_start - host_start;
    }
    if(host_len > 1023) return -1;
    strncpy(req->hostname,host_start,host_len);
    req->hostname[host_len] = '\0';
    if(path_start && *path_start == ':') {
       req->port = atoi(path_start + 1);
       path_start = strchr(path_start,'/');
    }

    if(path_start) {
        strncpy(req->path,path_start,4095);
        req->path[4095] = '\0';
    } else {
        strcpy(req->path,"/");
    }

    return 0;
}