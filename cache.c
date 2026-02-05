#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<utime.h>
#include<dirent.h>
#include"cache.h"

#define MAX_CACHE_SIZE 1024
//djb2 hash algorithm
unsigned long hash_url(char* url) {
    char* str = url;
    unsigned long hash = 5381;
    int c;
    while(c = *str++) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

void get_cache_filename(char* url,char* buffer) {
    unsigned long hash= hash_url(url);
    sprintf(buffer,"cache/%lu",hash);
}

void mark_as_used(char* filename) {
    utime(filename,NULL);
}

int check_cache(char* filename) {
    struct stat buffer;

    if(stat(filename,&buffer) != 0) {
        return 0;
    }
    time_t now = time(NULL);
    double seconds_passed = difftime(now,buffer.st_mtime);

    printf("File Age: %.0f seconds passed\n",seconds_passed);

    if(seconds_passed > 60) {
        printf("Cache Expired!Fetching a new copy....\n");
        return 0;
    }
    return 1;
}

void enforce_cache_capacity() {
    DIR *d;
    struct dirent *dir;
    struct stat filestat;
    char file_path[300];

    long total_size = 0;
    char oldest_file_path[300] = "";
    time_t oldest_time = time(NULL);
    d = opendir("cache");
    if(!d) {
        fprintf(stderr,"Could not open directory");
        return;
    }
    
    while((dir = readdir(d))) {
        if(dir->d_name[0] == '.') continue;
        
        snprintf(file_path,sizeof(file_path),"cache/%s",dir->d_name);

        if(stat(file_path,&filestat) == 0) {
              total_size += filestat.st_size;
              if(filestat.st_mtime < oldest_time) {
                oldest_time = filestat.st_mtime;
                strcpy(oldest_file_path,file_path);
              }
        }
    }
    closedir(d);

    if(total_size > MAX_CACHE_SIZE) {
        if(strlen(oldest_file_path) > 0) {
            printf("Cache Full(%ld bytes). LRU-style eviction %s\n",total_size,oldest_file_path);
            remove(oldest_file_path);
        }
    }
}

