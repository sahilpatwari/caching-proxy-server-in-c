#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<utime.h>
#include<dirent.h>
#include"cache.h"

#define MAX_CACHE_SIZE 1024

struct {
    char filename[256];
    time_t mtime;
    long size;
}CacheEntry;

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

int compare_cache_entries(const void* a,const void* b) {
    CacheEntry *entryA = (CacheEntry*)a;
    CacheEntry *entryB = (CacxheEntry*)b;
    return (entryA.st_mtime - entryB.st_mtime);
}

void enforce_cache_capacity() {
    DIR *d;
    struct dirent *dir;
    struct stat filestat;
    char file_path[256];
    
    CacheEntry entries[10000];
    int count = 0;
    long total_size = 0;

    d = opendir("cache");
    if(!d) {
        fprintf(stderr,"Could not open directory");
        return;
    }
    
    while((dir = readdir(d))) {
        if(dir->d_name[0] == '.') continue;
        
        snprintf(file_path,sizeof(file_path),"cache/%s",dir->d_name);

        if(stat(file_path,&filestat) == 0) {
              strcpy(entries[count].filename,filepath);
              entries[count].mtime = filestat.st_mtime;
              entries[count].size = filestat.st_size;

              total_size += filestat.st_size;
              count++;

              if(count >= 10000) break;
        }
    }
    closedir(d);
    
    qsort(entries,count,sizeof(CacheEntry),compare_cache_entries);
    int i = 0;
    while(total_size > MAX_CACHE_SIZE && i <= count) {
           printf("Cache Overflow (%ld/%d bytes). Evicting %s",total_size,MAX_CACHE_SIZE,entries[i].filename);
           if(remove(entries[i].filename) == 0) {
               total_size -= entries[i].size;
           } else {
               perror("Failed to delete cache file");
           }
           i++;
    }
}

