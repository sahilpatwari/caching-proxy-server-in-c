#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<utime.h>
#include<dirent.h>
#include<sys/socket.h>
#include<pthread.h>
#include"cache.h"
#include"proxy_log.h"
#include"errors.h"

#define MAX_CACHE_SIZE 1024 
#define BUFFER 8192

typedef struct {
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
    mark_as_used(filename);
    return 1;
}

int compare_cache_entries(const void* a,const void* b) {
    CacheEntry *entryA = (CacheEntry*)a;
    CacheEntry *entryB = (CacheEntry*)b;
    return (entryA->mtime - entryB->mtime);
}

void enforce_cache_capacity() {
    DIR *d;
    struct dirent *dir;
    struct stat filestat;
    char file_path[270];
    
    int max_files = 10000;
    CacheEntry *entries = malloc(max_files*sizeof(CacheEntry));
    if(!entries) {
        perror("malloc");
        return;
    }
    int count = 0;
    long total_size = 0;

    d = opendir("cache");
    if(!d) {
        fprintf(stderr,"Could not open directory");
        free(entries);
        return;
    }
    
    while((dir = readdir(d))) {
        if(dir->d_name[0] == '.') continue;
        
        snprintf(file_path,sizeof(file_path),"cache/%s",dir->d_name);

        if(stat(file_path,&filestat) == 0) {
              strcpy(entries[count].filename,file_path);
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
           printf("Cache Overflow (%ld/%d bytes). Evicting %s\n",total_size,MAX_CACHE_SIZE,entries[i].filename);
           if(remove(entries[i].filename) == 0) {
               total_size -= entries[i].size;
           } else {
               perror("Failed to delete cache file");
           }
           i++;
    }
    free(entries);
}

void serve_from_cache(int client_fd,char* url,char* client_ip,char* cache_file) {
    FILE *fp = fopen(cache_file,"rb");

    if (!fp) {
        perror("Error opening cache file");
        send_error_response(client_fd, 500, "Internal Cache Error");
        log_message(client_ip, url, 500, 0);
    }

    char file_buffer[BUFFER];
    int bytes_read;
    long total_sent = 0;
    int success = 1;
    while((bytes_read = fread(file_buffer,1,sizeof(file_buffer),fp)) > 0) {
    if(send(client_fd, file_buffer, bytes_read, MSG_NOSIGNAL) == -1) {
            perror("Client disconnected during cache transfer");
            success = 0;
            break;
        }
        total_sent += bytes_read;
    }
    fclose(fp);

    if (success) {
        log_message(client_ip, url, 200, total_sent);      
    } else {
        log_message(client_ip, url, 499, total_sent); // 499 Client Closed Request
    }
}

void fetch_and_cache(int client_fd,int serverfd,char* url,char* client_ip,char* cache_file) {

    enforce_cache_capacity();

    char temp_file[512];
    snprintf(temp_file,sizeof(temp_file),"%s.%ld.tmp",cache_file,pthread_self());

    FILE *cache_fp = fopen(temp_file,"wb");
    if(!cache_fp) {
        perror("Warning: Failed to create temp cache file");
    }
    char remote_buffer[8192];
    int n,completed = 0,has_sent_headers = 0;
    long total_bytes = 0;
    while((n =  recv(serverfd,remote_buffer,sizeof(remote_buffer) - 1,0)) > 0) {
        remote_buffer[n] = '\0';
        if(send(client_fd, remote_buffer, n, MSG_NOSIGNAL) == -1) {
            break;
        }
        has_sent_headers = 1;
        
        if(cache_fp) {
            if(fwrite(remote_buffer, 1, n, cache_fp) != n) {
                perror("Cache write error");
                fclose(cache_fp);
                cache_fp = NULL;
                remove(temp_file);
            }
        }
        total_bytes += n;
    }
    if (n == 0) {
        completed = 1;
    } else if (n < 0) {
        perror("Upstream recv error");
        if (!has_sent_headers) {
            send_error_response(client_fd, 502, "Bad Gateway: Upstream Error");
        }
    }

    if(cache_fp) {
        fclose(cache_fp);
        if(completed && total_bytes > 0) {
            if(rename(temp_file, cache_file) == 0) {
                printf("[Cache] Committed: %s (%ld bytes)\n", cache_file, total_bytes);
            } else {
                perror("Cache commit rename failed");
                remove(temp_file);
            }
        } else {
            remove(temp_file);
        }
    }
    
    if (completed) {
        log_message(client_ip, url, 200, total_bytes);
    } else {
        log_message(client_ip, url, 502, total_bytes);
    }
}

