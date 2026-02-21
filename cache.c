#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<utime.h>
#include<dirent.h>
#include<sys/socket.h>
#include<pthread.h>
#include<stdint.h>
#include"cache.h"
#include"proxy_log.h"
#include"errors.h"

#define MAX_CACHE_SIZE 10*1024*1024 
#define BUFFER 8192

typedef struct {
    char filename[256];
    time_t mtime;
    long size;
}CacheEntry;

typedef struct {
    time_t expires_at;
    long content_length;
    char url[5120];
    char padding[256];
}CacheHeader;

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

int parse_cache_policy(char* response_buffer) {
    int ttl = 300;// Default TTL
    char* cc_header = strstr(response_buffer,"Cache-Control:");
    if(!cc_header) return ttl;
    cc_header += 15;
    if(strncmp(cc_header,"no-store",8) == 0 || strncmp(cc_header,"no-cache",8) == 0 || strncmp(cc_header,"private",7) == 0) {
        return - 1; // Don't cache
    }

    if(strncmp(cc_header,"max-age=",8) == 0) {
        ttl = atoi(cc_header + 8);
    }
    return ttl;
}

int check_cache(char* filename,char* url) {
    FILE* fp = fopen(filename,"rb");
    if(!fp) return 0;
    
    CacheHeader header;
    if(fread(&header,sizeof(CacheHeader),1,fp) != 1) {
        fclose(fp); // Invalid File
        return 0;
    }
    time_t now = time(NULL);
    if(now > header.expires_at) {
        printf("Cache Expired!Fetching a new copy....\n");
        return 0;
    }

    if(strcmp(url,header.url) != 0) {
        printf("URL Expected: %s GOT: %s\n",url,header.url);
        fclose(fp);
        return 0;
    }

    fseek(fp,0,SEEK_END);
    long actual_size = ftell(fp);
    long expected_size = sizeof(CacheHeader) + header.content_length;

    if(actual_size < expected_size) {
        fclose(fp);
        return 0;
    }
    fclose(fp);
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
    while(total_size > MAX_CACHE_SIZE && i < count) {
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

void serve_from_cache(int client_fd,char* url,char* client_ip,char* cache_file,uint64_t req_id) {
    FILE *fp = fopen(cache_file,"rb");

    if (!fp) {
        send_error_response(client_fd, 500, "Internal Cache Error");
        log_event(LEVEL_ERROR, req_id, client_ip, "Cache Hit but failed to open file");
        return;
    }
    
    log_event(LEVEL_DEBUG, req_id, client_ip, "Streaming from cache...");
    if(fseek(fp,sizeof(CacheHeader),SEEK_SET) != 0) {
        log_event(LEVEL_ERROR, req_id, client_ip, "Seek failed on cache file");
        fclose(fp);
        return;
    }
    char file_buffer[BUFFER];
    int bytes_read;
    long total_sent = 0;
    int success = 1;
    while((bytes_read = fread(file_buffer,1,sizeof(file_buffer),fp)) > 0) {
    if(send(client_fd, file_buffer, bytes_read, MSG_NOSIGNAL) == -1) {
            log_event(LEVEL_WARN, req_id, client_ip, "Client disconnected during cache stream");
            success = 0;
            break;
        }
        total_sent += bytes_read;
    }
    fclose(fp);
    char log_msg[128];
    if (success) {
       snprintf(log_msg, sizeof(log_msg), "Cache HIT served: %ld bytes", total_sent);
       log_event(LEVEL_INFO, req_id, client_ip, log_msg); 
    } else {
       log_event(LEVEL_WARN, req_id, client_ip, "Client Closed Request");
    }
}

void fetch_and_cache(int client_fd,int serverfd,char* url,char* client_ip,char* cache_file,uint64_t req_id) {

    enforce_cache_capacity();

    char temp_file[512];
    snprintf(temp_file,sizeof(temp_file),"%s.%ld.tmp",cache_file,pthread_self());

    FILE *cache_fp = fopen(temp_file,"wb");
    if(!cache_fp) {
        log_event(LEVEL_WARN, req_id, client_ip, "Failed to create temp cache file (Disk full?)");
    }
    log_event(LEVEL_DEBUG, req_id, client_ip, "Downloading from upstream...");

    char remote_buffer[BUFFER];
    int n,completed = 0,has_sent_headers = 0,is_cacheable = 1,first_chunk = 1;
    long total_bytes = 0;
    CacheHeader header;
    memset(&header,0,sizeof(CacheHeader));
    strncpy(header.url,url,sizeof(header.url) - 1);
    header.url[sizeof(header.url) - 1] = '\0';
    while((n =  recv(serverfd,remote_buffer,BUFFER - 1,0)) > 0) {
        remote_buffer[n] = '\0';
        int client_connected = 1;
        int ttl;
        int send_bytes = 0;
        if(first_chunk) {
            ttl = parse_cache_policy(remote_buffer);
            if(ttl < 0) {
                is_cacheable = 0;
                log_event(LEVEL_INFO, req_id, client_ip, "Policy: DO NOT CACHE");
            } else {
                is_cacheable = 1;
                header.expires_at = time(NULL) + ttl;
                if(fwrite(&header,sizeof(CacheHeader),1,cache_fp) != 1) {
                    is_cacheable = 0;
                }
            }
            first_chunk = 0;
        }
        int byt = 0;
        while(send_bytes < n) {
            if((byt = send(client_fd, remote_buffer + send_bytes, n - send_bytes, MSG_NOSIGNAL)) == -1) {
                client_connected = 0;
                log_event(LEVEL_WARN, req_id, client_ip, "Client disconnected during download");
                break;
            }
            send_bytes += byt;
        }
        if(!client_connected) {
            is_cacheable = 0;
            break;
        }
        has_sent_headers = 1;
        
        if(cache_fp && is_cacheable) {
            if(fwrite(remote_buffer, 1, n, cache_fp) != n) {
                log_event(LEVEL_ERROR, req_id, client_ip, "Cache write error");
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
        log_event(LEVEL_ERROR, req_id, client_ip, "Upstream recv error");
        if (!has_sent_headers) {
            send_error_response(client_fd, 502, "Bad Gateway: Upstream Error");
        }
    }

    if(cache_fp) {
        if(is_cacheable && completed && total_bytes > 0) {
            header.content_length = total_bytes;
            fseek(cache_fp,0,SEEK_SET);
            if(fwrite(&header,sizeof(CacheHeader),1,cache_fp) != 1) {
                is_cacheable = 0;
            }
            fclose(cache_fp);
            if(is_cacheable && rename(temp_file, cache_file) == 0) {
                log_event(LEVEL_DEBUG, req_id, client_ip, "Cache commit successful");
            } else {
               log_event(LEVEL_ERROR, req_id, client_ip, "Cache commit failed (rename)");
                remove(temp_file);
            }
        } else {
            fclose(cache_fp);
            remove(temp_file);
        }
    }
    
    char log_msg[128];
    if (completed) {
        snprintf(log_msg, sizeof(log_msg), "Cache MISS served: %ld bytes", total_bytes);
        log_event(LEVEL_INFO, req_id, client_ip, log_msg);
    } else {
        snprintf(log_msg, sizeof(log_msg), "Transaction Failed. Relayed: %ld bytes", total_bytes);
        log_event(LEVEL_WARN, req_id, client_ip, log_msg);
    }
}

