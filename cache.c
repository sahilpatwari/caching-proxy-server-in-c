#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<dirent.h>
#include<sys/socket.h>
#include<pthread.h>
#include<stdint.h>
#include<sys/sendfile.h>
#include<sys/types.h>
#include<unistd.h>
#include<errno.h>
#include<fcntl.h>
#include"cache.h"

#define MAX_CACHE_SIZE 10*1024*1024 
#define DEFAULT_TTL 300
#define BUFFER 8192

extern int epoll_fd;
extern ConnectionContext* context_table[];
extern void handle_connect_upstream(ConnectionContext* ctx);
extern void handle_send_response_headers(ConnectionContext* ctx);
static pthread_mutex_t eviction_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    char filename[256];
    time_t mtime;
    long size;
}CacheEntry;

typedef struct {
    time_t expires_at;
    long content_length;
    int upstream_header_len;
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
    utimes(filename,NULL);
}

int parse_cache_policy(char* response_buffer) {
    int ttl = DEFAULT_TTL;// Default TTL
    char* cc_header = strstr(response_buffer,"Cache-Control:");
    if(!cc_header) return ttl;
    cc_header += 15;
    if(strncmp(cc_header,"no-store",8) == 0 || strncmp(cc_header,"no-cache",8) == 0 || strncmp(cc_header,"private",7) == 0) {
        return 0; // Don't cache
    }

    if(strncmp(cc_header,"max-age=",8) == 0) {
        ttl = atoi(cc_header + 8);
    }
    return ttl;
}

int filter_headers_to_buffer(int client_fd,char* header_block,int block_len,char* headers,int offset) {
    if (block_len < 0 || block_len >= BUFFER) return 0;
    char* save_ptr;
    char* headers_copy = malloc(block_len + 1);
    if(!headers_copy) return 0;
    memcpy(headers_copy,header_block,block_len);
    headers_copy[block_len] = '\0';

    char* line = strtok_r(headers_copy,"\r\n",&save_ptr);
    if(line) {
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    while(line != NULL) {
        if(strncmp(line,"Connection:",11) == 0 || strncmp(line,"Keep-Alive:", 11) == 0 || strncmp(line,"Transfer-Encoding:", 18) == 0 || strncmp(line,"Upgrade:", 8) == 0 || strncmp(line,"Content-Length:",15) == 0 || strncmp(line,"Proxy-Connection:",17) == 0) {
            line = strtok_r(NULL,"\r\n",&save_ptr);
            continue;
        }
        offset += snprintf(headers + strlen(headers),BUFFER - offset,"%s\r\n",line);
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    offset += snprintf(headers + strlen(headers),BUFFER - offset,"\r\n");
    free(headers_copy);
    return offset;
}

int check_cache(ConnectionContext* ctx) {
    char cache_file[256];
    get_cache_filename(ctx->url,cache_file);

    ctx->file_fd = open(cache_file,O_RDONLY);
    if(ctx->file_fd < 0) {
        //CACHE MISS
        return 0;
    }
    CacheHeader header;
    if (read(ctx->file_fd,&header,sizeof(CacheHeader))  != sizeof(CacheHeader)) {
        close(ctx->file_fd);
        unlink(cache_file);
        ctx->file_fd = -1;
        return 0;
    }
    
    struct stat st;
    fstat(ctx->file_fd,&st);
    long expected_size = sizeof(CacheHeader) + header.content_length;
    time_t now = time(NULL);

    if(now > header.expires_at || strcmp(ctx->url,header.url) != 0 || st.st_size < expected_size) {
        //CACHE MISS
        close(ctx->file_fd);
        unlink(cache_file);
        ctx->file_fd = -1;
        return 0;
    }
    
    ctx->upstream_content_length = header.content_length;
    ctx->upstream_header_len = header.upstream_header_len;
    mark_as_used(cache_file);
    return 1;
}

void handle_check_cache(ConnectionContext* ctx) {
    
    if(ctx->checkCache) {
        if(!check_cache(ctx)) {
            //CACHE MISS
            ctx->state = STATE_CONNECT_UPSTREAM;
            handle_connect_upstream(ctx);
            return;
        }
    }

    //CACHE HIT
    char file_buffer[BUFFER];
    int bytes_read = read(ctx->file_fd,file_buffer,sizeof(file_buffer));
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->bytes_remaining = ctx->upstream_content_length - ctx->upstream_header_len;
    ctx->write_len += snprintf(ctx->write_buf, sizeof(ctx->write_buf),
            "HTTP/1.1 200 OK\r\n"
            "Connection: keep-alive\r\n"
            "Content-Length: %ld\r\n",
            ctx->bytes_remaining);

    if(ctx->checkCache) {
        ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,
                          "X-Cache: HIT\r\n");
    }

    ctx->write_len += filter_headers_to_buffer(ctx->client_fd,file_buffer,ctx->upstream_header_len,ctx->write_buf,ctx->write_len);
    long body_offset = sizeof(CacheHeader) + ctx->upstream_header_len;
    lseek(ctx->file_fd,body_offset,SEEK_SET);
    ctx->state = STATE_SEND_RESPONSE_HEADERS;
    handle_send_response_headers(ctx);
    return;
}

int compare_cache_entries(const void* a,const void* b) {
    CacheEntry *entryA = (CacheEntry*)a;
    CacheEntry *entryB = (CacheEntry*)b;
    return (entryA->mtime - entryB->mtime);
}

void enforce_cache_capacity() {

    if(pthread_mutex_trylock(&eviction_lock) != 0) {
        return;
    }

    DIR *d;
    struct dirent *dir;
    struct stat filestat;
    char file_path[270];
    
    int max_files = 10000;
    CacheEntry *entries = malloc(max_files*sizeof(CacheEntry));
    if(!entries) {
        perror("malloc");
        pthread_mutex_unlock(&eviction_lock);
        return;
    }
    int count = 0;
    long total_size = 0;

    d = opendir("cache");
    if(!d) {
        fprintf(stderr,"Could not open directory");
        free(entries);
        pthread_mutex_unlock(&eviction_lock);
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
    if(total_size > MAX_CACHE_SIZE) {
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
    }
    free(entries);
    pthread_mutex_unlock(&eviction_lock);
}

void handle_send_cache(ConnectionContext *ctx) {
    while(ctx->bytes_remaining > 0) {
        ssize_t bytes_sent = sendfile(ctx->client_fd, ctx->file_fd, NULL , ctx->bytes_remaining);
        if (bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            perror("sendfile failed");
            ctx->state = STATE_CLOSE;
            return;
        } else if(bytes_sent == 0) {
            ctx->state = STATE_CLOSE;
            return;
        }
        ctx->bytes_remaining -= bytes_sent;
    }
    close(ctx->file_fd);
    ctx->file_fd = -1;
    if(ctx->keep_alive) {
        ctx->bytes_read = 0;
        memset(ctx->read_buf,0,sizeof(ctx->read_buf));
        ctx->state = STATE_READ_REQUEST;
    } else {
        ctx->state = STATE_CLOSE;
    }
}

void handle_fetch_upstream(ConnectionContext* ctx) {
    if(ctx->file_fd == -1) {
        char cache_file[256];
        get_cache_filename(ctx->url,cache_file);

        ctx->file_fd = open(cache_file,O_WRONLY | O_CREAT | O_TRUNC,0644);
        if(ctx->file_fd == -1) {
            perror("Failed to open Cache File");
            send_error_response(ctx->client_fd, 500, "Internal Cache Error",NULL);
            log_event(LEVEL_ERROR, ctx->req_id, ctx->client_ip, "Failed to open cache file");
            ctx->state = STATE_CLOSE;
            return;
        }

        ctx->upstream_headers_parsed = 0;
        ctx->cache_ttl = DEFAULT_TTL;
        ctx->upstream_header_len = 0;
        ctx->upstream_content_length = 0;
        ctx->upstream_body_downloaded = 0;

        CacheHeader dummyHeader;
        memset(&dummyHeader,0,sizeof(dummyHeader));
        if(write(ctx->file_fd,&dummyHeader,sizeof(dummyHeader)) < 0) {
            ctx->state = STATE_CLOSE;
            return;
        }
    }

    while(1) {
        if(!ctx->upstream_headers_parsed) {
            int n = recv(ctx->upstream_fd,ctx->upstream_header_buf + ctx->upstream_header_len,BUFFER - 1 - ctx->upstream_header_len,MSG_NOSIGNAL);

            if(n < 0) {
                if(errno == EWOULDBLOCK || errno == EAGAIN) return;
                ctx->state = STATE_CLOSE;
                return;
            }
            ctx->upstream_header_len += n;
            ctx->upstream_header_buf[ctx->upstream_header_len] = '\0';
            char* body_ptr = strstr(ctx->upstream_header_buf,"\r\n\r\n");
            if(body_ptr != NULL) {
                body_ptr += 4;
                char* cc_length = strstr(ctx->upstream_header_buf,"Content-Length:");
                if(cc_length) ctx->upstream_content_length = atoi(cc_length + 15);
        
                ctx->cache_ttl = parse_cache_policy(ctx->upstream_header_buf);

                int headers_len = body_ptr - ctx->upstream_header_buf;
                int body_in_buf = ctx->upstream_header_len - headers_len;

                ctx->upstream_body_downloaded += body_in_buf;
                write(ctx->file_fd,ctx->upstream_header_buf,ctx->upstream_header_len);
                ctx->upstream_header_len = headers_len;
                ctx->upstream_headers_parsed = 1;

                if(ctx->upstream_content_length > 0 && ctx->upstream_body_downloaded >= ctx->upstream_content_length) {
                    break;
                }
            }

            if(n == 0) break;
        } else {
            
            char temp_buf[BUFFER];
            int n = recv(ctx->upstream_fd, temp_buf,BUFFER - 1,MSG_NOSIGNAL);
            
            if(n < 0) {
                if(errno == EWOULDBLOCK || errno == EAGAIN) return;
                ctx->state = STATE_CLOSE;
                return;
            } else if(n == 0) {
                break;
            }

            if(write(ctx->file_fd,temp_buf,n) < 0) {
                perror("Disk Write Error");
                ctx->state = STATE_CLOSE;
                return;
            }
            ctx->upstream_body_downloaded += n;
            if(ctx->upstream_content_length > 0 && ctx->upstream_body_downloaded >= ctx->upstream_content_length) {
                break;
            }
        }
    }
    close(ctx->upstream_fd);
    context_table[ctx->upstream_fd] = NULL;
    ctx->upstream_fd = -1;

    
    CacheHeader finalHeader;
    memset(&finalHeader,0,sizeof(finalHeader));
    strncpy(finalHeader.url,ctx->url,sizeof(finalHeader.url) - 1);
    finalHeader.expires_at = time(NULL) + ctx->cache_ttl;
    
    struct stat st;
    fstat(ctx->file_fd,&st);
    finalHeader.content_length = st.st_size - sizeof(CacheHeader);
    ctx->bytes_remaining = finalHeader.content_length - ctx->upstream_header_len;
    finalHeader.upstream_header_len = ctx->upstream_header_len;
    lseek(ctx->file_fd,0,SEEK_SET);
    write(ctx->file_fd,&finalHeader,sizeof(finalHeader));

    close(ctx->file_fd);
    ctx->file_fd = -1;

    if(ctx->cache_ttl <= 0) {
        char cache_file[256];
        get_cache_filename(ctx->url,cache_file);
        unlink(cache_file);
    } else {
        enforce_cache_capacity();
    }
    
    char cache_file[256];
    get_cache_filename(ctx->url,cache_file);
    ctx->file_fd = open(cache_file,O_RDONLY);

    if(ctx->file_fd < 0) {
        ctx->state = STATE_CLOSE;
        return;
    }
    lseek(ctx->file_fd, sizeof(CacheHeader), SEEK_SET);
    ctx->checkCache = 0;
    ctx->state = STATE_CHECK_CACHE;
    handle_check_cache(ctx);
    return;
}

