#define _GNU_SOURCE

#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>
#include<sys/time.h>
#include<dirent.h>
#include<sys/socket.h>
#include<stdint.h>
#include<sys/sendfile.h>
#include<sys/types.h>
#include<unistd.h>
#include<errno.h>
#include<fcntl.h>
#include<sys/epoll.h>
#include"cache.h"
#include"thread_pool.h"

#define MAX_CACHE_SIZE 10*1024*1024 
#define DEFAULT_TTL 300
#define BUFFER 8192
#define MAX_WAITERS 1024

extern int epoll_fd;
extern ConnectionContext* context_table[];
extern void handle_connect_upstream(ConnectionContext* ctx);
extern void handle_send_response_headers(ConnectionContext* ctx);
extern void stash_connection(int fd,char* hostname,int port);
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

typedef struct CacheNode {
    char url[5120];
    time_t expires_at;
    unsigned long content_length;
    int is_downloading;

    void* waiters[MAX_WAITERS];
    int num_waiters;

    struct CacheNode* next;
}CacheNode;

static CacheNode* cache_table[BUFFER];
static pthread_mutex_t cache_locks[BUFFER];
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

void init_cache_map() {
    for(int i = 0; i < BUFFER; i++) {
        cache_table[i] = NULL;
        pthread_mutex_init(&cache_locks[i],NULL);
    }
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
    
    int status_code = 0;
    if(sscanf(response_buffer,"%*s %d",&status_code) == 1) {
        if(status_code != 200) {
            return 0;
        }
    }
    
    char* cc_header = strcasestr(response_buffer,"Cache-Control:");
    if(!cc_header) return ttl;

    cc_header = strchr(cc_header,':');
    if(!cc_header) return ttl;

    cc_header++;

    char* end_of_line = strstr(cc_header,"\r\n");
    int line_len = end_of_line ? (end_of_line - cc_header) : strlen(cc_header);

    char header_val[256];
    snprintf(header_val,sizeof(header_val),"%.*s",(line_len < sizeof(header_val) - 1) ? line_len : (int)(sizeof(header_val)) - 1,cc_header);
    if(strcasestr(header_val,"no-store")  || strcasestr(header_val,"no-cache")  || strcasestr(header_val,"private")) {
        return 0; // Don't cache
    }
    char* max_age = strcasestr(header_val,"max-age");
    if(max_age) {
        ttl = atoi(max_age + 8);
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

void remove_from_cache_ram(char* url) {
    unsigned long hash = hash_url(url);
    int bucket = hash % BUFFER;

    pthread_mutex_lock(&cache_locks[bucket]);
    CacheNode *current = cache_table[bucket], *prev = NULL;
    while(current != NULL) {
        if(strcmp(url,current->url) == 0) {

             for(int i = 0; i < current->num_waiters; i++) {
                ConnectionContext* waiter = current->waiters[i];

                pthread_mutex_lock(&waiter->state_lock);
                if(waiter->state != STATE_CLOSE) {
                    waiter->state = STATE_CHECK_CACHE;
                }
                pthread_mutex_unlock(&waiter->state_lock);

                submit_task(handle_state_machine,waiter);
            }

            if(prev == NULL) {
                current = current->next;
            } else {
                prev->next = current->next;
            }

            free(current);
            pthread_mutex_unlock(&cache_locks[bucket]);
            return;
        }
        prev = current;
        current = current->next;
    }
    pthread_mutex_unlock(&cache_locks[bucket]);
}

void add_to_cache_ram(char* url,time_t expires_at, size_t content_length) {
    unsigned long hash = hash_url(url);
    int bucket = hash % BUFFER;

    pthread_mutex_lock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    while(current != NULL) {
        if(strcmp(url,current->url) == 0) {
            current->expires_at = expires_at;
            current->content_length = content_length;
            current->is_downloading = 0;

            for(int i = 0; i < current->num_waiters; i++) {
                ConnectionContext* waiter = current->waiters[i];

                pthread_mutex_lock(&waiter->state_lock);
                if(waiter->state != STATE_CLOSE) {
                    waiter->state = STATE_CHECK_CACHE;
                }
                pthread_mutex_unlock(&waiter->state_lock);

                submit_task(handle_state_machine,waiter);
            }
            current->num_waiters = 0;
            pthread_mutex_unlock(&cache_locks[bucket]);
            return;
        }
        current = current->next;
    }
    pthread_mutex_unlock(&cache_locks[bucket]);
}

int check_cache_ram(void* ctx_ptr,size_t* file_size) {
    ConnectionContext* ctx = (ConnectionContext*)ctx_ptr;
    unsigned long hash = hash_url(ctx->url);
    int bucket = hash % BUFFER;
    
    pthread_mutex_lock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    while(current != NULL) {
        if(strcmp(ctx->url,current->url) == 0) {
            if(current->is_downloading) {
                if(current->num_waiters < MAX_WAITERS) {
                    current->waiters[current->num_waiters++] = ctx;
                    atomic_fetch_add(&ctx->active_threads,1);
                    pthread_mutex_unlock(&cache_locks[bucket]);
                    return 2;
                }

                pthread_mutex_unlock(&cache_locks[bucket]);
                return 0;
            }

            if(current->expires_at < time(NULL)) {
                current->is_downloading = 1;
                current->num_waiters = 0;
                pthread_mutex_unlock(&cache_locks[bucket]);
                return -1; // Expired
            } 
            
            *file_size = current->content_length;
            pthread_mutex_unlock(&cache_locks[bucket]);
            return 1; // Cache Hit
        }
        current = current->next;
    }

    CacheNode* new_node = malloc(sizeof(CacheNode));
    strncpy(new_node->url,ctx->url,sizeof(new_node->url) - 1);
    new_node->url[sizeof(new_node->url) - 1] = '\0';
    new_node->is_downloading = 1;
    new_node->expires_at = 0;
    new_node->content_length = 0;

    new_node->next = cache_table[bucket];
    cache_table[bucket] = new_node;
    
    ctx->is_designated_downloader = 1;
    pthread_mutex_unlock(&cache_locks[bucket]);
    return 0; // Cache Miss;
}

int check_cache(ConnectionContext* ctx) {
    size_t file_size = 0;
    int status = check_cache_ram(ctx,&file_size);

    if(status == 1) {
        char cache_file[256];
        get_cache_filename(ctx->url,cache_file);

        ctx->file_fd = open(cache_file,O_RDONLY);
        if(ctx->file_fd < 0) {
            return 0;
        }

        ctx->bytes_remaining = file_size;
        mark_as_used(cache_file);
        return 1;
    } else if(status == 2) {
        return -1;
    } else {
         return 0;
    }
}

void handle_check_cache(ConnectionContext* ctx) {
    
    if(ctx->checkCache) {
        int isHit = check_cache(ctx);
        if(isHit == 0) {
            //CACHE MISS
            ctx->state = STATE_CONNECT_UPSTREAM;
            handle_connect_upstream(ctx);
            return;
        } else if(isHit == -1) {
            ctx->state = STATE_WAIT_CACHE;
            return;
        }
    }

    //CACHE HIT
    char file_buffer[BUFFER];
    int bytes_read = read(ctx->file_fd,file_buffer,sizeof(file_buffer));
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->write_len += snprintf(ctx->write_buf, sizeof(ctx->write_buf),
            "HTTP/1.1 200 OK\r\n"
            "Connection: keep-alive\r\n"
            "Content-Length: %ld\r\n",
            ctx->bytes_remaining);

    if(ctx->checkCache) {
        ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,
                          "X-Cache: HIT\r\n");
    }

    ctx->write_len = filter_headers_to_buffer(ctx->client_fd,file_buffer,ctx->upstream_header_len,ctx->write_buf,ctx->write_len);
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

            if(errno == EPIPE || errno == ECONNRESET) {
                ctx->state = STATE_CLOSE;
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

        ctx->write_len = 0;
        ctx->write_offset = 0;
        ctx->bytes_remaining = 0;

        ctx->upstream_header_len = 0;
        ctx->upstream_headers_parsed = 0;
        ctx->upstream_content_length = 0;
        ctx->upstream_body_downloaded = 0;
        ctx->is_chunked = 0;
        ctx->chunk_state = 0;
        ctx->header_overshoot_len = 0;

        ctx->checkCache = 0;
        memset(&(ctx->req),0,sizeof(ctx->req));
        memset(ctx->method,0,sizeof(ctx->method));
        memset(ctx->url,0,sizeof(ctx->url));
        memset(ctx->protocol,0,sizeof(ctx->protocol));
        ctx->state = STATE_READ_REQUEST;
    } else {
        ctx->state = STATE_CLOSE;
    }
}

void handle_fetch_upstream(ConnectionContext* ctx) {
    if(ctx->file_fd == -1) {
        char cache_file[256],temp_file[300];
        get_cache_filename(ctx->url,cache_file);
        
        snprintf(temp_file, sizeof(temp_file), "%s.tmp.%d", cache_file, ctx->client_fd);

        ctx->file_fd = open(temp_file,O_WRONLY | O_CREAT | O_TRUNC,0644);
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
    int upstream_dropped = 0;
    while(1) {
        if(!ctx->upstream_headers_parsed) {

            if(ctx->upstream_header_len >= BUFFER - 1) {
                perror("Upstream Headers too large: exceeded limit");
                ctx->state = STATE_CLOSE;
                return;
            }

            int n = recv(ctx->upstream_fd,ctx->upstream_header_buf + ctx->upstream_header_len,BUFFER - 1 - ctx->upstream_header_len,MSG_NOSIGNAL);

            if(n < 0) {
                if(errno == EWOULDBLOCK || errno == EAGAIN) return;
                ctx->state = STATE_CLOSE;
                return;
            } else if(n == 0) {
                upstream_dropped = 1;
                break;
            }
            ctx->upstream_header_len += n;
            ctx->upstream_header_buf[ctx->upstream_header_len] = '\0';
            char* body_ptr = strstr(ctx->upstream_header_buf,"\r\n\r\n");
            if(body_ptr != NULL) {
                body_ptr += 4;
                char* cc_length = strstr(ctx->upstream_header_buf,"Content-Length:");
                if(cc_length) ctx->upstream_content_length = atoi(cc_length + 15);
                
                char* te_header = strstr(ctx->upstream_header_buf,"Transfer-Encoding:");
                if(te_header && strstr(te_header,"chunked")) {
                    ctx->is_chunked = 1;
                    ctx->chunk_state = 0;
                    ctx->current_chunk_bytes_read = 0;
                    ctx->hex_idx = 0;
                }

                ctx->cache_ttl = parse_cache_policy(ctx->upstream_header_buf);

                int headers_len = body_ptr - ctx->upstream_header_buf;
                ctx->header_overshoot_len = ctx->upstream_header_len - headers_len;
                
                write(ctx->file_fd,ctx->upstream_header_buf,headers_len);
                ctx->upstream_header_len = headers_len;

                if(ctx->header_overshoot_len > 0) {
                    memcpy(ctx->header_overshoot_buf,body_ptr,ctx->header_overshoot_len);
                }

                ctx->upstream_headers_parsed = 1;
                continue;
            }
        } else {
            
            char temp_buf[BUFFER];
            int n; 
            
            if(ctx->header_overshoot_len > 0) {
                memcpy(temp_buf,ctx->header_overshoot_buf,ctx->header_overshoot_len);
                n = ctx->header_overshoot_len;
                ctx->header_overshoot_len = 0;
            } else {
                n = recv(ctx->upstream_fd, temp_buf,BUFFER - 1,MSG_NOSIGNAL);

                if(n < 0) {
                    if(errno == EWOULDBLOCK || errno == EAGAIN) return;
                    ctx->state = STATE_CLOSE;
                    return;
                } else if(n == 0) {
                    upstream_dropped = 1;
                    break;
                }
            }
            
            if(ctx->is_chunked) {
                int i = 0;
                while(i < n && ctx->chunk_state != 3) {
                    if(ctx->chunk_state == 0) {
                        char c = temp_buf[i++];
                        if(c == '\r') continue;

                        if(c == '\n') {
                            ctx->hex_buf[ctx->hex_idx] = '\0';
                            ctx->current_chunk_size = strtol(ctx->hex_buf,NULL,16);
                            ctx->hex_idx = 0;

                            if(ctx->current_chunk_size == 0) {
                                ctx->chunk_state = 3;
                            } else {
                                ctx->chunk_state = 1;
                                ctx->current_chunk_bytes_read = 0;
                            }
                        } else {
                            if(ctx->hex_idx < 31) ctx->hex_buf[ctx->hex_idx++] = c;
                        }   
                    } else if(ctx->chunk_state == 1) {
                        long to_read = ctx->current_chunk_size - ctx->current_chunk_bytes_read;
                        long availiable = n - i;
                        long write_len = (availiable < to_read) ? availiable : to_read;

                        if(write(ctx->file_fd,temp_buf + i,write_len) != write_len) {
                            perror("Disk Write Failure");
                            ctx->state = STATE_CLOSE;
                            return;
                        }

                        ctx->current_chunk_bytes_read += write_len;
                        ctx->upstream_body_downloaded += write_len;
                        i += write_len;
                        if(ctx->current_chunk_bytes_read == ctx->current_chunk_size) {
                            ctx->chunk_state = 2;
                        }

                    } else if(ctx->chunk_state == 2) {
                        char c = temp_buf[i++];
                        if(c == '\n') {
                             ctx->chunk_state = 0;
                        }
                    }
                }

                if(ctx->chunk_state == 3) {
                    break;
                }
            } else {
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
    }
    
    epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->upstream_fd,NULL);
    context_table[ctx->upstream_fd] = NULL;

    char cache_file[256],temp_file[300];
    get_cache_filename(ctx->url,cache_file);
    snprintf(temp_file, sizeof(temp_file), "%s.tmp.%d", cache_file, ctx->client_fd);

    if(upstream_dropped) {
        close(ctx->upstream_fd);
    } else {
        stash_connection(ctx->upstream_fd,(ctx->req).hostname,(ctx->req).port);
    }
    ctx->upstream_fd = -1;
    
    if(ctx->upstream_content_length > 0 && ctx->upstream_body_downloaded < ctx->upstream_content_length) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
        unlink(temp_file);
        ctx->state = STATE_CLOSE;
        return;
    }
    
    CacheHeader finalHeader;
    memset(&finalHeader,0,sizeof(CacheHeader));
    strncpy(finalHeader.url,ctx->url,sizeof(finalHeader.url) - 1);
    finalHeader.expires_at = time(NULL) + ctx->cache_ttl;
    
    struct stat st;
    fstat(ctx->file_fd,&st);
    finalHeader.content_length = st.st_size - sizeof(CacheHeader);
    ctx->bytes_remaining = finalHeader.content_length - ctx->upstream_header_len;
    finalHeader.upstream_header_len = ctx->upstream_header_len;
    ctx->upstream_content_length = finalHeader.content_length;
    lseek(ctx->file_fd,0,SEEK_SET);
    write(ctx->file_fd,&finalHeader,sizeof(finalHeader));

    close(ctx->file_fd);
    ctx->file_fd = -1;

    if(ctx->cache_ttl <= 0) {
        ctx->file_fd = open(temp_file, O_RDONLY);

        unlink(temp_file);
    } else {
        if(rename(temp_file,cache_file) == 0) {
            add_to_cache_ram(ctx->url,finalHeader.expires_at,ctx->bytes_remaining);
            ctx->is_designated_downloader = 0;
        }

        enforce_cache_capacity();

        ctx->file_fd = open(cache_file,O_RDONLY);
    }


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

