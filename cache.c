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
#include<netdb.h>
#include<arpa/inet.h>
#include<netinet/tcp.h>
#include<sys/mman.h>
#include"cache.h"
#include"thread_pool.h"

#define CACHE_BUCKETS        8192
#define MAX_WAITERS          8192
#define PERSIST_QUEUE_SIZE   8192
#define PERSIST_BATCH_SIZE   1024
extern _Atomic int active_upstream_connections;

extern __thread int epoll_fd;
extern _Atomic(ConnectionContext*) context_table[];
extern void handle_connect_upstream(ConnectionContext* ctx);
extern void handle_send_response_headers(ConnectionContext* ctx);
extern void stash_connection(int fd,char* hostname,int port);
extern void signal_reactor_task(int target_reactor_id,ConnectionContext* ctx);
extern _Atomic uint64_t metric_cache_hits;
extern _Atomic uint64_t metric_cache_misses;

extern _Atomic uint64_t global_upstream_latency_us;
extern _Atomic int consecutive_upstream_errors;
static FILE* registry_file = NULL;
static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;

extern Reactor* global_reactors;
void* expiry_worker(void*);
void* persistence_worker(void*);

typedef struct {
    char action;
    char url[512];
    time_t expires_at;
    time_t cached_at;
    unsigned long content_length;
    int upstream_header_len;
}RegistryRecord;

typedef struct {
    time_t expires_at;
    time_t cached_at;
    long content_length;
    int upstream_header_len;
    char url[512];
    char padding[248];
}CacheHeader;

void init_registry() {
    registry_file = fopen("cache_registry.bin","ab+");
    if(!registry_file) {
        perror("Failed to open registry file! Fast boot disabled");
    }
}

void append_to_registry(char action,char* url,time_t expires_at,time_t cached_at,unsigned content_length,int upstream_header_len) {
    if(!registry_file) return;

    RegistryRecord rec;
    memset(&rec, 0, sizeof(RegistryRecord));
    rec.action = action;
    strncpy(rec.url,url,sizeof(rec.url) - 1);
    rec.url[sizeof(rec.url) - 1] = '\0';
    rec.expires_at = expires_at;
    rec.cached_at = cached_at;
    rec.content_length = content_length;
    rec.upstream_header_len = upstream_header_len;

    pthread_mutex_lock(&registry_lock);
    fwrite(&rec,sizeof(RegistryRecord),1,registry_file);
    fflush(registry_file);
    pthread_mutex_unlock(&registry_lock);
}


typedef struct CacheNode {
    char url[512];
    time_t expires_at;
    time_t cached_at;

    char* response;
    long response_len;

    int is_large;
    long body_size;
    int upstream_header_len;
    
    _Atomic time_t last_accessed;
    
    int is_downloading;
    void* waiters[MAX_WAITERS];
    int num_waiters;
    
    struct CacheNode* next;

    long memory_usage;

    _Atomic int ref_count;

    int purged;
    int sync_to_disk;
}CacheNode;

static CacheNode* cache_table[CACHE_BUCKETS];
static pthread_rwlock_t cache_locks[CACHE_BUCKETS];

_Atomic long total_cache_memory = 0;

static pthread_t expiry_thread;
static pthread_t persistence_worker_thread;
static volatile int  cache_running;

static CacheNode* persist_queue[PERSIST_QUEUE_SIZE];
static int p_head = 0;
static int p_tail = 0;
static int p_count = 0;
static pthread_mutex_t p_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t p_notify = PTHREAD_COND_INITIALIZER;

//djb2 hash algorithm
unsigned long hash_url(char* url) {
    char* str = url;
    unsigned long hash = 5381;
    int c;
    while((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
} 

//fnv-1a hash algorithm
unsigned long hash_url_second(char* url) {
    char* str = url;
    unsigned long hash = 14695981039346656037UL;
    int c;
    while((c = *str++)) {
        hash ^= c;
        hash *= 1099511628211;
    }
    return hash;
}

static void approximate_evict_if_needed() {
    //Approximate LRU Logic
    while(atomic_load(&total_cache_memory) >= (size_t)global_config.max_cache_mem) {
        CacheNode* best_victim = NULL;
        int best_bucket = -1;
        time_t oldest_time = time(NULL) + 1;
        
        unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)pthread_self();

        for(int i = 0; i < 5; i++) {
            int bucket = rand_r(&seed) % CACHE_BUCKETS;
            pthread_rwlock_rdlock(&cache_locks[bucket]);
            CacheNode* current = cache_table[bucket];
            while(current) {
                if(!current->is_downloading && current->response != NULL && current->ref_count == 0) {
                    time_t acc = atomic_load(&current->last_accessed);
                    if(acc < oldest_time) {
                        oldest_time = acc;
                        best_bucket = bucket;
                        best_victim = current;
                    }
                }
                current = current->next;
            }
           pthread_rwlock_unlock(&cache_locks[bucket]);
        }

        if(best_victim != NULL && best_bucket != -1 && !best_victim->is_downloading && best_victim->ref_count == 0) {
            pthread_rwlock_wrlock(&cache_locks[best_bucket]);
            int found = 0;
            CacheNode* current = cache_table[best_bucket];
            while(current) {
                if(current == best_victim) {
                    found = 1;
                    break;
                }
                current = current->next;
            }

            if(found) {
                long mem_usage = best_victim->memory_usage;
                atomic_fetch_sub(&total_cache_memory,mem_usage);
                free(best_victim->response);
                best_victim->response = NULL;
                best_victim->response_len = 0;
                if(!best_victim->sync_to_disk) {
                    best_victim->is_large = 1;
                }
                best_victim->memory_usage = 0;
            }
            pthread_rwlock_unlock(&cache_locks[best_bucket]);
        } else {
            break;
        }
    }
}
void init_cache() {
    for(int i = 0; i < CACHE_BUCKETS; i++) {
        cache_table[i] = NULL;
        pthread_rwlock_init(&cache_locks[i],NULL);
    }
    atomic_store(&total_cache_memory,0);
    
    cache_running = 1;
    pthread_create(&expiry_thread,NULL,expiry_worker,NULL);
    pthread_create(&persistence_worker_thread,NULL,persistence_worker,NULL);
}

void destroy_cache() {
    cache_running = 0;
    pthread_cancel(expiry_thread);
    pthread_join(expiry_thread,NULL);
    
    pthread_cond_broadcast(&p_notify);
    pthread_cancel(persistence_worker_thread);
    pthread_join(persistence_worker_thread,NULL);

    for(int i = 0; i < CACHE_BUCKETS; i++) {
        pthread_rwlock_wrlock(&cache_locks[i]);
        CacheNode* node = cache_table[i];
        while(node) {
            CacheNode* temp = node;
            node = node->next;
            free(temp->response);
            free(temp);
        }
        pthread_rwlock_unlock(&cache_locks[i]);
        pthread_rwlock_destroy(&cache_locks[i]);
    }

    if(registry_file) {
        fclose(registry_file);
        registry_file = NULL;
    }
}

void get_cache_filename(char* url,char* buffer) {
    unsigned long hash= hash_url(url);
    unsigned long hash_second = hash_url_second(url);
    sprintf(buffer,"cache/%lu.%lu",hash,hash_second);
}

void acquire_cache_ref(void* ref) {
    if(ref == NULL) return;
    CacheNode* node = (CacheNode*)ref;
    atomic_fetch_add(&node->ref_count,1);
}

void release_cache_ref(void* ref) {
    if(ref == NULL) return;
    CacheNode* node = (CacheNode*)ref;
    atomic_fetch_sub(&node->ref_count,1);
    if(node->ref_count == 0 && node->purged) {
        free(node->response);
        node->response = NULL;
        free(node);
    }
    
}
void compact_registry() {
    pthread_mutex_lock(&registry_lock);

    if(!registry_file) {
        pthread_mutex_unlock(&registry_lock);
        return;
    }

    fclose(registry_file);
    
    FILE* old = fopen("cache_registry.bin","rb");
    FILE* new = fopen("cache_registry.tmp","wb");
    
    if(!old || !new) {
        if(old) fclose(old);
        if(new) {fclose(new); unlink("cache_registry.tmp"); }

        registry_file = fopen("cache_registry.bin","ab+");
        pthread_mutex_unlock(&registry_lock);
        return;
    }
    
    time_t now = time(NULL);
    RegistryRecord rec;
    int write_error = 0;
    while(fread(&rec,sizeof(RegistryRecord),1,old) == 1) {
        if(rec.action == 'A' && rec.expires_at > now) {
            if(fwrite(&rec,sizeof(RegistryRecord),1,new) != 1) {
                write_error = 1;
                break;
            }
        }
    }

    if(ferror(old)) {
        write_error = 1;
    }

    if(!write_error && fflush(new) != 0) {
        write_error = 1;
    }

    fclose(old);
    fclose(new);

    if(write_error) {
        unlink("cache_registry.tmp");
        log_event(LEVEL_ERROR,0,"[system]","Registry Compaction failed due to write error");
    } else {
        rename("cache_registry.tmp","cache_registry.bin");
    }

    registry_file = fopen("cache_registry.bin","ab+");
    pthread_mutex_unlock(&registry_lock);
}

void* persistence_worker(void* arg) {
    while(cache_running) {
        pthread_mutex_lock(&p_lock);

        while(p_count == 0 && cache_running) {
            pthread_cond_wait(&p_notify,&p_lock);
        }

        if(!cache_running && p_count == 0) {
            pthread_mutex_unlock(&p_lock);
            return NULL;
        }
        
        CacheNode* batch[PERSIST_BATCH_SIZE];
        int batch_size = 0;

        while(p_count > 0 && batch_size < PERSIST_BATCH_SIZE) {
            batch[batch_size] = persist_queue[p_head];
            p_head = (p_head + 1) % PERSIST_QUEUE_SIZE;
            p_count--;
            batch_size++;
        }

        pthread_mutex_unlock(&p_lock);

        time_t now = time(NULL);
        for(int j = 0; j < batch_size; j++) {
            CacheNode* target = batch[j];
            if(target->expires_at > now  && target->response != NULL) {
                char cache_file[256],temp_file[300];
                get_cache_filename(target->url,cache_file);
                snprintf(temp_file,sizeof(temp_file),"%s.tmp",cache_file);
                int fd = open(temp_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                int data_len = 0;
                while(data_len < target->response_len) {
                    int n = (target->response_len - data_len) > BUFFER ? BUFFER : (target->response_len - data_len);
                    int bytes_written = write(fd,target->response,n);
                    if(bytes_written < 0) {
                        close(fd);
                        unlink(temp_file);
                        continue;
                    }
                    data_len += bytes_written;
                }
                if(data_len < target->response_len) {
                    close(fd);
                    unlink(temp_file);
                } else {
                    if(rename(temp_file,cache_file) != 0) {
                        close(fd);
                        unlink(temp_file);
                        continue;
                    }
                    target->sync_to_disk = 1;
                    append_to_registry('A',target->url,target->expires_at,target->cached_at,target->response_len,target->upstream_header_len);
                }
            }
            atomic_fetch_sub(&target->ref_count,1);
        }
    }
    return NULL;
}

void* expiry_worker(void* arg) {
    while(cache_running) {
        sleep(global_config.expiry_interval);
        for(int i = 0; i < CACHE_BUCKETS; i++) {
            pthread_rwlock_wrlock(&cache_locks[i]);
            CacheNode* current = cache_table[i];
            CacheNode* prev = NULL;
            while(current != NULL) {
                if(current->expires_at < time(NULL) && !current->is_downloading && current->ref_count == 0) {
                    if(prev == NULL) {
                        cache_table[i] = current->next;
                    } else {
                        prev->next = current->next;
                    }
                    
                    if(current->memory_usage > 0) {
                        atomic_fetch_sub(&total_cache_memory,current->memory_usage);
                    }

                    char cache_file[256];
                    get_cache_filename(current->url,cache_file);
                    unlink(cache_file);
                    
                    free(current->response);
                    current->response = NULL;
                    free(current);
                    current = NULL;
                    if(prev == NULL) {
                        current = cache_table[i];
                    } else {
                        current = prev->next;
                    }
                } else {
                    prev = current;
                    current = current->next;
                }
            }
            pthread_rwlock_unlock(&cache_locks[i]);
        }
    
        compact_registry();
    }
}

int parse_cache_policy(char* response_buffer) {
    int ttl = global_config.default_ttl;// Default TTL
    
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

int filter_headers_to_buffer(char* header_block,int block_len,char* headers,int offset) {
    if (block_len < 0 || block_len >= BUFFER) return offset;
    char* save_ptr;
    char headers_copy[BUFFER + 1];
    memcpy(headers_copy,header_block,block_len);
    headers_copy[block_len] = '\0';

    char* line = strtok_r(headers_copy,"\r\n",&save_ptr);
    if(line) {
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    while(line != NULL) {
        if(strncasecmp(line,"Connection:",11) == 0 || 
           strncasecmp(line,"Keep-Alive:", 11) == 0 || 
           strncasecmp(line,"Transfer-Encoding:", 18) == 0 || 
           strncasecmp(line,"Upgrade:", 8) == 0 || 
           strncasecmp(line,"Content-Length:",15) == 0 || 
           strncasecmp(line,"Proxy-Connection:",17) == 0 || 
           strncasecmp(line, "Via:", 4) == 0 || 
           strncasecmp(line, "Age:", 4) == 0) {
            line = strtok_r(NULL,"\r\n",&save_ptr);
            continue;
        }
        offset += snprintf(headers + offset,BUFFER - offset,"%s\r\n",line);
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    offset += snprintf(headers + offset,BUFFER - offset,"\r\n");
    return offset;
}

static char* build_response(char* upstream_headers,int upstream_header_len,char* body,long body_size, long* out_len) {
    char hdr_buffer[BUFFER];
    
    int hdr_len = snprintf(hdr_buffer,sizeof(hdr_buffer),
        "HTTP/1.1 200 OK\r\n"
        "Connection: keep-alive\r\n"
        "Content-Length: %ld\r\n"
        "Via: 1.1 caching-proxy\r\n",body_size);

    if (upstream_header_len > 0) {
        memcpy(hdr_buffer + hdr_len, upstream_headers, upstream_header_len);
        hdr_len += upstream_header_len;
    }

    long total = hdr_len + body_size;
    char* resp = malloc(total + 1);
    if(!resp) return NULL;

    memcpy(resp,hdr_buffer,hdr_len);
    memcpy(resp + hdr_len,body,body_size);
    *out_len = total;
    return resp;
}

void bypass_cache_for_waiters(char* url,CacheNode* target) {
    unsigned long hash = hash_url(url);
    int bucket = hash % CACHE_BUCKETS;

    CacheNode *current = cache_table[bucket], *prev = NULL;
    pthread_rwlock_wrlock(&cache_locks[bucket]);
    while(current != NULL) {
        if(current == target) {

            ConnectionContext* temp_waiters[MAX_WAITERS];
            int temp_num_waiters = current->num_waiters;
            
            for(int i = 0; i < temp_num_waiters; i++) {
                temp_waiters[i] = current->waiters[i];
            }
            current->num_waiters = 0;
            atomic_fetch_add(&metric_cache_misses,temp_num_waiters);

            if(prev == NULL) {
                cache_table[bucket] = current->next;
            } else {
                prev->next = current->next;
            }
            
            if(current->memory_usage > 0) {
                atomic_fetch_sub(&total_cache_memory,current->memory_usage);
            }

            free(current->response);
            current->response = NULL;
            free(current);
            target = NULL;
            pthread_rwlock_unlock(&cache_locks[bucket]);

            for(int i = 0; i < temp_num_waiters; i++) {
                ConnectionContext* waiter = temp_waiters[i];

                pthread_mutex_lock(&waiter->state_lock);
                if(waiter->state != STATE_CLOSE) {
                    waiter->state = STATE_CONNECT_UPSTREAM;
                    signal_reactor_task(waiter->reactor_id, waiter);
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }
            return;
        }
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
}

void abort_cache_download(char* url,void* node_ref) {
    unsigned long hash = hash_url(url);
    int bucket = hash % CACHE_BUCKETS;

    pthread_rwlock_wrlock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    CacheNode* target = (CacheNode*)node_ref;
    CacheNode* prev = NULL;
    while(current != NULL) {
        if(current == target) {
            ConnectionContext* temp_waiters[MAX_WAITERS];
            int temp_num_waiters = 0;

            if(current->is_downloading) {
                temp_num_waiters = current->num_waiters;
                for(int i = 0; i < temp_num_waiters; i++) {
                    temp_waiters[i] = current->waiters[i];
                }
                current->num_waiters = 0;
            }

            if(prev == NULL) {
                cache_table[bucket] = current->next;
            } else {
                prev->next = current->next;
            }

            pthread_rwlock_unlock(&cache_locks[bucket]);
            free(current->response);
            current->response = NULL;
            free(current);
            current = NULL;
            node_ref = NULL;
            for(int i = 0; i < temp_num_waiters; i++) {
                ConnectionContext* waiter = temp_waiters[i];
                pthread_mutex_lock(&waiter->state_lock);
                if(waiter->state != STATE_CLOSE) {
                    waiter->write_len = snprintf(waiter->write_buf,sizeof(waiter->write_buf),
                    "HTTP/1.1 502 Bad Gateway\r\n"
                    "Content-Type:text/plain\r\n"
                    "Connection:close\r\n"
                    "\r\n"
                    "Upstream Server failed to provide the file");

                    waiter->write_offset = 0;
                    waiter->keep_alive = 0;

                    waiter->state = STATE_SEND_RESPONSE_HEADERS;
                    signal_reactor_task(waiter->reactor_id, waiter);
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }

            char cache_file[256];
            get_cache_filename(url,cache_file);
            unlink(cache_file);
            return;
        } 
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
}

int add_to_cache_ram(ConnectionContext* ctx, char* url, time_t expires_at, time_t cached_at, char* upstream_headers, int upstream_header_len, char* body, long body_size) {
    if(body_size <= 0)  {
        return -1;
    }

    CacheNode* current = NULL;
    if(ctx != NULL && ctx->cache_ref != NULL) {
        current = (CacheNode*)ctx->cache_ref;
    }

    unsigned long hash = hash_url(url);
    int bucket = hash % CACHE_BUCKETS;
    
    approximate_evict_if_needed();
    
    if(ctx == NULL) {
        current = cache_table[bucket];
        while(current != NULL) {
            if(strcmp(current->url,url) == 0) {
                break;
            }
            current = current->next;
        }
    }
    pthread_rwlock_wrlock(&cache_locks[bucket]);
    
    if(current != NULL) {
            current->expires_at = expires_at;
            current->cached_at = cached_at;
            current->body_size = body_size;
            current->upstream_header_len = upstream_header_len;
            current->is_downloading = 0;

            free(current->response);
            current->response = NULL;
            current->response_len = 0;
            current->is_large = 0;
         
            if(body_size < global_config.large_file_threshold && body != NULL) {
                long resp_len = 0;
                current->response = build_response(upstream_headers,upstream_header_len,body,body_size,&resp_len);
                current->response_len = resp_len;
                current->is_large = 0;
                current->memory_usage = resp_len;
                current->sync_to_disk = 0;

                if(ctx != NULL) {
                    ctx->send_mem_buf = current->response;
                    ctx->send_mem_len = resp_len;
                    ctx->send_mem_offset = current->response_len - current->body_size;
                }
                
                pthread_mutex_lock(&p_lock);
                if(p_count < PERSIST_QUEUE_SIZE) {
                    persist_queue[p_tail] = current;
                    p_tail = (p_tail + 1) % PERSIST_QUEUE_SIZE;
                    p_count++;
                    atomic_fetch_add(&current->ref_count,1);
                    pthread_cond_signal(&p_notify);
                } else {
                    log_event(LEVEL_WARN, 0, "[system]", "Persistence queue filled to 8192! Dropping disk sync to prevent network IO crash.");
                }
                pthread_mutex_unlock(&p_lock);
               atomic_store(&current->last_accessed,time(NULL));
               atomic_fetch_add(&total_cache_memory,current->memory_usage);
            } else {
                current->is_large = 1;
                current->memory_usage = 0;
            }

            ConnectionContext* temp_waiters[MAX_WAITERS];
            int temp_num_waiters = current->num_waiters;
            for(int i = 0; i < temp_num_waiters; i++) {
                temp_waiters[i] = current->waiters[i];
            }

            current->num_waiters = 0;
            pthread_rwlock_unlock(&cache_locks[bucket]);

            for(int i = 0; i < temp_num_waiters; i++) {
                ConnectionContext* waiter = temp_waiters[i];
                pthread_mutex_lock(&waiter->state_lock);
                if(waiter->state != STATE_CLOSE) {
                    waiter->state = STATE_CHECK_CACHE;
                    signal_reactor_task(waiter->reactor_id, waiter);
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }
            return 1;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    return -1;
}

int check_cache_ram(ConnectionContext* ctx) {
    unsigned long hash = hash_url(ctx->url);
    int bucket = hash % CACHE_BUCKETS;
    
    pthread_rwlock_rdlock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    CacheNode* prev = NULL;
    while(current != NULL) {
        if(strcmp(ctx->url,current->url) == 0) {
            if(!current->is_downloading && current->expires_at > time(NULL) ) {
                if(!current->is_large && current->response != NULL) {
                    ctx->send_mem_buf = current->response;
                    ctx->send_mem_len = current->response_len;
                    ctx->send_mem_offset = current->response_len - current->body_size;
                    ctx->cached_at = current->cached_at;
                    atomic_store(&current->last_accessed,time(NULL));
                    atomic_fetch_add(&metric_cache_hits,1);
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 1; // Cache Hit
                } else if(current->is_large) {
                    ctx->bytes_remaining = current->body_size;
                    ctx->upstream_header_len = current->upstream_header_len;
                    ctx->send_mem_buf = NULL;
                    ctx->cached_at = current->cached_at;
                    atomic_fetch_add(&metric_cache_hits,1);
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 1; // Cache Hit
                }
            }
            break;
        }
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    
    pthread_rwlock_wrlock(&cache_locks[bucket]);
    while(current != NULL) {
       if(strcmp(ctx->url,current->url) == 0) {
            //Recheck in write lock
            if(!current->is_downloading && current->expires_at > time(NULL) ) {
                if(!current->is_large && current->response != NULL) {
                        ctx->send_mem_buf = current->response;
                        ctx->send_mem_len = current->response_len;
                        ctx->send_mem_offset = current->response_len - current->body_size;
                        ctx->cached_at = current->cached_at;
                        ctx->cache_ref = current;
                        atomic_store(&current->last_accessed,time(NULL));
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        atomic_fetch_add(&metric_cache_hits,1);
                        return 1; // Cache Hit
                } else {
                    ctx->bytes_remaining = current->body_size;
                    ctx->upstream_header_len = current->upstream_header_len;
                    ctx->send_mem_buf = NULL;
                    ctx->cache_ref = current;
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    atomic_fetch_add(&metric_cache_hits,1);
                    return 1; // Cache Hit
                }
            }

            if(current->is_downloading) {
                if(current->num_waiters < MAX_WAITERS) {
                    current->waiters[current->num_waiters++] = ctx;
                    atomic_fetch_add(&ctx->active_threads,1);
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 2;
                }
                atomic_fetch_add(&metric_cache_misses,1);
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return 0;
            }

            if(current->expires_at < time(NULL)) {

                if(current->ref_count > 0) {
                    current->purged = 1;

                    if(prev == NULL) cache_table[bucket] = current->next;
                    else prev->next = current->next;
                    current->next = NULL;

                    break;
                }
                current->is_downloading = 1;
                current->num_waiters = 0;

                if(current->memory_usage > 0) {
                    atomic_fetch_sub(&total_cache_memory,current->memory_usage);
                }

                free(current->response);
                current->response = NULL;
                current->response_len = 0;
                current->memory_usage = 0;
                
                ctx->cache_ref = current;
                atomic_fetch_add(&metric_cache_misses,1);
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return -1; // Expired
            }
       } 
       prev = current;
       current = current->next;
    }

    CacheNode* new_node = calloc(1,sizeof(CacheNode));
    if(!new_node) {
        pthread_rwlock_unlock(&cache_locks[bucket]);
        return 0;
    }
    strncpy(new_node->url,ctx->url,sizeof(new_node->url) - 1);
    new_node->url[sizeof(new_node->url) - 1] = '\0';
    new_node->is_downloading = 1;
    new_node->expires_at = 0;
    new_node->upstream_header_len = 0;
    new_node->is_large = 0;
    new_node->num_waiters = 0;
    new_node->purged = 0;
    new_node->next = cache_table[bucket];
    cache_table[bucket] = new_node;
    ctx->cache_ref = new_node;
    
    ctx->is_designated_downloader = 1;
    atomic_fetch_add(&metric_cache_misses,1);
    pthread_rwlock_unlock(&cache_locks[bucket]);
    return 0; // Cache Miss;
}

int check_cache(ConnectionContext* ctx) {
    int status = check_cache_ram(ctx);
    
    if(status == 1 && ctx->send_mem_buf != NULL) return 1;
    else if(status == 1 && ctx->send_mem_buf == NULL) {
        char cache_file[256];
        get_cache_filename(ctx->url,cache_file);

        ctx->file_fd = open(cache_file,O_RDONLY);
        if(ctx->file_fd < 0) return 0;
        return 1;
    } else if(status == 2) return -1;

    return 0;
}

void handle_check_cache(ConnectionContext* ctx) {
    
    if(ctx->checkCache) {
        int isHit = check_cache(ctx);
        if(isHit == 0) {
            //CACHE MISS
            ctx->state = STATE_CONNECT_UPSTREAM;
            return;
        } else if(isHit == -1) {
            ctx->state = STATE_WAIT_CACHE;
            return;
        }
    }
    int cork = 1;
    setsockopt(ctx->client_fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof(cork));
    //CACHE HIT
    acquire_cache_ref(ctx->cache_ref);

    if(ctx->send_mem_buf != NULL) {
        ctx->write_len = 0;
        ctx->write_offset = 0;
        int offset = ctx->send_mem_offset;
        memcpy(ctx->write_buf,ctx->send_mem_buf,offset);
        ctx->write_len = offset;

        if(ctx->checkCache) {
            ctx->write_len -= 2;
            long age_seconds = time(NULL) - ctx->cached_at;
            ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,
                          "X-Cache: HIT\r\n"
                          "Age:%ld\r\n"
                          "\r\n",age_seconds);
        }

        ctx->state = STATE_SEND_RESPONSE_HEADERS;
        return;
    }
    
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->write_len += snprintf(ctx->write_buf, sizeof(ctx->write_buf),
            "HTTP/1.1 200 OK\r\n"
            "Connection: keep-alive\r\n"
            "Content-Length: %ld\r\n"
            "Via: 1.1 c_proxy\r\n",
            ctx->bytes_remaining);
    
    if(ctx->checkCache) {
        long age_seconds = time(NULL) - ctx->cached_at;
        ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,
                          "X-Cache: HIT\r\n"
                          "Age:%ld\r\n",age_seconds);
    }
    
    char file_buffer[BUFFER];
    read(ctx->file_fd,file_buffer,sizeof(file_buffer));
    memcpy(ctx->write_buf + ctx->write_len, file_buffer + sizeof(CacheHeader), ctx->upstream_header_len);
    ctx->write_len += ctx->upstream_header_len;
    long body_offset = sizeof(CacheHeader) + ctx->upstream_header_len;
    ctx->send_mem_buf = NULL;
    lseek(ctx->file_fd,body_offset,SEEK_SET);
    ctx->state = STATE_SEND_RESPONSE_HEADERS;
    return;
}

void handle_send_cache(ConnectionContext *ctx) {
    if(ctx->send_mem_buf != NULL) {
        while(ctx->send_mem_offset < ctx->send_mem_len) {
            long bytes_sent = send(ctx->client_fd,ctx->send_mem_buf + ctx->send_mem_offset, 
                                 ctx->send_mem_len - ctx->send_mem_offset,MSG_NOSIGNAL);
            if(bytes_sent < 0) {
                if(errno == EWOULDBLOCK || errno == EAGAIN) {
                    return;
                }

                if(errno == EPIPE || errno == ECONNRESET) {
                    ctx->state = STATE_CLOSE;
                    return;
                }
                perror("send failure");
                ctx->state = STATE_CLOSE;
                return;
            } else if(bytes_sent == 0) {
                ctx->state = STATE_CLOSE;
                return;
            } 
            atomic_store(&ctx->last_active,time(NULL));
            ctx->send_mem_offset += bytes_sent;
        }
        int cork = 0;
        setsockopt(ctx->client_fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof(cork));

        ctx->send_mem_buf = NULL;
        ctx->send_mem_len = 0;
        ctx->send_mem_offset = 0;
    } else {
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
            atomic_store(&ctx->last_active,time(NULL));
            ctx->bytes_remaining -= bytes_sent;
        }

        close(ctx->file_fd);
        ctx->file_fd = -1;

        int cork = 0;
        setsockopt(ctx->client_fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof(cork));
    }
    
    release_cache_ref(ctx->cache_ref);
    ctx->cache_ref = NULL;

    if(ctx->keep_alive) {
        ctx->bytes_read = 0;

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
        ctx->read_buf[0] = '\0';
        ctx->method[0] = '\0';
        ctx->url[0] = '\0';
        ctx->protocol[0] = '\0';
        ctx->req.hostname[0] = '\0';
        ctx->req.path[0] = '\0';
        ctx->state = STATE_READ_REQUEST;
    } else {
        ctx->state = STATE_CLOSE;
    }
}

void handle_fetch_upstream(ConnectionContext* ctx) {
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

                int current_errors = atomic_fetch_add(&consecutive_upstream_errors, 1) + 1;
                log_event(LEVEL_ERROR, ctx->req_id, ctx->client_ip, "Origin recv() error. Strike issued.");

                ctx->state = STATE_CLOSE;
                return;
            } else if(n == 0) {

                if (ctx->upstream_headers_parsed == 0) {
                    int current_errors = atomic_fetch_add(&consecutive_upstream_errors, 1) + 1;
                    log_event(LEVEL_ERROR, ctx->req_id, ctx->client_ip, "Origin hung up early. Strike issued.");
                }

                upstream_dropped = 1;
                break;
            }

            if(ctx->upstream_header_len == 0 && n > 0) {
                struct timeval now;
                gettimeofday(&now,NULL);
                
                atomic_store(&consecutive_upstream_errors,0);
                uint64_t time_elapsed_us = (now.tv_sec - ctx->upstream_send_time.tv_sec) * 1000000ULL + (now.tv_usec - ctx->upstream_send_time.tv_usec);

                uint64_t current_ema = atomic_load(&global_upstream_latency_us);
                uint64_t new_ema = (current_ema * 9 + time_elapsed_us) / 10;
                atomic_store(&global_upstream_latency_us,new_ema);
            }

            atomic_store(&ctx->last_active,time(NULL));
            ctx->upstream_header_len += n;
            ctx->upstream_header_buf[ctx->upstream_header_len] = '\0';
            char* body_ptr = strstr(ctx->upstream_header_buf,"\r\n\r\n");
            if(body_ptr != NULL) {
                body_ptr += 4;
                char* cc_length = strcasestr(ctx->upstream_header_buf,"Content-Length:");
                if(cc_length) ctx->upstream_content_length = atoi(cc_length + 15);
                
                char* te_header = strcasestr(ctx->upstream_header_buf,"Transfer-Encoding:");
                if(te_header && strstr(te_header,"chunked")) {
                    ctx->is_chunked = 1;
                    ctx->chunk_state = 0;
                    ctx->current_chunk_bytes_read = 0;
                    ctx->hex_idx = 0;
                }
                
                if (strcasestr((ctx->upstream_header_buf), "Connection: close") == 0 || 
                    strstr(ctx->upstream_header_buf, "HTTP/1.0")) {
                    upstream_dropped = 1; 
                }
                ctx->cache_ttl = parse_cache_policy(ctx->upstream_header_buf);

                if(ctx->upstream_content_length > global_config.large_file_threshold) {
                    char cache_file[256],temp_file[300];
                    get_cache_filename(ctx->url,cache_file);
                    snprintf(temp_file,sizeof(temp_file),"%s.tmp.%d",cache_file,ctx->client_fd);
                    ctx->file_fd = open(temp_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                } else {
                    ctx->file_fd = memfd_create("data-stream",0);

                    if(ctx->file_fd < 0) {
                        FILE* temp = tmpfile();
                        if(temp) {
                            ctx->file_fd = dup(fileno(temp));
                            fclose(temp);
                        }
                    }
                }
                
                if(ctx->file_fd < 0) {
                    perror("Denied File Descriptor Allocation");
                    ctx->state = STATE_CLOSE;
                    return;
                }
                CacheHeader dummyHeader;
                memset(&dummyHeader,0,sizeof(dummyHeader));
                write(ctx->file_fd,&dummyHeader,sizeof(dummyHeader));

                int headers_len = body_ptr - ctx->upstream_header_buf;
                ctx->header_overshoot_len = ctx->upstream_header_len - headers_len;
                
                char filtered_headers[BUFFER];
                int filtered_len = filter_headers_to_buffer(ctx->upstream_header_buf, headers_len, filtered_headers, 0);
                
                write(ctx->file_fd, filtered_headers, filtered_len);
                ctx->upstream_header_len = filtered_len;

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
                atomic_store(&ctx->last_active,time(NULL));
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
                        
                        if(!ctx->is_spooled_disk) {
                            if((ctx->upstream_header_len + ctx->upstream_body_downloaded) > global_config.large_file_threshold) {
                                char cache_file[256],temp_file[300];
                                get_cache_filename(ctx->url,cache_file);
                                snprintf(temp_file,sizeof(temp_file),"%s.tmp.%d",cache_file,ctx->client_fd);
                                
                                char log_buf[128];
                                snprintf(log_buf,sizeof(log_buf),"Large File Requested Switching to disk transfer");
                                log_event(LEVEL_INFO,ctx->req_id,ctx->client_ip,log_buf);

                                int physical_fd = open(temp_file,O_WRONLY | O_CREAT | O_TRUNC, 0644);
                                if(physical_fd >= 0) { 
                                    
                                    lseek(ctx->file_fd,0,SEEK_SET);
                                    char copy_buffer[BUFFER];
                                    long bytes_r = 0;
                                    int io_error = 0;
                                    while((bytes_r = read(ctx->file_fd,copy_buffer,sizeof(copy_buffer))) > 0) {
                                        if(write(physical_fd,copy_buffer,bytes_r) != bytes_r) {
                                            io_error = 1;
                                            break;
                                        }
                                    }
                                    
                                    if(bytes_r < 0) {
                                        io_error = 1;
                                    }

                                    if(io_error) {
                                        char log_buf[128];
                                        snprintf(log_buf,sizeof(log_buf),"I/O Error while switching to disk transfer");
                                        log_event(LEVEL_ERROR,ctx->req_id,ctx->client_ip,log_buf);
                                        unlink(temp_file);
                                        close(physical_fd);
                                        ctx->state = STATE_CLOSE;
                                        return;
                                    }

                                    close(ctx->file_fd);
                                    ctx->file_fd = physical_fd;
                                    ctx->is_spooled_disk = 1;
                                }
                            }
                        }

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
    atomic_store(&context_table[ctx->upstream_fd],NULL);
    
    if(upstream_dropped) {
        atomic_fetch_sub(&active_upstream_connections,1);
        close(ctx->upstream_fd);
    } else {
        stash_connection(ctx->upstream_fd,(ctx->req).hostname,(ctx->req).port);
    }
    ctx->upstream_fd = -1;

    char cache_file[256],temp_file[300];
    get_cache_filename(ctx->url,cache_file);
    snprintf(temp_file, sizeof(temp_file), "%s.tmp.%d", cache_file, ctx->client_fd);
    
    if(!ctx->upstream_headers_parsed) {
        if(ctx->file_fd != -1) {
            close(ctx->file_fd);
            ctx->file_fd = -1;
            unlink(temp_file);
        }
        ctx->state = STATE_CLOSE;
        return;
    }

    
    if(ctx->upstream_content_length > 0 && ctx->upstream_body_downloaded < ctx->upstream_content_length) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
        unlink(temp_file);
        ctx->state = STATE_CLOSE;
        return;
    }
    
    if(ctx->is_chunked && ctx->chunk_state != 3) {
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
    finalHeader.cached_at = time(NULL);

    struct stat st;
    fstat(ctx->file_fd,&st);
    finalHeader.content_length = st.st_size - sizeof(CacheHeader);
    ctx->bytes_remaining = finalHeader.content_length - ctx->upstream_header_len;
    finalHeader.upstream_header_len = ctx->upstream_header_len;
    
    long body_size = finalHeader.content_length - ctx->upstream_header_len;

    lseek(ctx->file_fd,0,SEEK_SET);
    write(ctx->file_fd,&finalHeader,sizeof(finalHeader));


    if(ctx->cache_ttl <= 0) {
        
        lseek(ctx->file_fd, 0, SEEK_SET);

        if(ctx->is_designated_downloader) {
            bypass_cache_for_waiters(ctx->url,(CacheNode*)ctx->cache_ref);
            ctx->is_designated_downloader = 0;
        }

        ctx->send_mem_buf = NULL;
        ctx->checkCache = 0;
        ctx->state = STATE_CHECK_CACHE;
        return;
    }
    
    int read_fd = -1;
    if(ctx->upstream_body_downloaded >= global_config.large_file_threshold) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
        if(rename(temp_file,cache_file) != 0) {
            ctx->state = STATE_CLOSE;
        }

        read_fd = open(cache_file,O_RDONLY);
        if(read_fd < 0) {ctx->state = STATE_CLOSE; return;}
    } else {
        read_fd = ctx->file_fd;
        ctx->file_fd = -1;
    }


    char file_buf[BUFFER];
    lseek(read_fd,sizeof(CacheHeader),SEEK_SET);
    int hdr_read = read(read_fd,file_buf,ctx->upstream_header_len);
    if(hdr_read < ctx->upstream_header_len) {
        ctx->state = STATE_CLOSE;
        close(read_fd);
        read_fd = -1;
        return;
    }
    
    if(body_size < global_config.large_file_threshold) {
        char* body_buf = malloc(body_size);
        if(body_buf) {
            long total_read = 0;
            while(total_read < body_size) {
                int r = read(read_fd,body_buf + total_read, body_size - total_read);
                if(r <= 0) { 
                    close(read_fd);
                    free(body_buf);
                    ctx->state = STATE_CLOSE; 
                    return;
                }
                total_read += r;
            }
            int cache_status = add_to_cache_ram(ctx,ctx->url, finalHeader.expires_at,finalHeader.cached_at,file_buf, ctx->upstream_header_len,body_buf, body_size);
            if(cache_status <= 0) {
                ctx->state = STATE_CLOSE;
                return;
            }
            ctx->is_designated_downloader = 0;
            free(body_buf);
        }
        close(read_fd);

        ctx->checkCache = 0;
        ctx->state = STATE_CHECK_CACHE;
        return;
    } else {
        add_to_cache_ram(NULL,ctx->url, finalHeader.expires_at,finalHeader.cached_at,file_buf, ctx->upstream_header_len,NULL, body_size);
        ctx->is_designated_downloader = 0;
        append_to_registry('A', ctx->url, finalHeader.expires_at,finalHeader.cached_at,finalHeader.content_length, ctx->upstream_header_len);
        close(read_fd);

        ctx->file_fd = open(cache_file,O_RDONLY);
        if(ctx->file_fd < 0) { ctx->state = STATE_CLOSE;  return; }
        lseek(ctx->file_fd, sizeof(CacheHeader), SEEK_SET);
        ctx->checkCache = 0;
        ctx->send_mem_buf = NULL;
        ctx->state = STATE_CHECK_CACHE;
        return;
    }
}

void rehydrate_cache() {

    FILE* reg = fopen("cache_registry.bin","rb+");

    if(reg) {
        fseek(reg,0,SEEK_END);
        long file_size = ftell(reg);
        rewind(reg);

        if(file_size > 0) {
            RegistryRecord rec;
            while(fread(&rec,sizeof(RegistryRecord),1,reg) == 1) {
                char cache_file[256];
                get_cache_filename(rec.url,cache_file);
                if(rec.expires_at > time(NULL) && rec.action == 'A') {
                    int read_fd = open(cache_file,O_RDONLY);
                    if(read_fd < 0) continue;
                    
                    char hdr_buf[BUFFER];
                    
                    // SECURITY GUARD against FORTIFY_SOURCE stack buffer overflow
                    if(rec.upstream_header_len > BUFFER || rec.upstream_header_len < 0) {
                        fprintf(stderr, "[rehydrate] Fatal: registry header length %d exceeds BUFFER %d! Skipping node.\n", 
                                rec.upstream_header_len, BUFFER);
                        close(read_fd);
                        continue;
                    }
                    
                    lseek(read_fd,sizeof(CacheHeader),SEEK_SET);
                    int hdr_len = read(read_fd,hdr_buf,rec.upstream_header_len);
                    if(hdr_len > 0 && hdr_len < rec.upstream_header_len) {
                        close(read_fd);
                        continue;
                    }

                    long body_size = rec.content_length - rec.upstream_header_len;
                    if(body_size > 0 && body_size < global_config.large_file_threshold) {
                        char* body = malloc(body_size);
                        if(body) {
                           long total_read = 0;
                            while(total_read < body_size) {
                                int r = read(read_fd,body + total_read,body_size - total_read);
                                if(r <= 0) {
                                    close(read_fd);
                                    free(body);
                                    total_read = 0;
                                    break;
                                }
                                total_read += r;
                            }
                            if(total_read == 0)  {
                                close(read_fd);
                                continue;
                            }
                        }

                        unsigned long hash = hash_url(rec.url);
                        int bucket = hash % CACHE_BUCKETS;

                        pthread_rwlock_wrlock(&cache_locks[bucket]);
                        CacheNode* node = calloc(1,sizeof(CacheNode));
                        if(node) {
                            strncpy(node->url,rec.url,sizeof(rec.url) - 1);
                            node->is_downloading = 1;
                            node->num_waiters = 0;
                            node->next = cache_table[bucket];
                            cache_table[bucket] = node;
                        } else {
                            pthread_rwlock_unlock(&cache_locks[bucket]);
                            continue;
                        }

                        pthread_rwlock_unlock(&cache_locks[bucket]);

                        add_to_cache_ram(NULL,rec.url, rec.expires_at,rec.cached_at,hdr_buf, hdr_len, body, body_size);
                        free(body);
                    } else {
                        unsigned long hash = hash_url(rec.url);
                        int bucket = hash % CACHE_BUCKETS;
                        pthread_rwlock_wrlock(&cache_locks[bucket]);
                        CacheNode* node = calloc(1,sizeof(CacheNode));
                        if(node) {
                            strncpy(node->url,rec.url,sizeof(rec.url) - 1);
                            node->is_downloading = 1;
                            node->num_waiters = 0;
                            node->next = cache_table[bucket];
                            cache_table[bucket] = node;
                        } else {
                            pthread_rwlock_unlock(&cache_locks[bucket]);
                            continue;
                        }

                        pthread_rwlock_unlock(&cache_locks[bucket]);

                        add_to_cache_ram(NULL,rec.url, rec.expires_at,rec.cached_at,hdr_buf, hdr_len, NULL, body_size);
                    }
                    close(read_fd);
                } else {
                    unlink(cache_file);
                }
            }
            fclose(reg);
            return;
        }
        fclose(reg);
    }

    DIR* dir = opendir("cache");
    if(!dir) return;

    struct dirent *entry;
    while((entry = readdir(dir)) != NULL) {
        if(entry->d_name[0] == '.') continue;

        char filepath[1024];
        snprintf(filepath,sizeof(filepath),"cache/%s",entry->d_name);

        if(strstr(entry->d_name,"tmp")) {
            unlink(filepath);
            continue;
        }

        int read_fd = open(filepath,O_RDONLY);
        if(read_fd < 0) continue;

        CacheHeader header;
        if(read(read_fd,&header,sizeof(CacheHeader)) == sizeof(CacheHeader)) {
            if(header.expires_at > time(NULL)) {
                char hdr_buf[BUFFER];
                int hdr_len = read(read_fd,hdr_buf,header.upstream_header_len);
                if(hdr_len > 0 && hdr_len < header.upstream_header_len) {
                    close(read_fd);
                    continue;
                }

                long body_size = header.content_length - header.upstream_header_len;
                if(body_size > 0 && body_size < global_config.large_file_threshold) {
                    char* body = malloc(body_size);
                    if(body) {
                        long total_read = 0;
                        while(total_read < body_size) {
                            int r = read(read_fd,body + total_read,body_size - total_read);
                            if(r <= 0) {
                                close(read_fd);
                                free(body);
                                total_read = 0;
                                break;
                            }
                            total_read += r;
                        }
                        if(total_read == 0) break;
                    }

                    unsigned long hash = hash_url(header.url);
                    int bucket = hash % CACHE_BUCKETS;

                    pthread_rwlock_wrlock(&cache_locks[bucket]);
                    CacheNode* node = calloc(1,sizeof(CacheNode));
                    if(node) {
                        strncpy(node->url,header.url,sizeof(header.url) - 1);
                        node->is_downloading = 1;
                        node->next = cache_table[bucket];
                        cache_table[bucket] = node;
                    } else {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        continue;
                    }

                    pthread_rwlock_unlock(&cache_locks[bucket]);

                    add_to_cache_ram(NULL,header.url, header.expires_at,header.cached_at,hdr_buf, hdr_len, body, body_size);
                    free(body);
                } else {
                    unsigned long hash = hash_url(header.url);
                    int bucket = hash % CACHE_BUCKETS;
                    pthread_rwlock_wrlock(&cache_locks[bucket]);
                    CacheNode* node = calloc(1,sizeof(CacheNode));
                    if(node) {
                        strncpy(node->url,header.url,sizeof(header.url) - 1);
                        node->is_downloading = 1;
                        node->next = cache_table[bucket];
                        cache_table[bucket] = node;
                    } else {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        continue;
                    }

                    pthread_rwlock_unlock(&cache_locks[bucket]);

                    add_to_cache_ram(NULL,header.url, header.expires_at,header.cached_at,hdr_buf, hdr_len,NULL, body_size);
                }
                append_to_registry('A', header.url, header.expires_at,header.cached_at,header.content_length, header.upstream_header_len);
            } else {
                unlink(filepath);
            }
        }
        close(read_fd);
    }
    closedir(dir);
}


int purge_cache_entry(char* url) {
    unsigned long hash = hash_url(url);
    unsigned long hash_second = hash_url_second(url);
    int bucket = hash % CACHE_BUCKETS;
    
    pthread_rwlock_wrlock(&cache_locks[bucket]);

    CacheNode* current = cache_table[bucket];
    CacheNode* prev = NULL;
    while(current != NULL) {
        if(strcmp(current->url,url) == 0) {
            if(current->is_downloading) {
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return 409;
            }
            
            if(prev == NULL) {
                cache_table[bucket] = current->next;
            } else {
                prev->next = current->next;
            }
            
            if(current->ref_count == 0) {
                free(current->response);
                current->response = NULL;
                free(current);
                current = NULL;
            } else {
                current->next = NULL;
                current->purged = 1;
            }

            char cache_file[256];
            snprintf(cache_file,sizeof(cache_file),"cache/%lu.%lu",hash,hash_second);
            unlink(cache_file);
            pthread_rwlock_unlock(&cache_locks[bucket]);
            return 200;
        }
        prev = current;
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    return 404;
}