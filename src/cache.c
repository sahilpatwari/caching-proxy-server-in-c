#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<time.h>
#include<stdint.h>
#include<sys/time.h>
#include<dirent.h>
#include<unistd.h>
#include<fcntl.h>


#include"proxy.h"
#include"cache.h"
#include"config.h"
#include"connection.h"
#include"reactor.h"
#include"workers.h"

#define CACHE_BUCKETS        8192
#define MAX_WAITERS          8192
#define PERSIST_QUEUE_SIZE   8192
#define PERSIST_BATCH_SIZE   1024
#define GRACE_WINDOW 30

void* expiry_worker(void*);
void* persistence_worker(void*);


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
    int is_evicted;

    char etag[256];
    int revalidating;
}CacheNode;

static CacheNode* cache_table[CACHE_BUCKETS];
static pthread_rwlock_t cache_locks[CACHE_BUCKETS];

_Atomic long total_cache_memory = 0;

static pthread_t expiry_thread;
static pthread_t persistence_worker_thread;
static volatile int  cache_running;

static pthread_mutex_t eviction_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t eviction_condition = PTHREAD_COND_INITIALIZER;
static pthread_t eviction_thread;
static long eviction_soft_limit = 0;
static long eviction_target = 0;

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


static void approximate_evict_if_needed() {
    //Approximate LRU Logic
    unsigned int seed = (unsigned int)(uintptr_t)pthread_self() ^ (unsigned int)clock();

    while(atomic_load(&total_cache_memory) >= (size_t)eviction_target) {
        CacheNode* best_victim = NULL;
        int best_bucket = -1;
        time_t oldest_time = time(NULL) + 1;

        for(int i = 0; i < 32; i++) {
            int bucket = rand_r(&seed) % CACHE_BUCKETS;
            pthread_rwlock_rdlock(&cache_locks[bucket]);
            CacheNode* current = cache_table[bucket];
            while(current) {
                if(!current->is_large && !current->is_downloading && current->response != NULL && current->ref_count == 0) {
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
        
        if(best_victim == NULL && best_bucket == -1) break;

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

        if(found && !best_victim->is_downloading && best_victim->ref_count == 0) {
            long mem_usage = best_victim->memory_usage;
            atomic_fetch_sub(&total_cache_memory,mem_usage);
            free(best_victim->response);
            best_victim->response = NULL;
            best_victim->response_len = 0;
            best_victim->memory_usage = 0;
            if(best_victim->sync_to_disk) {
                best_victim->is_large = 1;
            } else {
                best_victim->is_evicted = 1;
            }
        }
        pthread_rwlock_unlock(&cache_locks[best_bucket]);
    }
}

void* eviction_worker(void* args) {
    pthread_mutex_lock(&eviction_lock);
    while(cache_running) {
        while(atomic_load(&total_cache_memory) < eviction_soft_limit && cache_running) {
            pthread_cond_wait(&eviction_condition,&eviction_lock);
        }
        if(!cache_running) break;
        pthread_mutex_unlock(&eviction_lock);

        approximate_evict_if_needed();

        pthread_mutex_lock(&eviction_lock);
    }
    pthread_mutex_unlock(&eviction_lock);
    return NULL;    
}

void init_cache() {
    for(int i = 0; i < CACHE_BUCKETS; i++) {
        cache_table[i] = NULL;
        pthread_rwlock_init(&cache_locks[i],NULL);
    }
    atomic_store(&total_cache_memory,0);
    
    cache_running = 1;
    eviction_soft_limit = global_config.max_cache_mem * 0.8;
    eviction_target = global_config.max_cache_mem * 0.65;
    pthread_create(&expiry_thread,NULL,expiry_worker,NULL);
    pthread_create(&persistence_worker_thread,NULL,persistence_worker,NULL);
    pthread_create(&eviction_thread,NULL,eviction_worker,NULL);
}

void destroy_cache() {
    cache_running = 0;
    pthread_cancel(expiry_thread);
    pthread_join(expiry_thread,NULL);
    
    pthread_cond_broadcast(&p_notify);
    pthread_cancel(persistence_worker_thread);
    pthread_join(persistence_worker_thread,NULL);

    pthread_cond_broadcast(&eviction_condition);
    pthread_cancel(eviction_thread);
    pthread_join(eviction_thread,NULL);

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
}

void get_cache_filename(char* url, char* buffer) {
    unsigned long hash = hash_url(url);
    unsigned long hash_second = hash_url_second(url);
    snprintf(buffer, 256, "cache/%lu.%lu", hash, hash_second);
}

void acquire_cache_ref(void* ref) {
    if(ref == NULL) return;
    CacheNode* node = (CacheNode*)ref;
    atomic_fetch_add(&node->ref_count,1);
}

void release_cache_ref(void* ref) {
    if(ref == NULL) return;
    CacheNode* node = (CacheNode*)ref;
    int old = atomic_fetch_sub(&node->ref_count,1);
    if(old == 1 && node->purged) {
        free(node->response);
        node->response = NULL;
        free(node);
    }
    
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

                CacheHeader header;
                memset(&header,0,sizeof(CacheHeader));
                strncpy(header.url,target->url,sizeof(header.url) - 1);
                header.url[sizeof(header.url) - 1] = '\0';
                if(strlen(target->etag) > 0) {
                    strncpy(header.etag,target->etag,sizeof(header.etag) - 1);
                    header.etag[sizeof(header.etag) - 1] = '\0';
                }
                header.expires_at = target->expires_at;
                header.cached_at = target->cached_at;
                header.upstream_header_len = target->upstream_header_len;
                header.content_length = target->body_size + target->upstream_header_len;

                if(write(fd,&header,sizeof(CacheHeader)) != sizeof(CacheHeader)) {
                    close(fd);
                    unlink(temp_file);
                    continue;
                }
                int data_len = 0;
                while(data_len < target->response_len) {
                    int n = (target->response_len - data_len) > BUFFER ? BUFFER : (target->response_len - data_len);
                    int bytes_written = write(fd,target->response + data_len,n);
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
    }
    return NULL;
}

void bypass_cache_for_waiters(char* url,void* node) {
    unsigned long hash = hash_url(url);
    int bucket = hash % CACHE_BUCKETS;
    
    pthread_rwlock_wrlock(&cache_locks[bucket]);
    CacheNode* target = (CacheNode*)node;
    CacheNode *current = cache_table[bucket], *prev = NULL;
    while(current != NULL) {
        if(current == target) {

            ConnectionContext* temp_waiters[MAX_WAITERS];
            int temp_num_waiters = current->num_waiters;
            
            int waiters_per_core = MAX_WAITERS / NUM_REACTORS;
            ConnectionContext* wakeup_waiters[NUM_REACTORS][waiters_per_core];
            int waiter_count[NUM_REACTORS];
            memset(waiter_count,0,sizeof(waiter_count));

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
                    int r_id = waiter->reactor_id;
                    wakeup_waiters[r_id][waiter_count[r_id]++] = waiter;
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }

            for(int i = 0; i < NUM_REACTORS; i++) {
                if(waiter_count[i] > 0) {
                    signal_reactor_task_bulk(i,wakeup_waiters[i],waiter_count[i]);
                }
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
        if(strcmp(current->url,url) == 0) {
            ConnectionContext* temp_waiters[MAX_WAITERS];
            int temp_num_waiters = 0;
            int waiters_per_core = MAX_WAITERS / NUM_REACTORS;
            ConnectionContext* wakeup_waiters[NUM_REACTORS][waiters_per_core];
            int waiter_count[NUM_REACTORS];
            memset(waiter_count,0,sizeof(waiter_count));

            if(current->is_downloading) {
                temp_num_waiters = current->num_waiters;
                for(int i = 0; i < temp_num_waiters; i++) {
                    temp_waiters[i] = current->waiters[i];
                }
                current->num_waiters = 0;
            }
            
            if(current == target && target != NULL) {
                if(prev == NULL) {
                    cache_table[bucket] = current->next;
                } else {
                    prev->next = current->next;
                }
            } else {
                current->is_downloading = 0;
                current->revalidating = 0;
            }

            pthread_rwlock_unlock(&cache_locks[bucket]);
            if(current == target) {
                free(current->response);
                current->response = NULL;
                free(current);
                current = NULL;
            } else if(target != NULL) {
                free(target->response);
                target->response = NULL;
                free(target);
                target = NULL;
            }
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
                    int r_id = waiter->reactor_id;
                    wakeup_waiters[r_id][waiter_count[r_id]++] = waiter;
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }
            
            for(int i = 0; i < NUM_REACTORS; i++) {
                if(waiter_count[i] > 0) {
                    signal_reactor_task_bulk(i,wakeup_waiters[i],waiter_count[i]);
                }
            }
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
    
    if(ctx == NULL) {
        return -1;
    }

    CacheNode* current = NULL;
    if(ctx != NULL && ctx->cache_ref != NULL) {
        current = (CacheNode*)ctx->cache_ref;
    }

    unsigned long hash = hash_url(url);
    int bucket = hash % CACHE_BUCKETS;
    
  
    if(current != NULL && current->revalidating) {
        pthread_rwlock_wrlock(&cache_locks[bucket]);
        CacheNode* node = cache_table[bucket];
        CacheNode* prev = NULL;
        while(node != NULL) {
            if(strncmp(node->url,url,sizeof(node->url)) == 0) {
                break;
            }
            prev = node;
            node = node->next;
        }
        
        if(prev == NULL) cache_table[bucket] = node->next;
        else prev->next = node->next;
        
        current->num_waiters = node->num_waiters;
        for(int i = 0; i < node->num_waiters; i++) {
            current->waiters[i] = node->waiters[i];
        }
        node->num_waiters = 0;
        current->next = cache_table[bucket];
        cache_table[bucket] = current;

        if(node->ref_count > 0) {
            node->purged = 1;
        } else {
            free(node->response);
            node->response = NULL;
            free(node);
            node = NULL;
        }
    } else {
        pthread_rwlock_wrlock(&cache_locks[bucket]);
    }
    
    if(current != NULL) {
            current->expires_at = expires_at;
            current->cached_at = cached_at;
            current->body_size = body_size;
            current->upstream_header_len = upstream_header_len;
            current->is_downloading = 0;
            current->is_evicted = 0;
            
            if(strlen(ctx->etag) > 0) {
                strncpy(current->etag,ctx->etag,sizeof(current->etag) - 1);
                current->etag[sizeof(current->etag) - 1] = '\0';
            }
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
                    if(p_count >= 128) {
                        pthread_cond_signal(&p_notify);
                    }
                } else {
                    log_event(LEVEL_WARN, 0, "[system]", "Persistence queue filled to 8192! Dropping disk sync to prevent network IO crash.");
                }
                pthread_mutex_unlock(&p_lock);
               atomic_store(&current->last_accessed,time(NULL));
               atomic_fetch_add(&total_cache_memory,current->memory_usage);
               
               pthread_mutex_lock(&eviction_lock);
               if(atomic_load(&total_cache_memory) >= eviction_soft_limit) {
                   pthread_cond_signal(&eviction_condition);
               }
               pthread_mutex_unlock(&eviction_lock);

            } else {
                current->is_large = 1;
                current->memory_usage = 0;
            }

            ConnectionContext* temp_waiters[MAX_WAITERS];
            int waiters_per_core = MAX_WAITERS / NUM_REACTORS;
            ConnectionContext* wakeup_waiters[NUM_REACTORS][waiters_per_core];
            int waiter_count[NUM_REACTORS];
            memset(waiter_count,0,sizeof(waiter_count));

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
                    int r_id = waiter->reactor_id;
                    wakeup_waiters[r_id][waiter_count[r_id]++] = waiter;
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }
            
            for(int i = 0; i < NUM_REACTORS; i++) {
                if(waiter_count[i] > 0) {
                    signal_reactor_task_bulk(i,wakeup_waiters[i],waiter_count[i]);
                }
            }
            return 1;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    return -1;
}

int serve_cache_hit(ConnectionContext* ctx, CacheNode* current) {

    if(strlen(current->etag) > 0 && strlen(ctx->client_if_none_match) > 0) {
        if(strncmp(ctx->client_if_none_match,current->etag,sizeof(current->etag)) == 0) {
            ctx->write_len = snprintf(ctx->write_buf,sizeof(ctx->write_buf),
                             "HTTP/1.1 304 Not Modified\r\n"
                             "\r\n"
                             "Content Not Modified");
            ctx->write_offset = 0;
            ctx->send_mem_buf = NULL;
            ctx->bytes_remaining = 0;
            return -1;
        }
    }
    if(!current->is_large && current->response != NULL) {
        ctx->send_mem_buf = current->response;
        ctx->send_mem_len = current->response_len;
        ctx->send_mem_offset = current->response_len - current->body_size;
        ctx->cached_at = current->cached_at;
        ctx->cache_ref = current;
        atomic_store(&current->last_accessed,time(NULL));
        atomic_fetch_add(&metric_cache_hits,1);
        return 1; // Cache Hit
    } else if(current->is_large) {
        ctx->bytes_remaining = current->body_size;
        ctx->upstream_header_len = current->upstream_header_len;
        ctx->send_mem_buf = NULL;
        ctx->cached_at = current->cached_at;
        ctx->cache_ref = current;
        atomic_fetch_add(&metric_cache_hits,1);
        return 1; // Cache Hit
    }
    return 0; // Cache Miss
}

int allocate_cache_node(ConnectionContext* ctx,int bucket) {
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
    new_node->revalidating = 0;
    ctx->cache_ref = new_node;
    return 1;
}

int check_cache_ram(ConnectionContext* ctx) {
    unsigned long hash = hash_url(ctx->url);
    int bucket = hash % CACHE_BUCKETS;
    
    pthread_rwlock_rdlock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    while(current != NULL) {
        if(strcmp(ctx->url,current->url) == 0) {
            if(!current->is_downloading && current->expires_at > time(NULL) ) {
                int cache_status = serve_cache_hit(ctx,current);
                if(cache_status == 1) {
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 1; //Cache Hit
                } else if(cache_status == -1) {
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 3; //Not Modified Content
                }
            } else if(current->is_downloading) {
                if(current->revalidating && time(NULL) < current->expires_at + GRACE_WINDOW) {
                    int cache_status = serve_cache_hit(ctx,current);
                    if(cache_status == 1) {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        return 1; //Cache Hit
                    } else if(cache_status == -1) {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        return 3; //Not Modified Content
                    }
                }
            }
            break;
        }
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    
    pthread_rwlock_wrlock(&cache_locks[bucket]);
    current = cache_table[bucket];
    while(current != NULL) {
       if(strcmp(ctx->url,current->url) == 0) {
            //Recheck in write lock
            if(!current->is_downloading && current->expires_at > time(NULL) ) {
                int cache_status = serve_cache_hit(ctx,current);
                if(cache_status == 1) {
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 1; //Cache Hit
                } else if(cache_status == -1) {
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 3; //Not Modified Content
                }
            }

            if(current->is_downloading) {
                if(current->revalidating && time(NULL) < current->expires_at + GRACE_WINDOW) {
                    int cache_status = serve_cache_hit(ctx,current);
                    if(cache_status == 1) {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        return 1; //Cache Hit
                    } else if(cache_status == -1) {
                        pthread_rwlock_unlock(&cache_locks[bucket]);
                        return 3; //Not Modified Content
                    }
                }

                if(current->num_waiters < MAX_WAITERS) {
                    current->waiters[current->num_waiters++] = ctx;
                    pthread_rwlock_unlock(&cache_locks[bucket]);
                    return 2;
                }
                atomic_fetch_add(&metric_cache_misses,1);
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return 0;
            }

            if(current->expires_at < time(NULL)) {
                current->is_downloading = 1;
                current->revalidating = 1;
                current->num_waiters = 0;
                
                if(strlen(current->etag) > 0) {
                    strncpy(ctx->etag,current->etag,sizeof(ctx->etag) - 1);
                    ctx->etag[sizeof(ctx->etag) - 1] = 0;
                }
                if(current->memory_usage > 0) {
                    atomic_fetch_sub(&total_cache_memory,current->memory_usage);
                }
                
                if(allocate_cache_node(ctx,bucket) == 0) return 0;
                CacheNode* new_node = (CacheNode*)ctx->cache_ref;
                new_node->revalidating = 1;
                ctx->is_designated_downloader = 1;
                atomic_fetch_add(&metric_cache_misses,1);
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return -1; // Expired
            } else if(current->is_evicted) {
                current->is_downloading = 1;
                current->revalidating = 0;
                current->num_waiters = 0;
                ctx->cache_ref = current;
                ctx->is_designated_downloader = 1;
                current->is_evicted = 0;
                atomic_fetch_add(&metric_cache_misses,1);
                pthread_rwlock_unlock(&cache_locks[bucket]);
                return -1;
            }
       } 
       current = current->next;
    }

    if(allocate_cache_node(ctx,bucket) == 0) return 0;
    CacheNode* new_node = (CacheNode*)ctx->cache_ref;
    new_node->next = cache_table[bucket];
    cache_table[bucket] = new_node;
    
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
    else if(status == 3) return -2;

    return 0;
}

void cache_not_modified(ConnectionContext* ctx) {
    unsigned long hash = hash_url(ctx->url);
    int bucket = hash % CACHE_BUCKETS;

    pthread_rwlock_rdlock(&cache_locks[bucket]);
    CacheNode* current = cache_table[bucket];
    while(current != NULL) {
        if(strcmp(ctx->url,current->url) == 0) {
            current->is_downloading = 0;
            current->revalidating = 0;
            current->expires_at = time(NULL) + (current->expires_at - current->cached_at);
            current->cached_at = time(NULL);
            
            if(!current->is_large && current->response != NULL) {
                ctx->send_mem_buf = current->response;
                ctx->send_mem_len = current->response_len;
                ctx->send_mem_offset = current->response_len - current->body_size;
                ctx->cached_at = current->cached_at;
                ctx->cache_ref = current;
                atomic_store(&current->last_accessed,time(NULL));
            } else if(current->is_large) {
                ctx->bytes_remaining = current->body_size;
                ctx->upstream_header_len = current->upstream_header_len;
                ctx->send_mem_buf = NULL;
                ctx->cached_at = current->cached_at;
                ctx->cache_ref = current;
            }
            
            ConnectionContext* temp_waiters[MAX_WAITERS];
            int waiters_per_core = MAX_WAITERS / NUM_REACTORS;
            ConnectionContext* wakeup_waiters[NUM_REACTORS][waiters_per_core];
            int waiter_count[NUM_REACTORS];
            memset(waiter_count,0,sizeof(waiter_count));

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
                    int r_id = waiter->reactor_id;
                    wakeup_waiters[r_id][waiter_count[r_id]++] = waiter;
                }
                pthread_mutex_unlock(&waiter->state_lock);
            }
            
            for(int i = 0; i < NUM_REACTORS; i++) {
                if(waiter_count[i] > 0) {
                    signal_reactor_task_bulk(i,wakeup_waiters[i],waiter_count[i]);
                }
            }

            CacheNode* target = (CacheNode*)ctx->cache_ref;
            if(target != current) {
                free(target->response);
                free(target);
                ctx->cache_ref = current;
            }

            return;
        }
        current = current->next;
    }
    pthread_rwlock_unlock(&cache_locks[bucket]);
    return;
}

void rehydrate_cache() {
    DIR* dir = opendir("cache");
    if(!dir) return;
    
    time_t now = time(NULL);
    struct dirent *entry;
    while((entry = readdir(dir)) != NULL) {
        if(entry->d_name[0] == '.') continue;

        char cache_file[300];
        snprintf(cache_file,sizeof(cache_file),"cache/%s",entry->d_name);

        if(strstr(entry->d_name,"tmp")) {
            unlink(cache_file);
            continue;
        }

        int read_fd = open(cache_file,O_RDONLY);
        if(read_fd < 0) continue;

        CacheHeader header;
        if(read(read_fd,&header,sizeof(CacheHeader)) == sizeof(CacheHeader)) {
            if(header.expires_at > now) {
                // SECURITY GUARD against FORTIFY_SOURCE stack buffer overflow
                if(header.upstream_header_len > BUFFER || header.upstream_header_len < 0) {
                    if(global_config.log_level <= LEVEL_WARN) {
                        char log_buf[128];
                        snprintf(log_buf,sizeof(log_buf),"[rehydrate] Warning: file header length %d exceeds BUFFER length %d! Skipping node.\n",
                                header.upstream_header_len, BUFFER);
                        log_event(LEVEL_WARN, 0, "[system]",log_buf);    
                    }
                    close(read_fd);
                    continue;
                }
                
                long payload_size = header.content_length;
                long body_size = header.content_length - header.upstream_header_len;

                unsigned long hash = hash_url(header.url);
                int bucket = hash % CACHE_BUCKETS;

                CacheNode* node = calloc(1,sizeof(CacheNode));
                if(!node) {
                    close(read_fd);
                    continue;
                }

                strncpy(node->url,header.url,sizeof(node->url) - 1);
                node->cached_at = header.cached_at;
                node->expires_at = header.expires_at;
                node->body_size = body_size;
                node->upstream_header_len = header.upstream_header_len;
                node->is_downloading = 0;
                node->is_evicted = 0;
                
                if(payload_size > 0 && body_size < global_config.large_file_threshold) {
                    char* payload = malloc(payload_size);
                    if(payload) {
                        long payload_read = 0;
                        while(payload_read < payload_size) {
                            int r = read(read_fd,payload + payload_read,payload_size - payload_read);
                            if(r <= 0) {
                                close(read_fd);
                                free(payload);
                                free(node);
                                payload_read = 0;
                                continue;
                            }
                            payload_read += r;
                        }
                        if(payload_read == 0) continue;
                        free(node->response);
                        node->response = payload;
                        node->response_len = payload_size;
                        node->is_large = 0;
                        node->memory_usage = payload_size;
                        node->sync_to_disk = 1;
                    }
                } else {
                    node->is_large = 1;
                }

                node->next = cache_table[bucket];
                cache_table[bucket] = node;
            } else {
                unlink(cache_file);
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