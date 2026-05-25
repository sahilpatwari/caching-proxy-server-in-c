#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>

#include"proxy.h"
#include"config.h"
#include"connection.h"
#include"cache.h"
#include"upstream.h"

#define MAX_FDS 65536

// ATOMIC REQUEST ID COUNTER
_Atomic uint64_t global_req_id = 0;

_Atomic(ConnectionContext*) context_table[MAX_FDS];
static ConnectionContext* context_pool_memory;
static ConnectionContext** free_context_stack;
static int free_context_stack_top = -1;
static pthread_mutex_t context_pool_lock = PTHREAD_MUTEX_INITIALIZER;

void init_context_pool() {
    context_pool_memory = calloc(global_config.pool_max_connections,sizeof(ConnectionContext));
    free_context_stack = malloc(global_config.pool_max_connections * sizeof(ConnectionContext*));

    if(!context_pool_memory || !free_context_stack) {
        printf("Failed to pre-allocate RAM");
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < global_config.pool_max_connections; i++) {
        free_context_stack[i] = &context_pool_memory[i];
    }
    free_context_stack_top = global_config.pool_max_connections - 1;
}

void destroy_context_pool() {
    if(context_pool_memory) free(context_pool_memory);
    if(free_context_stack)  free(free_context_stack);

    context_pool_memory = NULL;
    free_context_stack = NULL;
    free_context_stack_top = -1;
}

ConnectionContext* create_context(int fd) {
    if(fd >= MAX_FDS) return NULL;
    
    ConnectionContext* ctx = NULL;
    pthread_mutex_lock(&context_pool_lock);
    if(free_context_stack_top >= 0) {
        ctx = free_context_stack[free_context_stack_top--];
    }
    pthread_mutex_unlock(&context_pool_lock);
    
    if(!ctx) {
        ctx = calloc(1,sizeof(ConnectionContext));
        if(!ctx) return NULL;
    }

    ctx->send_mem_buf = NULL;
    ctx->send_mem_len = 0;
    ctx->send_mem_offset = 0;
    ctx->next_task = NULL;

    struct sockaddr_storage addr;
    socklen_t addr_size = sizeof(addr);
    if(getpeername(fd,(struct sockaddr*)&addr,&addr_size) == 0) {
        if (addr.ss_family == AF_INET) {
            inet_ntop(AF_INET, &((struct sockaddr_in*)&addr)->sin_addr,
                      ctx->client_ip, sizeof(ctx->client_ip));
        } else {
            inet_ntop(AF_INET6, &((struct sockaddr_in6*)&addr)->sin6_addr,
                      ctx->client_ip, sizeof(ctx->client_ip));
        }
    } else {
        strcpy(ctx->client_ip,"UNKNOWN");
    }
    
    ctx->client_fd = fd;
    ctx->upstream_fd = -1;
    ctx->file_fd = -1;
    pthread_mutex_init(&ctx->state_lock, NULL);
    atomic_store(&ctx->last_active,time(NULL));
    ctx->req_id = atomic_fetch_add(&global_req_id,1);
    ctx->state = STATE_READ_REQUEST;
    
    ctx->cached_at = 0;
    ctx->bytes_read = 0;
    ctx->write_len = 0;
    ctx->write_offset = 0;
    ctx->is_designated_downloader = 0;
    ctx->bytes_remaining = 0;
    ctx->file_offset = 0;
    ctx->keep_alive = 1;
    ctx->upstream_header_len = 0;
    ctx->upstream_headers_parsed = 0;
    ctx->cache_ttl = 0;
    ctx->checkCache = 0;
    ctx->upstream_content_length = 0;
    ctx->upstream_body_downloaded = 0;
    ctx->cache_ref = NULL;

    ctx->is_chunked = 0;
    ctx->chunk_state = 0;
    ctx->current_chunk_size = 0;
    ctx->current_chunk_bytes_read = 0;
    ctx->hex_idx = 0;
    ctx->header_overshoot_len = 0;
    
    ctx->read_buf[0] = '\0';
    ctx->method[0] = '\0';
    ctx->url[0] = '\0';
    ctx->protocol[0] = '\0';
    ctx->req.hostname[0] = '\0';
    ctx->req.port = 0;
    ctx->req.path[0] = '\0';
    ctx->etag[0] = '\0';
    ctx->client_if_none_match[0] = '\0';
    ctx->client_interests = 0;
    ctx->upstream_interests = 0;
    ctx->revalidating = 0;
    ctx->is_reused_upstream = 0;
    ctx->is_error = 0;
    atomic_store(&context_table[fd],ctx);
    return ctx; 
}

void free_context(ConnectionContext* ctx) {
    if (ctx == NULL) return;
     
    if(ctx->is_designated_downloader) {
        abort_cache_download(ctx->url,ctx->cache_ref);
        ctx->is_designated_downloader = 0;
        ctx->cache_ref = NULL;
    }
    
    release_cache_ref(ctx->cache_ref);
    ctx->cache_ref = NULL;

    if (ctx->upstream_fd != -1) {
        atomic_store(&context_table[ctx->upstream_fd],NULL); 
        atomic_fetch_sub(&active_upstream_connections,1);
        close(ctx->upstream_fd);
        ctx->upstream_fd = -1;
    }
    
    if (ctx->file_fd != -1) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
    }

    if (ctx->client_fd != -1) {
        atomic_store(&context_table[ctx->client_fd],NULL);
        close(ctx->client_fd);
        ctx->client_fd = -1;
    }
    
    pthread_mutex_lock(&context_pool_lock);
    if(free_context_stack_top < global_config.pool_max_connections - 1) {
        free_context_stack[++free_context_stack_top] = ctx;
        pthread_mutex_unlock(&context_pool_lock);
        return;
    } 
    pthread_mutex_unlock(&context_pool_lock);

    free(ctx);
}
