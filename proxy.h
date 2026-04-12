#ifndef PROXY_H
#define PROXY_H

#include<netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include"proxy_log.h"

typedef struct {
    int port;
    int backlog;
    int buffer;
    int max_fds;
    int epoll_batch_size;
    int pool_buckets;
    int pool_max_connections;
    int idle_timeout_sec;
    int latency_upstream_threshold;
    int cooldown_sec;
    int max_consecutive_errors;
    int max_cache_mem;
    int large_file_threshold;
    int default_ttl;
    int cache_buckets;
    int max_waiters;
    int expiry_interval;
    int prefetch_threshold;
    int prefetch_window;
    int rate_limiter_capacity;
    int refill_rate;
    int client_timeout;
    int cleanup_interval;
    int hash_table_size;
    LogLevel log_level;
    int log_queue_size;
    int max_log_msg;
    int log_batch_size;
} ProxyConfig;

extern ProxyConfig global_config;
void load_config(const char* filename);

#define BUFFER 8192

struct ProxyRequest {
    char hostname[256];
    int port;
    char path[2048];
};

typedef enum {
    STATE_READ_REQUEST,      // Waiting for/Reading the HTTP request from client
    STATE_PARSE_REQUEST,     // Parsing headers (Method, URL, Keep-Alive)
    STATE_CHECK_CACHE,       // Determining if it's a Hit or Miss
    STATE_SEND_CACHE,        // Streaming file to client via sendfile() for larger payloads, for smaller payloads use send()(in-memory cache)
    STATE_WAIT_CACHE,        // Waiting for cache file download
    STATE_CONNECT_UPSTREAM,  // Non-blocking connect() to upstream server
    STATE_WAIT_CONNECT,      // Wait till handshake is complete
    STATE_SEND_UPSTREAM,     // Sends the HTTP GET request to upstream server
    STATE_FETCH_UPSTREAM,    // Downloading from upstream and saving to cache
    STATE_SEND_RESPONSE_HEADERS, // Send response headers back to the client
    STATE_TUNNELING,         // Handling POST requests
    STATE_CLOSE              // Connection is dead, clean up memory
} ConnectionState;



typedef struct ConnectionContext {
    int client_fd;         
    int upstream_fd;        
    uint64_t req_id;       
    time_t cached_at;
    _Atomic time_t last_active;

    ConnectionState state;
     
    char client_ip[INET6_ADDRSTRLEN];
    
    char read_buf[BUFFER];
    int bytes_read;        
    int keep_alive;         
    
    char write_buf[BUFFER];
    int write_len;
    int write_offset;
    
    char upstream_header_buf[BUFFER];
    int upstream_header_len;
    int upstream_headers_parsed;
    int cache_ttl;
    int checkCache;
    long upstream_content_length;
    long upstream_body_downloaded;
    int is_designated_downloader;
    
    int is_chunked;     
    int chunk_state;  // 0=READ_SIZE, 1=READ_DATA, 2=READ_CRLF, 3=DONE
    long current_chunk_size;     
    long current_chunk_bytes_read;
    char hex_buf[32];
    int hex_idx;
    
    int header_overshoot_len;
    char header_overshoot_buf[BUFFER];
    char method[16];
    char url[512];
    char protocol[16];
    struct ProxyRequest req;
    
    _Atomic int active_threads; 
    pthread_mutex_t state_lock;
    
    int file_fd;            
    off_t file_offset;     
    long bytes_remaining;  
    
    const char* send_mem_buf;
    long send_mem_len;
    long send_mem_offset;
    int thread_epoll_fd;

    int is_spooled_disk;

    struct timeval upstream_send_time;

    void* cache_ref;
    int reactor_id;        // The ID of the owner reactor (0, 1, etc.)
    struct ConnectionContext* next_task; // For the Reactor's linked-list work queue
    uint32_t epoll_interests; // Track currently registered epoll events
} ConnectionContext;

typedef struct ConnectionNode {
    int fd;
    struct ConnectionNode* next;
} ConnectionNode;

typedef struct HostEntry{
    char hostname[256];
    int port;
    ConnectionNode* fd_head;   
    struct HostEntry* next_host;      
} HostEntry;

typedef struct HostBucket{
    HostEntry* head;
    pthread_mutex_t bucket_lock;
}HostBucket;


typedef struct {
    int reactor_id;
    int epoll_fd;
    int wakeup_fd;         // eventfd used for signalling
    ConnectionContext* task_head;
    ConnectionContext* task_tail;
    pthread_mutex_t task_lock;
} Reactor;


void* handle_state_machine(void* args);

int parse_url(char*,struct ProxyRequest*);
#endif