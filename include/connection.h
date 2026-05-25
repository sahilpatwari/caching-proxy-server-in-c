#ifndef CONNECTION_H
#define CONNECTION_H

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
    uint32_t client_interests;   // Track currently registered client events
    uint32_t upstream_interests; // Track currently registered upstream events

    char etag[256];
    char client_if_none_match[256];
    int revalidating;
    int is_reused_upstream;
    int is_error;
} ConnectionContext;

#endif