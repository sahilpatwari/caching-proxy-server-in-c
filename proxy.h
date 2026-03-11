#ifndef PROXY_H
#define PROXY_H

#include<netinet/in.h>

#define BUFFER 8192

struct ProxyRequest {
    char hostname[1024];
    int port;
    char path[4096];
};


typedef enum {
    STATE_READ_REQUEST,      // Waiting for/Reading the HTTP request from client
    STATE_PARSE_REQUEST,     // Parsing headers (Method, URL, Keep-Alive)
    STATE_CHECK_CACHE,       // Determining if it's a Hit or Miss
    STATE_SEND_CACHE,        // Streaming file to client via sendfile()
    STATE_CONNECT_UPSTREAM,  // Non-blocking connect() to upstream server
    STATE_WAIT_CONNECT,      // Wait till handshake is complete
    STATE_SEND_UPSTREAM,     // Sends the HTTP GET request to upstream server
    STATE_FETCH_UPSTREAM,    // Downloading from upstream and saving to cache
    STATE_SEND_RESPONSE_HEADERS, // Send response headers back to the client
    STATE_TUNNELING,         // Handling POST requests
    STATE_CLOSE              // Connection is dead, clean up memory
} ConnectionState;



typedef struct {
    int client_fd;         
    int upstream_fd;        
    uint64_t req_id;       
    
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

    char method[16];
    char url[5120];
    char protocol[16];
    struct ProxyRequest req;

    int file_fd;            
    off_t file_offset;     
    long bytes_remaining;   
} ConnectionContext;

int parse_url(char*,struct ProxyRequest*);
int connect_to_host(char*,int);
#endif