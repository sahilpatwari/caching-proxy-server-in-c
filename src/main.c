#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<sys/stat.h>
#include<string.h>
#include<signal.h>
#include <stdint.h> 
#include <inttypes.h>
#include <sys/time.h>

#include"proxy.h"
#include"cache.h"
#include"context_pool.h"
#include"config.h"
#include"proxy_log.h"
#include"reactor.h"
#include"upstream.h"
#include"workers.h"


// SHUTDOWN FLAG
volatile sig_atomic_t server_running = 1;

// SIGNAL HANDLER
void handle_shutdown_signal(int sig) {
    (void)sig; // Suppress unused warning
    server_running = 0; 
}

int main(int argc,char* argv[]) {
    
    char hostname[256] = "127.0.0.1";
    char port_str[16]  = "8080";

    if(argc == 1) {
        fprintf(stderr,"use --help for usage details");
        return 0;
    }

    for(int i = 1; i < argc; i++) {
        if(strcmp(argv[i],"--help") == 0) {
            fprintf(stderr,"Usage Details: ./http_proxy.exe --port <port> --origin <hostname>\n");
            fprintf(stderr,"Example: ./http_proxy.exe --port 8080 --origin 127.0.0.1");
            return 0;
        } else{
            if(argc < 5) {
                fprintf(stderr,"Wrong Usage. use --help for usage details");
                return 0;
            }
            if(strcmp(argv[i],"--port") == 0 && i + 1 < argc) {
                strncpy(port_str,argv[++i],sizeof(port_str) - 1);
                port_str[sizeof(port_str) - 1] = '\0';

            } else if(strcmp(argv[i],"--origin") == 0 && i + 1 < argc) {
                strncpy(hostname,argv[++i],sizeof(hostname) - 1);
                hostname[sizeof(hostname) - 1] = '\0';

            }
        }
    }

    if(resolve_origin_dns(hostname,port_str) == -1) {
        return 0;
    }

    signal(SIGPIPE, SIG_IGN); 

    struct sigaction sa;
    sa.sa_handler = handle_shutdown_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);  // Catch Ctrl+C
    sigaction(SIGTERM, &sa, NULL); // Catch kill commands

    mkdir("cache", 0777);
    
    // Load config before initializing systems
    load_config("conf/proxy.conf");
    
    init_log(global_config.log_level);
    //init_rate_limiter();
    init_connection_pool();
    init_cache();
    init_context_pool();
    init_upstream_dns_lock();
    rehydrate_cache();

    printf("Server is listening\n");
    global_req_id = (uint64_t)time(NULL) << 16;
    printf("Proxy Server started. Initial Req ID: %" PRIu64 "\n", global_req_id);
    
    server_start_time = time(NULL);
    

    // Initialize Global Reactors
    global_reactors = malloc(sizeof(Reactor) * NUM_REACTORS);
    for (int i = 0; i < NUM_REACTORS; i++) {
        global_reactors[i].reactor_id = i;
        global_reactors[i].task_head = global_reactors[i].task_tail = NULL;
        pthread_mutex_init(&global_reactors[i].task_lock, NULL);
    }

    pthread_t reactors[NUM_REACTORS];
    for(int i = 0; i < NUM_REACTORS; i++) {
        // Pass the Reactor pointer directly to the thread
        if(pthread_create(&reactors[i],NULL,worker_reactor_loop, (void*)(intptr_t)i) != 0) {
            perror("Failed to start worker reactor thread");
        }
    }

    pthread_t telemetry_thread;
    if (pthread_create(&telemetry_thread, NULL, telemetry_worker, NULL) != 0) {
        perror("Failed to start telemetry daemon");
    }
    pthread_detach(telemetry_thread);
    printf("[System] Telemetry daemon started.\n");

    pthread_t reaper_thread;
    if(pthread_create(&reaper_thread,NULL,connection_reaper_worker,NULL) != 0) {
        perror("Failed to start the reaper daemon");
    }
    pthread_detach(reaper_thread);
    printf("[System] Idle Reaper daemon started.\n");
    
    pthread_t dns_thread;
    if(pthread_create(&dns_thread,NULL,dns_refresh_worker,NULL) != 0) {
        perror("Failed to start the dns thread");
    }
    pthread_detach(dns_thread);
    printf("[System] DNS Thread started.\n");
    for(int i = 0; i < NUM_REACTORS; i++) {
        pthread_join(reactors[i],NULL);
    }

    printf("\nInitiating graceful shutdown sequence...\n");
    //destroy_rate_limiter(); 
    destroy_cache();
    destroy_context_pool();
    close_log(); 

    printf("Proxy shut down successfully. All resources freed.\n");
    return 0;
}