#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/epoll.h>
#include<sys/eventfd.h>
#include<fcntl.h>
#include<errno.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<netinet/tcp.h>

#include"proxy.h"
#include"proxy_log.h"
#include"config.h"
#include"context_pool.h"
#include"http_handler.h"
#include"reactor.h"
#define BACKLOG SOMAXCONN
#define EPOLL_BATCH_SIZE 128
__thread int epoll_fd;

Reactor* global_reactors;

void signal_reactor_task_bulk(int target_reactor_id, ConnectionContext** waiters,int count) {
    Reactor* r = &global_reactors[target_reactor_id];

    pthread_mutex_lock(&r->task_lock);
    for(int i = 0; i < count; i++) {
        ConnectionContext* ctx = waiters[i];
        ctx->next_task = NULL;
        if (r->task_tail == NULL) {
            r->task_head = r->task_tail = ctx;
        } else {
            r->task_tail->next_task = ctx;
            r->task_tail = ctx;
        }
    }
    pthread_mutex_unlock(&r->task_lock);

    uint64_t u = 1;
    if(atomic_exchange(&r->wakeup_pending,1) == 0) {
        if (write(r->wakeup_fd, &u, sizeof(uint64_t)) == -1) {
            if (errno != EAGAIN) perror("signal_reactor_task: write failed");
        }
    }
}

int make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return -1;
    }
    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) == -1) {
        perror("fcntl F_SETFL O_NONBLOCK");
        return -1;
    }
    return 0;
}

void* worker_reactor_loop(void* args) {
    int r_id = (int)(intptr_t)args;
    Reactor* reactor = &global_reactors[r_id];
    int status, sockfd = -1,yes = 1;
    struct addrinfo hints,*res,*p;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", global_config.port);

    if((status = getaddrinfo(NULL,port_str,&hints,&res)) == -1) {
        fprintf(stderr,"worker: getaddrinfo: %s\n",gai_strerror(status));
        return NULL;
    }

    for(p = res;p !=  NULL; p = p->ai_next) {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1) {
            perror("worker: socket allocation thread failed");
            continue;
        }
        
        if(setsockopt(sockfd,SOL_SOCKET, SO_REUSEADDR,&yes,sizeof(int)) == -1) {
            perror("setsocketopt");
            exit(1);
        }

        if(setsockopt(sockfd,SOL_SOCKET, SO_REUSEPORT,&yes,sizeof(int)) == -1) {
            perror("setsocketopt");
            exit(1);
        }
        
        if(bind(sockfd,p->ai_addr,p->ai_addrlen) == -1) {
            perror("worker: bind failed");
            close(sockfd);
            continue;
        }

        break;
    }
    
    freeaddrinfo(res);

    if(p == NULL) {
        fprintf(stderr,"worker: Couldn't bind a to a port");
    }

    if(listen(sockfd,BACKLOG) == -1) {
        perror("worker: listen failed");
        exit(1);
    }

    make_socket_non_blocking(sockfd); //make the main listener non-blocking

    epoll_fd = epoll_create1(0);
    reactor->epoll_fd = epoll_fd;
    if(epoll_fd == -1) {
        perror("worker: epoll instance creation failed");
        exit(1);
    }

    struct epoll_event event;
    memset(&event,0,sizeof(event));
    struct epoll_event events[EPOLL_BATCH_SIZE];

    event.data.fd = sockfd;
    event.events = EPOLLIN | EPOLLET; // Listener must be Edge-Triggered
    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,sockfd,&event) == -1) {
        perror("worker: epoll ctl: sockfd failed");
        exit(1);
    }
    
    reactor->wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if(reactor->wakeup_fd == -1) {
        perror("eventfd creation failed");
        exit(1);
    }

    struct epoll_event ev;
    memset(&ev,0,sizeof(ev));
    ev.data.fd = reactor->wakeup_fd;
    ev.events = EPOLLIN;
    if(epoll_ctl(reactor->epoll_fd,EPOLL_CTL_ADD,reactor->wakeup_fd,&ev) == -1) {
        perror("wakeup fd: add failed");
        exit(1);
    }
    
   ConnectionContext* local_stolen_head = NULL;

    while(server_running) {
        
        int timeout = (local_stolen_head == NULL) ? 100 : 0;
        int n_ready = epoll_wait(epoll_fd,events,EPOLL_BATCH_SIZE,timeout);
        if(n_ready == -1) {
            if(errno == EINTR) {
                continue;
            }

            perror("worker: epoll_wait failed");
            break;
        }

        for(int i = 0; i < n_ready; i++) {
            int currentfd = events[i].data.fd;
            if(currentfd == sockfd) {
                while(1) {
                    struct sockaddr_storage their_addr;
                    socklen_t sin_size;
                    sin_size = sizeof their_addr;

                    int newfd = accept(sockfd,(struct sockaddr*)&their_addr,&sin_size);
                    if(newfd == -1) {
                        if(errno == EAGAIN || errno == EWOULDBLOCK) {
                            break; //We have accepted all incoming connections
                        }
                        perror("worker: accept");
                        break;
                    }

                    make_socket_non_blocking(newfd);
                    int flag = 1;
                    if(setsockopt(newfd,IPPROTO_TCP,TCP_NODELAY,&flag,sizeof(flag)) == -1) {
                        perror("worker: setsockopt TCP_NODELAY Failed(Non-Fatal)");
                    }

                    ConnectionContext* ctx = create_context(newfd);
                    if(ctx == NULL) {
                        fprintf(stderr,"worker: create_context failed");
                        close(newfd);
                        continue;
                    }

                    ctx->thread_epoll_fd = epoll_fd;
                    ctx->reactor_id = r_id; 
                    ctx->client_interests = EPOLLIN | EPOLLET; 
                    ctx->upstream_interests = 0;

                    event.data.fd = newfd;
                    event.events = ctx->client_interests;
                    if(epoll_ctl(epoll_fd,EPOLL_CTL_ADD,newfd,&event) == -1) {
                        perror("worker: epoll ctl failed adding client descriptor");
                        ctx->state = STATE_CLOSE;
                        free_context(ctx);
                        close(newfd);
                        continue;
                    }
                }
            } else if(currentfd == reactor->wakeup_fd) {
                uint64_t u;
                if (read(reactor->wakeup_fd, &u, sizeof(uint64_t)) == -1) {
                    if (errno != EAGAIN) perror("read eventfd failed");
                }
                atomic_store(&reactor->wakeup_pending, 0);
                // Atomically steal the entire task list
                pthread_mutex_lock(&reactor->task_lock);
                if(local_stolen_head == NULL) {
                    local_stolen_head = reactor->task_head;
                } else {
                    reactor->task_tail->next_task = local_stolen_head;
                    local_stolen_head = reactor->task_head;
                }
                reactor->task_head = reactor->task_tail = NULL;
                pthread_mutex_unlock(&reactor->task_lock);

            } else {
                ConnectionContext* ctx = atomic_load(&context_table[currentfd]);
                if(ctx != NULL) {
                    atomic_store(&ctx->last_active,time(NULL));
                    handle_state_machine(ctx);
                } else {
                    fprintf(stderr,"Ghost file descriptor event triggered on fd %d\n",currentfd);
                }
            }
        }

        int quota = 1024;
        while(local_stolen_head != NULL && quota-- > 0) {
            ConnectionContext* current = local_stolen_head;
            local_stolen_head = current->next_task;
            handle_state_machine(current);
        }
    }
    close(epoll_fd);
    close(sockfd);
    return NULL;  
}