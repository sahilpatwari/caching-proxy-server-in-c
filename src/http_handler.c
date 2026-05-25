#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<netinet/in.h>
#include<sys/stat.h>
#include <time.h>
#include <sys/time.h>
#include<errno.h>
#include<sys/epoll.h>
#include<sys/sendfile.h>
#include<sys/mman.h>
#include<fcntl.h>
#include<netinet/tcp.h>


#include"proxy.h"
#include"proxy_log.h"
#include"config.h"
#include"cache.h"
#include"context_pool.h"
#include"upstream.h"
#include"workers.h"
#include"reactor.h"

int parse_upstream_headers(ConnectionContext* ctx,char* body_ptr,int* upstream_dropped_out) {
    int headers_len = body_ptr - ctx->upstream_header_buf;
    ctx->cache_ttl = global_config.default_ttl;// Default TTL
    
    char copy_buf[BUFFER + 1];
    int safe_len = (headers_len < BUFFER) ? headers_len : BUFFER;
    memcpy(copy_buf,ctx->upstream_header_buf,safe_len);
    copy_buf[safe_len] = '\0';
    char* save_ptr;

    char* line = strtok_r(copy_buf,"\r\n",&save_ptr);

    if(line) {
        if(strncmp(line,"HTTP/1.0",8) == 0) *upstream_dropped_out = 1;
        int status_code = 0;
        if(sscanf(line,"%*s %d",&status_code) == 1) {
            if(status_code != 200 && status_code != 304) {
                ctx->cache_ttl = 0;
            }
            if(status_code == 304) {
                return 304;
            }
        }
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    
    while(line) {
        if(strncasecmp(line,"Content-Length:",15) == 0) {
            ctx->upstream_content_length = atoi(line + 15);
        } 
        else if(strncasecmp(line,"Transfer-Encoding:",18) == 0) {
            if(strstr(line + 18,"chunked") != NULL) {
                ctx->is_chunked = 1;
                ctx->chunk_state = 0;
                ctx->current_chunk_bytes_read = 0;
                ctx->hex_idx = 0;
            }
        } 
        else if(strncasecmp(line,"Connection:",11) == 0) {
            if(strcasestr(line + 11,"close") != NULL) *upstream_dropped_out = 1;
        } 
        else if(strncasecmp(line,"Cache-Control:",14) == 0) {
            char* header_val = line + 14;
            while(*header_val == ' ') header_val++;
            if(strcasestr(header_val,"no-store")  || strcasestr(header_val,"no-cache")  || strcasestr(header_val,"private")) {
                ctx->cache_ttl = 0; // Don't cache
            } else {
                char* max_age = strcasestr(header_val,"max-age");
                if(max_age) {
                    ctx->cache_ttl = atoi(max_age + 8);
                }
            }
        }
        else if(strncasecmp(line,"ETag:",5) == 0) {
            char* header_val = line + 5;
            while(*header_val == ' ') header_val++;
            strncpy(ctx->etag,header_val,sizeof(ctx->etag) - 1);
            ctx->etag[sizeof(ctx->etag) - 1] = '\0';
        }
        line = strtok_r(NULL,"\r\n",&save_ptr);
    }
    return 200;
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

void format_error_response(ConnectionContext* ctx,int status_code,const char *message,const char* extra_headers) {
    char body[1024];
    char *status_text;

    switch(status_code) {
        case 400: status_text = "Bad Request"; break;
        case 403: status_text = "Forbidden"; break;
        case 404: status_text = "Not Found"; break;
        case 411: status_text = "Length Required"; break;
        case 429: status_text = "Too Many Requests"; break;
        case 500: status_text = "Internal Server Error"; break;
        case 501: status_text = "Not Implemented"; break;
        case 502: status_text = "Bad Gateway"; break;
        case 503: status_text = "Service Unavailable"; break;
        case 504: status_text = "Gateway Timeout"; break;
        default:  status_text = "Error"; break;
    }

    int body_len = snprintf(body,sizeof(body),"<html><body><h1>%d %s</h1><p>%s</p></body></html>",status_code,status_text,message);
    const char* headers = extra_headers ? extra_headers : "";
    ctx->write_len = snprintf(ctx->write_buf,sizeof(ctx->write_buf),
             "HTTP/1.1 %d %s\r\n"
             "Content-Type:text/html\r\n"
             "Content-Length:%d\r\n"
             "Connection:close\r\n"
             "%s"
             "\r\n"
             "%s\r\n",
             status_code,status_text,body_len,headers,body);
    
    ctx->write_offset = 0;
    ctx->keep_alive = 0;
    ctx->is_error = 1;
    ctx->state = STATE_SEND_RESPONSE_HEADERS;
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
        } else if(isHit == -2) {
            //Not Modified Content
            ctx->state = STATE_SEND_RESPONSE_HEADERS;
            return;
        } 
    }
    
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
    
    int cork = 1;
    setsockopt(ctx->client_fd, IPPROTO_TCP, TCP_CORK, &cork, sizeof(cork));
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
        
        ctx->is_designated_downloader = 0;
        ctx->revalidating = 0;
        
        ctx->checkCache = 0;
        ctx->read_buf[0] = '\0';
        ctx->method[0] = '\0';
        ctx->url[0] = '\0';
        ctx->protocol[0] = '\0';
        ctx->req.hostname[0] = '\0';
        ctx->req.path[0] = '\0';
        ctx->is_reused_upstream = 0;
        ctx->is_error = 0;
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
                
                if(ctx->upstream_header_len == 0 && ctx->is_reused_upstream) {
                        atomic_fetch_sub(&active_upstream_connections,1);
                        atomic_store(&context_table[ctx->upstream_fd],NULL);
                        close(ctx->upstream_fd);
                        ctx->upstream_fd = -1;
                        
                        ctx->write_offset = 0;
                        ctx->state = STATE_CONNECT_UPSTREAM;
                        return;
                }

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
                int status_code = parse_upstream_headers(ctx,body_ptr,&upstream_dropped);
                
                if(status_code == 304) {
                    cache_not_modified(ctx);
                    ctx->checkCache = 0;
                    ctx->state = STATE_CHECK_CACHE;
                    return;
                }

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
                            format_error_response(ctx, 500, "Internal Server Error", NULL); 
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
                                        format_error_response(ctx, 500, "Internal Server Error", NULL); 
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
                    format_error_response(ctx, 500, "Internal Server Error", NULL); 
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
        format_error_response(ctx, 502, "Bad Gateway", NULL);
        return;
    }

    
    if(ctx->upstream_content_length > 0 && ctx->upstream_body_downloaded < ctx->upstream_content_length) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
        unlink(temp_file);
        format_error_response(ctx, 502, "Bad Gateway", NULL); 
        return;
    }
    
    if(ctx->is_chunked && ctx->chunk_state != 3) {
        close(ctx->file_fd);
        ctx->file_fd = -1;
        unlink(temp_file);
        format_error_response(ctx, 502, "Bad Gateway", NULL); 
        return;
    }

    CacheHeader finalHeader;
    memset(&finalHeader,0,sizeof(CacheHeader));
    strncpy(finalHeader.url,ctx->url,sizeof(finalHeader.url) - 1);
    finalHeader.url[sizeof(finalHeader.url) - 1] = '\0';
    if(strlen(ctx->etag) > 0) {
        strncpy(finalHeader.etag,ctx->etag,sizeof(finalHeader.etag) - 1);
        finalHeader.etag[sizeof(finalHeader.etag) - 1] = '\0';
    }
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
            bypass_cache_for_waiters(ctx->url,ctx->cache_ref);
            ctx->is_designated_downloader = 0;
            ctx->cache_ref = NULL;
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
            format_error_response(ctx, 500, "Internal Server Error", NULL);
        }

        read_fd = open(cache_file,O_RDONLY);
        if(read_fd < 0) {format_error_response(ctx, 500, "Internal Server Error", NULL); return;}
    } else {
        read_fd = ctx->file_fd;
        ctx->file_fd = -1;
    }


    char file_buf[BUFFER];
    lseek(read_fd,sizeof(CacheHeader),SEEK_SET);
    int hdr_read = read(read_fd,file_buf,ctx->upstream_header_len);
    if(hdr_read < ctx->upstream_header_len) {
        format_error_response(ctx, 500, "Internal Server Error", NULL);
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
                    format_error_response(ctx, 500, "Internal Server Error", NULL);
                    return;
                }
                total_read += r;
            }
            int cache_status = add_to_cache_ram(ctx,ctx->url, finalHeader.expires_at,finalHeader.cached_at,file_buf, ctx->upstream_header_len,body_buf, body_size);
            if(cache_status <= 0) {
                close(read_fd);
                format_error_response(ctx, 500, "Internal Server Error", NULL);
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
        add_to_cache_ram(ctx,ctx->url, finalHeader.expires_at,finalHeader.cached_at,file_buf, ctx->upstream_header_len,NULL, body_size);
        ctx->is_designated_downloader = 0;
        close(read_fd);

        ctx->file_fd = open(cache_file,O_RDONLY);
        if(ctx->file_fd < 0) { format_error_response(ctx, 500, "Internal Server Error", NULL); return; }
        lseek(ctx->file_fd, sizeof(CacheHeader), SEEK_SET);
        ctx->checkCache = 0;
        ctx->send_mem_buf = NULL;
        ctx->state = STATE_CHECK_CACHE;
        return;
    }
}


void handle_send_response_headers(ConnectionContext* ctx) {
    while(ctx->write_offset < ctx->write_len) {
        int bytes_sent = send(ctx->client_fd,ctx->write_buf + ctx->write_offset,ctx->write_len - ctx->write_offset,MSG_NOSIGNAL);

        if(bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) return;
            perror("Client send error");
            ctx->state = STATE_CLOSE;
            return;
        } else if (bytes_sent == 0) {
            ctx->state = STATE_CLOSE; // Client hung up early
            return;
        }
        atomic_store(&ctx->last_active,time(NULL));
        ctx->write_offset += bytes_sent;
    }

    if(!ctx->is_error) {
        ctx->state = STATE_SEND_CACHE;
    } else {
        ctx->state = STATE_CLOSE;
    }
}

void handle_send_upstream(ConnectionContext* ctx) {
    
    if(ctx->write_len == 0) {
        ctx->write_len = snprintf(ctx->write_buf,sizeof(ctx->write_buf),
        "%s %s %s\r\nHost: %s\r\nConnection: keep-alive\r\n"
        ,ctx->method,(ctx->req).path,ctx->protocol,(ctx->req).hostname);
        
        if(ctx->revalidating == 1 && strlen(ctx->etag) > 0) {
            ctx->write_len += snprintf(ctx->write_buf + ctx->write_len,sizeof(ctx->write_buf) - ctx->write_len,
                             "If-None-Match: %s\r\n",ctx->etag);
        }
        ctx->write_offset = 0;
        char* p = (char*)memchr(ctx->read_buf,'\n',ctx->bytes_read);
        if(!p) goto append_terminator;
        p++;

        char* buf_end = ctx->read_buf + ctx->bytes_read;
        while(p < buf_end) {
            char* line_end = (char*)memchr(p,'\r',buf_end - p);
            if(!line_end || line_end + 1 >= buf_end || *(line_end + 1) != '\n') break;

            int line_len = line_end - p;
            if(line_len == 0) break;

            if(strncasecmp(p,"Host:",5) == 0 || strncasecmp(p,"Connection:",11) == 0 || strncasecmp(p,"If-None-Match:",14) == 0) {
                p = line_end + 2;
                continue;
            }

            if(ctx->write_len + line_len + 2 < (int)sizeof(ctx->write_buf)) {
                memcpy(ctx->write_buf + ctx->write_len, p, line_len);
                ctx->write_buf[ctx->write_len + line_len] = '\r';
                ctx->write_buf[ctx->write_len + line_len + 1] = '\n';
                ctx->write_len += line_len + 2;
            }

            p = line_end + 2;
        }
        
        append_terminator:
        
        if(ctx->write_len + 2 <= sizeof(ctx->write_buf)) {
             ctx->write_buf[ctx->write_len++] = '\r';
             ctx->write_buf[ctx->write_len++] = '\n';
        }
    }

    while(ctx->write_offset < ctx->write_len) {
        int bytes_sent = send(ctx->upstream_fd,ctx->write_buf + ctx->write_offset,ctx->write_len - ctx->write_offset,MSG_NOSIGNAL);

        if(bytes_sent < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            if(errno == EPIPE || errno == ECONNRESET) {

                int current_errors = atomic_fetch_add(&consecutive_upstream_errors, 1) + 1;
                char log_buf[128];
                snprintf(log_buf, sizeof(log_buf), "Origin dropped connection during send(). Strike: %d/%d", current_errors, global_config.max_consecutive_errors);
                log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, log_buf);

                atomic_store(&context_table[ctx->upstream_fd],NULL);
                close(ctx->upstream_fd);
                ctx->upstream_fd = -1;
                atomic_fetch_sub(&active_upstream_connections, 1);
                
                ctx->write_offset = 0;
                ctx->state = STATE_CONNECT_UPSTREAM;
                return;
            }
            perror("upstream send error");
            format_error_response(ctx, 502, "Bad Gateway", NULL);
            return;
        } else if(bytes_sent == 0) {
            format_error_response(ctx, 502, "Bad Gateway", NULL);
            return;
        }

        ctx->write_offset += bytes_sent;
    }
    gettimeofday(&ctx->upstream_send_time,NULL);

    ctx->state = STATE_FETCH_UPSTREAM;
    return;
}

void handle_parse_request(ConnectionContext* ctx) {
    //RATE LIMIT
    /*if(!check_rate_limit(ctx->client_ip)) {
        printf("[Rate Limit] Denied: %s\n", ctx->client_ip);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Rate Limit Exceeded (429)");
        send_error_response(ctx->client_fd, 429, "Too Many Requests. Slow down.",NULL);
        ctx->state = STATE_CLOSE;
        return;
    }*/

    ctx->read_buf[ctx->bytes_read] = '\0';
    int count = sscanf(ctx->read_buf,"%15s %511s %15s",ctx->method,ctx->url,ctx->protocol);
    if(count != 3) {
        format_error_response(ctx, 400, "Invalid HTTP Request Format.",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Malformed Request");
        return;
    }
    
    if(strstr(ctx->protocol,"HTTP/1.0") != NULL) {
        ctx->keep_alive = 0;
    }
    
    char* p = (char*)memchr(ctx->read_buf,'\n',ctx->bytes_read);
    if(p != NULL) {
        p++;
        char* buf_end = ctx->read_buf + ctx->bytes_read;
        while(p < buf_end) {
            char* line_end = (char*)memchr(p,'\r',buf_end - p);
            if(!line_end || line_end + 1 >= buf_end || *(line_end + 1) != '\n') break;

            int line_len = line_end - p;
            if(line_len == 0) break;
            if(strncasecmp(p,"Connection:",11) == 0) {
                char* val = p + 11;
                while(*val == ' ') val++;
                if(strncasecmp(val,"close",5) == 0) ctx->keep_alive = 0;
                else if(strncasecmp(val,"Keep-Alive",10) == 0) ctx->keep_alive = 1;

            }

            if(strncasecmp(p,"If-None-Match:",14) == 0) {
                char* val = p + 14;
                while(*val == ' ') val++;
                char* start = val;

                int len = line_len;
                if(len >= sizeof(ctx->client_if_none_match) - 1) len = sizeof(ctx->client_if_none_match) - 1;
                strncpy(ctx->client_if_none_match,start,len);
                ctx->client_if_none_match[len] = '\0';
            } 

            p = line_end + 2;
        }
    }
    
    //printf("[Proxy] Request: %s %s\n", method, url);
    if (global_config.log_level == LEVEL_INFO) {
        char log_buf[1536];
        char up_host[256]; int up_port;
        get_upstream_info(up_host, sizeof(up_host), &up_port);
        snprintf(log_buf, sizeof(log_buf), "Request: %s %s:%d", ctx->method, up_host, up_port);
        log_event(LEVEL_INFO, ctx->req_id, ctx->client_ip, log_buf);
    }
    
    atomic_fetch_add(&metric_total_requests,1);
    
    if(strcmp(ctx->method,"PURGE") == 0) {
        int purge_status = purge_cache_entry(ctx->url);
        
        if (purge_status == 200) {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"status\": \"purged\", \"path\": \"%s\"}", ctx->url);
        } 
        else if (purge_status == 409) {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"error\": \"File is actively being fetched. Try again.\"}");
        } 
        else {
            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                "{\"error\": \"Not in cache\", \"path\": \"%s\"}", ctx->url);
        }

        ctx->bytes_remaining = 0;
        ctx->file_fd = -1;
        ctx->keep_alive = 0;
        ctx->state = STATE_SEND_RESPONSE_HEADERS;
        return;
    }

    if(strncmp(ctx->method,"GET",3) == 0) {
        if(strcmp(ctx->url,"/stats") == 0) {
            uint64_t hits = metric_cache_hits;
            uint64_t misses = metric_cache_misses;
            uint64_t total = hits + misses;
            double hit_ratio = total > 0 ? ((double)hits / total)* 100.0 : 0.0;
            long uptime = time(NULL) - server_start_time;
            uint64_t cpu = atomic_load(&metric_cpu_usage);

            ctx->write_len = snprintf(ctx->write_buf, sizeof(ctx->write_buf),
                "HTTP/1.1 200 OK\r\n"
                "Content-Type: application/json\r\n"
                "Connection: close\r\n\r\n"
                "{\n"
                "  \"server_uptime_seconds\": %ld,\n"
                "  \"traffic\": {\n"
                "    \"current_rps\": %lu,\n"
                "    \"total_requests\": %lu\n"
                "  },\n"
                "  \"cache\": {\n"
                "    \"hit_ratio_percent\": %.2f,\n"
                "    \"total_hits\": %lu,\n"
                "    \"total_misses\": %lu,\n"
                "    \"tracked_ram_bytes\": %ld\n"
                "  },\n"
                "  \"hardware\": {\n"
                "    \"cpu_usage_percent\": %.2f,\n"
                "    \"memory_rss_kb\": %ld\n"
                "  }\n"
                "}", 
                uptime, metric_current_rps, metric_total_requests, 
                hit_ratio, hits, misses, total_cache_memory,
                cpu / 100.0, metric_memory_rss_kb);
                
                ctx->bytes_remaining = 0;
                ctx->file_fd = -1;
                ctx->keep_alive = 0;
                ctx->state = STATE_SEND_RESPONSE_HEADERS;
                return;
        } 
        ctx->state = STATE_CHECK_CACHE;
        ctx->checkCache = 1;
    } else {
        format_error_response(ctx,501,"Not Implemented",NULL);
        log_event(LEVEL_WARN, ctx->req_id, ctx->client_ip, "Unsupported Method");
    }
    return;
}

void handle_read_request(ConnectionContext* ctx) {
    while(1) {
        int n = recv(ctx->client_fd,ctx->read_buf + ctx->bytes_read,BUFFER - 1 - ctx->bytes_read,MSG_NOSIGNAL);

        if(n < 0) {
            if(errno == EWOULDBLOCK || errno == EAGAIN) {
                return;
            }
            if (errno != ECONNRESET) perror("recv error");
            ctx->state = STATE_CLOSE;
            return;
        } else if(n == 0) {
            //Client disconnected prematurely
            ctx->state = STATE_CLOSE;
            return;
        }
        
        atomic_store(&ctx->last_active,time(NULL));
        ctx->bytes_read += n;
        ctx->read_buf[ctx->bytes_read] = '\0';

        if(strstr(ctx->read_buf,"\r\n\r\n") != NULL) {
            ctx->state = STATE_PARSE_REQUEST;
            return;
        }
    }
}

void* handle_state_machine(void* args) {
    ConnectionContext* ctx = (ConnectionContext*)args;
    if(ctx == NULL) return NULL;

    int state_changed = 1;
    pthread_mutex_lock(&ctx->state_lock);

    while(state_changed) {
        ConnectionState initialState = ctx->state;
        if(ctx->state != STATE_CLOSE) {
            switch(ctx->state) {
                case STATE_READ_REQUEST:
                    handle_read_request(ctx);
                    break;
                case STATE_PARSE_REQUEST:
                    handle_parse_request(ctx);
                    break;
                case STATE_CHECK_CACHE:
                    handle_check_cache(ctx);
                    break;
                case STATE_SEND_CACHE:
                    handle_send_cache(ctx);
                    break;
                case STATE_CONNECT_UPSTREAM:
                    handle_connect_upstream(ctx);
                    break;
                case STATE_WAIT_CONNECT:
                    handle_wait_connect(ctx);
                    break;
                case STATE_SEND_UPSTREAM:
                    handle_send_upstream(ctx);
                    break;
                case STATE_FETCH_UPSTREAM:
                    handle_fetch_upstream(ctx);
                    break;
                case STATE_SEND_RESPONSE_HEADERS:
                    handle_send_response_headers(ctx);
                    break;
            }
        }
        
        state_changed = 0;
        if(ctx->state != initialState && ctx->state != STATE_CLOSE && ctx->state != STATE_WAIT_CACHE) {
            state_changed = 1;
        }
    }
    
    if(ctx->state != STATE_CLOSE && ctx->state != STATE_WAIT_CACHE) {
        uint32_t desired_events = 0;
        int target_fd = -1;
        uint32_t* current_interests_ptr = NULL;

        if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM || ctx->state == STATE_FETCH_UPSTREAM) {
            target_fd = ctx->upstream_fd;
            current_interests_ptr = &ctx->upstream_interests;
            if(ctx->state == STATE_WAIT_CONNECT || ctx->state == STATE_SEND_UPSTREAM) {
                desired_events = EPOLLOUT | EPOLLET;
            } else {
                desired_events = EPOLLIN | EPOLLET;
            }
        } else if(ctx->state == STATE_READ_REQUEST || ctx->state == STATE_SEND_RESPONSE_HEADERS || ctx->state == STATE_SEND_CACHE) {
            target_fd = ctx->client_fd;
            current_interests_ptr = &ctx->client_interests;
            if(ctx->state == STATE_SEND_CACHE || ctx->state == STATE_SEND_RESPONSE_HEADERS) {
                desired_events = EPOLLOUT | EPOLLET;
            } else {
                desired_events = EPOLLIN | EPOLLET;
            }
        }

        if(target_fd != -1 && current_interests_ptr != NULL && desired_events != *current_interests_ptr) {
            struct epoll_event event;
            memset(&event,0,sizeof(event));
            event.data.fd = target_fd;
            event.events = desired_events;
            
            if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, target_fd, &event) == -1) {
                if (errno == ENOENT) {
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, target_fd, &event) == -1) {
                         perror("epoll_ctl add failed during ET switch");
                         ctx->state = STATE_CLOSE;
                    }
                } else {
                    perror("epoll_ctl mod failed in ET switch");
                    ctx->state = STATE_CLOSE; 
                }
            }
            if (ctx->state != STATE_CLOSE) {
                *current_interests_ptr = desired_events;
            }
        }
    }
    
    if(ctx->state == STATE_CLOSE) {
        if(ctx->client_fd != -1) {
            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->client_fd,NULL);
        }

        if(ctx->upstream_fd != -1) {
            epoll_ctl(epoll_fd,EPOLL_CTL_DEL,ctx->upstream_fd,NULL);
        }
    }

    int is_closed = (ctx->state == STATE_CLOSE) ? 1 : 0;
    pthread_mutex_unlock(&ctx->state_lock);

    if(is_closed) {
        pthread_mutex_destroy(&ctx->state_lock);
        free_context(ctx);
    }
    return NULL;
}
