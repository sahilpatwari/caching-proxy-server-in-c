#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/socket.h>

void send_error_response(int client_fd,int status_code,char *message) {
    char response[1024];
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

    snprintf(response,sizeof(response),
             "HTTP %d %s\r\n"
             "Content-Type:text/html\r\n"
             "Content-Length:%ld\r\n"
             "Connection:close\r\n"
             "\r\n"
             "<html><body><h1>%d %s</h1><p>%s</p></body></html>",
             status_code,status_text,strlen(message) + 50,status_code,status_text,message);
    
    if(send(client_fd,response,strlen(response),0) == -1) {
       perror("Failed to send error response to client");
    }
}