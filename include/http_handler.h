#ifndef HTTP_HANDLER_H
#define HTTP_HANDLER_H

#include"connection.h"

extern void* handle_state_machine(void* args);
extern void format_error_response(ConnectionContext* ctx,int status_code,const char *message,const char* extra_headers);
#endif