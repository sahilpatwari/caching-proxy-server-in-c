#ifndef PROXY_LOG_H
#define PROXY_LOG_H

// Safe Enum: Avoids collision with <syslog.h> macros (LOG_INFO, etc.)
typedef enum {
    LEVEL_DEBUG = 0,  // Verbose: Headers, Variables, Connection details
    LEVEL_INFO  = 1,  // Standard: Requests, Start/Stop events
    LEVEL_WARN  = 2,  // Warning: Cache full, High latency (Recoverable)
    LEVEL_ERROR = 3   // Critical: Malloc failure, Bind failed (Non-recoverable)
} LogLevel;

void init_log(LogLevel);

void log_event(LogLevel, uint64_t, const char*, const char*);

void close_log();
#endif