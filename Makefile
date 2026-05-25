CC = gcc
CFLAGS = -O3 -D_GNU_SOURCE -Iinclude
LDFLAGS = -lpthread
TARGET = build/custom_proxy_cache

# Source files
SRCS = src/main.c src/cache.c  src/proxy_log.c src/rate_limiter.c src/config.c src/reactor.c src/connection_context.c \
       src/upstream.c src/http_handler.c src/workers.c

# Object files
OBJS = $(SRCS:src/%.c=build/%.o)

.PHONY: all clean debug install

# Default build
all: build $(TARGET)

# Build the proxy
build:
	mkdir -p build


$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS)

# Compile C files into object files
build/%.o: src/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Debug build with sanitizers and no optimization
debug: CFLAGS = -Wall -Wextra -g -O0 -fsanitize=address -D_GNU_SOURCE -Iinclude
debug: LDFLAGS += -fsanitize=address
debug: clean all

# Clean build artifacts
clean:
	rm -rf build/

# Simple install target (optional)
install: $(TARGET)
	install -d /usr/local/bin
	install -m 755 $(TARGET) /usr/local/bin/
