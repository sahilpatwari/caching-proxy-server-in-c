CC = gcc
CFLAGS = -O3 -D_GNU_SOURCE
LDFLAGS = -lpthread
TARGET = custom_proxy_cache

# Source files
SRCS = http_proxy.c cache.c  proxy_log.c rate_limiter.c  errors.c config.c

# Object files
OBJS = $(SRCS:.c=.o)

.PHONY: all clean debug install

# Default build
all: $(TARGET)

# Build the proxy
$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS)

# Compile C files into object files
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Debug build with sanitizers and no optimization
debug: CFLAGS = -Wall -Wextra -g -O0 -fsanitize=address -D_GNU_SOURCE
debug: LDFLAGS += -fsanitize=address
debug: clean $(TARGET)

# Clean build artifacts
clean:
	rm -f $(OBJS) $(TARGET)

# Simple install target (optional)
install: $(TARGET)
	install -d /usr/local/bin
	install -m 755 $(TARGET) /usr/local/bin/
