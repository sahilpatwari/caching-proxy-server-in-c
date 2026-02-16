#ifndef RATE_LIMITER_H
#define RATE_LIMITER_H

void init_rate_limiter();

int check_rate_limit(char*);

void destroy_rate_limiter();
#endif