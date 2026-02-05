#ifndef CACHE_H
#define CACHE_H

void get_cache_filename(char*,char*);

int check_cache(char*);

void enforce_cache_capacity();
#endif