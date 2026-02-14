#ifndef CACHE_H
#define CACHE_H

void get_cache_filename(char*,char*);

int check_cache(char*);

void enforce_cache_capacity();

void  serve_from_cache(int,char*,char*,char*,unsigned long);

void fetch_and_cache(int,int,char*,char*,char*,unsigned long);
#endif