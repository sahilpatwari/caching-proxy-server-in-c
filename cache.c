#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<string.h>
#include<time.h>

#include"cache.h"
//djb2 hash algorithm
unsigned long hash_url(char* url) {
    char* str = url;
    unsigned long hash = 5381;
    int c;
    while(c = *str++) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

void get_cache_filename(char* url,char* buffer) {
    unsigned long hash= hash_url(url);
    sprintf(buffer,"cache/%lu",hash);
}

int check_cache(char* filename) {
    struct stat buffer;

    if(stat(filename,&buffer) != 0) {
        return 0;
    }
    time_t now = time(NULL);
    double seconds_passed = difftime(now,buffer.st_mtime);

    printf("File Age: %.0f seconds passed\n",seconds_passed);

    if(seconds_passed > 60) {
        printf("Deleting old file....\n");
        return 0;
    }
    return 1;
}

