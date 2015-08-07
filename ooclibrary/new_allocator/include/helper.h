#ifndef __HELPER_H_
#define __HELPER_H_

#include "common.h"


char htoc(int hex);
void generate_uuid_string(char* strbuf);
void touch_file(const char* path);
void truncate_file(const char* path, size_t size);
#endif