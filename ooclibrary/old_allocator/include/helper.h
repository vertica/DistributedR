#ifndef __HELPER_H_
#define __HELPER_H_

#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <uuid/uuid.h>
#include <sys/mman.h>
#include <sys/time.h>

#include "common.h"
#include "timer.h"


char htoc(int hex);

void print_memory_usage();

inline void bfprint(FILE* fd, const char* buf, size_t length){
	fflush(fd);
	fwrite(buf, 1UL, length, fd);
}

int is_big_endian();

template <typename type>
inline void bfprint_hex(FILE* fd, const type& val){

	const char* chars = reinterpret_cast<const char*>(&val);
	char buffer[sizeof(type) * 2];

	if (is_big_endian()){
		for (size_t i = 0; i < sizeof(type); i++){
			char c = chars[i];
			buffer[2 * i] = htoc(c / 16);
			buffer[2 * i + 1] = htoc(c % 16);
		}
	}else{
		for (size_t i = 0; i < sizeof(type); i++){
			char c = chars[sizeof(type) - 1 - i];
			buffer[2 * i] = htoc(c / 16);
			buffer[2 * i + 1] = htoc(c % 16);
		}
	}

	bfprint(fd, buffer, sizeof(buffer));
}

#define RE_TEST_PRINT(str, ...) bfprint(stderr, str, sizeof(str))

size_t get_timestamp_milli();
mmap_handle touch_file(const char* dir, const char* name, size_t size, timer& truncate_timer);
void generate_uuid_string(char* strbuf);
#endif