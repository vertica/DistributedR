#include "helper.h"
#include "common.h"

#include <stdio.h>

/**
 * touch and truncate file
 * @param  dir  directory
 * @param  name file name
 * @param  size minimum file size
 * @return      handle object
 */
mmap_handle touch_file(const char* dir, const char* name, size_t size, timer& truncate_timer){
	char buf[FILE_HANDLE_PATH_MAX];
	snprintf(buf, FILE_HANDLE_PATH_MAX, "%s/%s", dir, name);

	truncate_timer.start();
#ifdef O_DIRECT_TRUNCATE

	int trunc_fd = open(buf, O_RDWR | O_CREAT | O_DIRECT, S_IRWXU);
	if (trunc_fd < 0){
		ERROR_PRINT("open failed, error: %s\n", strerror(errno));
	}

	char buffer[4096];
	memset(buffer, 0, 4096);
	for (size_t i = 0; i < size; i += 4096){
		write(trunc_fd, buffer, 4096);		
	}

	close(trunc_fd);

	int fd = open(buf, O_RDWR, S_IRWXU);
	
#else
	int fd = open(buf, O_RDWR | O_CREAT, S_IRWXU);
	
	int truncate_ret = ftruncate(fd, size);

	if (truncate_ret != 0){
		ERROR_PRINT("truncate failed, error: %s\n", strerror(errno));
	}

#endif
	truncate_timer.stop();

	if (fd < 0){
		ERROR_PRINT("open failed, error: %s\n", strerror(errno));
	}

	mmap_handle handle;
	handle.fd = fd;
	handle.offset = 0;
	return handle;
}

/**
 * hex to char
 * @param  hex hex value
 * @return     character
 */
char htoc(int hex){
	if (hex >= 0 && hex < 10){
		return 0x30 + hex;
	}else if (hex >= 10 && hex < 16){
		return 0x41 + (hex - 10);
	}else{
		return '\1';
	}
}

int is_big_endian(){
    union {
        int i;
        char c[4];
    } bint = {0x01020304};

    return (bint.c[0] == 1) ? 1 : -1; 
}

/**
 * generate uuid string in buffer
 * @param strbuf buffer
 */
void generate_uuid_string(char* strbuf){
	uuid_t buf;
	uuid_generate_time(buf);
	size_t i;
	for (i = 0; i < 16; i++){
		strbuf[2*i] = htoc(buf[i] % 16);
		strbuf[2*i + 1] = htoc(buf[i] / 16);
	}
	strbuf[32] = '\0';
}

void print_memory_usage(){
#ifndef NOTEST

	FILE* f = fopen("/proc/self/statm", "r");
	if (f == NULL){
		return;
	}

	size_t size;
	fscanf(f, "%zu", &size);
	fclose(f);

	TEST_PRINT("Current virtual memory space: %zuM\n", size * 4 / 1024);
#endif
}

size_t get_timestamp_milli(){
	struct timeval tm;
	gettimeofday(&tm, NULL);
	return tm.tv_sec * 1000 + tm.tv_usec / 1000;
}