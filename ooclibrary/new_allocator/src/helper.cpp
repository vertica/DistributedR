#include "helper.h"

// #include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <uuid/uuid.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>


// TODO: error

void touch_file(const char* path){
	int fd = open(path, O_RDWR | O_CREAT, S_IRWXU);
	if (fd < 0){
		ERROR_PRINT("touch_file %s failed\n", path);
	}
	close(fd);
}


void truncate_file(const char* path, size_t size){



#ifdef O_DIRECT_TRUNCATE

	int trunc_fd = open(path, O_RDWR | O_DIRECT, S_IRWXU);
	if (trunc_fd < 0){
		ERROR_PRINT("truncate_file %s failed, error: %s\n", path, strerror(errno));
	}

	char buffer[4096];
	memset(buffer, 0, 4096);
	for (size_t i = 0; i < size; i += 4096){
		write(trunc_fd, buffer, 4096);		
	}

	close(trunc_fd);
	
#else
	int fd = open(path, O_RDWR, S_IRWXU);

	if (fd < 0){
		ERROR_PRINT("open %s failed, error: %s\n", path, strerror(errno));
	}
	
	int truncate_ret = ftruncate(fd, size);

	if (truncate_ret != 0){
		ERROR_PRINT("truncate %s failed, error: %s\n", path, strerror(errno));
	}

#endif
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