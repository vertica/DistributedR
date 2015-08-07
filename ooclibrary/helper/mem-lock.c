#include <malloc.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define CHUNK_SIZE (256UL * 1024UL * 1024UL)
void* mem_ptrs[4096];

int main(int argc, char** argv){
	size_t size = (size_t) atol(argv[1]) * 1024UL * 1024UL;
	size_t release = (size + CHUNK_SIZE - 1) / CHUNK_SIZE;

	int i = 0;
	void* mem;
	while (1){
		mem = mmap(0, CHUNK_SIZE, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
		if (mem == MAP_FAILED){
			printf("mmap error: %d %d\n", i, errno);
			break;
		}

		printf("mmap %d\n", i);

		int ret = mlock(mem, CHUNK_SIZE);
		
		if (ret != 0){
//			printf("mlock error: %d %d %d\n", ret, i, errno);
			break;
		}

		printf("mlock %d\n", i);

		memset(mem, 0, CHUNK_SIZE);

		mem_ptrs[i] = mem;
		i++;
	}

	printf("Releasing\n");
	sleep(1);

	int j;
	for (j = 0; j < release && j < i; j++){
		munlock(mem_ptrs[j], CHUNK_SIZE);
		munmap(mem_ptrs[j], CHUNK_SIZE);
	}

	printf("Released\n");

	sleep(atol(argv[2]));
}
