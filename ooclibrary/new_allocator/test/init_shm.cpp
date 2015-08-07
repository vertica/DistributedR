#include "new_allocator.h"
#include "wrapper.h"

#define ENV_NAME "MMAP_SHM_FILE"
#define NAME "coord_shm"

int main(int argc, char** argv){
	size_t shm_size = 0;
	if (argc == 1){
		shm_size = 0;
	}else{
		shm_size = atol(argv[1]) * 1024UL * 1024UL;
	}

	multi_reservation_object_allocator_t::shared_memory_pool_t::init(NAME, shm_size);
	printf("export %s=%s\n", ENV_NAME, NAME);
}