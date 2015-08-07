#include "new_allocator.h"

#include <malloc.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdint.h>

#include "profiling.h"
#include "signal_blocker.h"

int read_object(mem_object_handle object){
	std::lock_guard<std::mutex> _fd_guard(object->vm_info.region->fd_lock);
	int fd = object->vm_info.region->fd;
	lseek(fd, object->vm_info.offset, SEEK_SET);
	read(fd, object->store, object->vm_info.size);

	return 0;
}

int write_object(mem_object_handle object){
	if (object->store == nullptr){
		return -1;
	}

	timer_scope _timer(timers.swap_write);
	std::lock_guard<std::mutex> _fd_guard(object->vm_info.region->fd_lock);
	int fd = object->vm_info.region->fd;
	lseek(fd, object->vm_info.offset, SEEK_SET);
	write(fd, object->store, object->vm_info.size);
	
	return 0;
}