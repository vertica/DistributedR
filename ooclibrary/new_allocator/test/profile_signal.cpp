#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdio.h>
#include "timer.h"

const size_t page_size = 4UL * 1024UL;

void handler0(int sig, siginfo_t *si, void *unused){
	void* ptr = reinterpret_cast<void*>(reinterpret_cast<size_t>(si->si_addr) & ~(page_size - 1));
	// printf("Called: %p %p\n", ptr, si->si_addr);
	mprotect(ptr, page_size, PROT_READ | PROT_WRITE);
	// usleep(10);
	return;
}

static const size_t repeat = 32768;
static size_t counter = repeat;

void handler1(int sig, siginfo_t *si, void *unused){
	counter--;
	// printf("Triggered\n");
	if (counter == 0){
		void* ptr = reinterpret_cast<void*>(reinterpret_cast<size_t>(si->si_addr) & ~(page_size - 1));
		mprotect(ptr, page_size, PROT_READ | PROT_WRITE);
	}
	// printf("Called: %p %p\n", ptr, si->si_addr);
	return;
}

void handler3(int sig, siginfo_t *si, void *unused){
	printf("Triggered\n");
	unsigned char* ptr = reinterpret_cast<unsigned char*>(reinterpret_cast<size_t>(si->si_addr) & ~(page_size - 1));
	*ptr = *ptr;
	return;
}

timer all;

int mode0(){
	all.start();
	void* ptr = mmap(NULL, repeat * page_size, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
	printf("ptr: %p\n", ptr);
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sigaddset(&sa.sa_mask, SIGSEGV);
	sa.sa_sigaction = handler0;

	sigaction(SIGSEGV, &sa, NULL);

	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	printf("total: %zu\n", repeat);
	for (size_t i = 0; i < repeat; i++){
		// if (i % 256 == 0){
			// printf("\t%zu\n", i);
		// }
		cptr[i * page_size] = cptr[i * page_size];
	}
	all.stop();
	printf("avergae: %f\n", all.elapsed() * 1.0f / repeat);
	return 0;
}

int mode1(){
	all.start();
	void* ptr = mmap(NULL, page_size, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
	printf("ptr: %p\n", ptr);
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigfillset(&sa.sa_mask);
	sa.sa_sigaction = handler1;

	sigaction(SIGSEGV, &sa, NULL);

	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	printf("total: %zu\n", repeat);

	*cptr = *cptr;
	all.stop();
	printf("avergae: %f\n", all.elapsed() * 1.0f / repeat);
	return 0;
}

int mode2(){
	all.start();
	void* ptr = mmap(NULL, repeat * page_size, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
	printf("ptr: %p\n", ptr);
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigfillset(&sa.sa_mask);
	sa.sa_sigaction = handler0;

	sigaction(SIGSEGV, &sa, NULL);

	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	printf("total: %zu\n", repeat);

	for (size_t i = 0; i < repeat; i++){
		// if (i % 256 == 0){
			// printf("\t%zu\n", i);
		// }
		mprotect(&cptr[i * page_size], page_size, PROT_READ | PROT_WRITE);
		// cptr[i * page_size] = cptr[i * page_size];
	}

	all.stop();
	printf("avergae: %f\n", all.elapsed() * 1.0f / repeat);
	return 0;
}

int mode3(){
	all.start();
	void* ptr = mmap(NULL, page_size, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0);
	printf("ptr: %p\n", ptr);
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sa.sa_sigaction = handler3;

	sigaction(SIGSEGV, &sa, NULL);

	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	printf("total: %zu\n", repeat);

	*cptr = *cptr;
	all.stop();
	printf("avergae: %f\n", all.elapsed() * 1.0f / repeat);
	return 0;
}

int mode4(){
	all.start();
	void* ptr = mmap(NULL, repeat * page_size, PROT_NONE , MAP_PRIVATE | MAP_ANON, -1, 0);
	printf("ptr: %p\n", ptr);
	struct sigaction sa;
	sa.sa_flags = SA_SIGINFO;
	sigemptyset(&sa.sa_mask);
	sigaddset(&sa.sa_mask, SIGSEGV);
	sa.sa_sigaction = handler0;

	sigaction(SIGSEGV, &sa, NULL);

	unsigned char* cptr = static_cast<unsigned char*>(ptr);
	printf("total: %zu\n", repeat);
	for (size_t i = 0; i < repeat; i++){
		// if (i % 256 == 0){
			// printf("\t%zu\n", i);
		// }
		mprotect(ptr, repeat * page_size, PROT_READ | PROT_WRITE);
	}
	all.stop();
	printf("avergae: %f\n", all.elapsed() * 1.0f / repeat);
	return 0;
}


int main(int argc, char* argv[]){
	int choice = argv[1][0] - '0';
	if (choice == 0){
		return mode0();
	}else if (choice == 1){
		return mode1();
	}else if (choice == 2){
		return mode2();
	}else if (choice == 3){
		return mode3();
	}else if (choice == 4){
		return mode4();
	}


}
