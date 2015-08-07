#include "timer.h"

const size_t repeat = 32768;

int main(){
	timer all;
	all.start();
	timer local;
	for (size_t i = 0; i < repeat; i++){
		// timer_scope _scope(local);
		local.start();
		local.hit();
		local.stop();
	}
	all.stop();
	printf("all: %zu, %f\n", all.elapsed(), all.elapsed() * 1.0 / repeat);
	printf("local: %zu, %f\n", local.elapsed(), local.elapsed() * 1.0 / repeat);
}