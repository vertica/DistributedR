#include <stdio.h>
#include "gprintf.h"

int f(int a, ...){
	va_list valist;
	va_start(valist, a);

  // int* ptr = (int*) ((unsigned char*) &a + sizeof(int));

  int i;
  // printf("%p\n", &a);


  for (i = 0; i < 5; i++){
  	printf("%zu\n", va_arg(valist, size_t));
    // printf("%p, %x\n", (ptr - 10 + i), *(ptr - 10 + i));
  }
  // // fflush(fd);
  // // // int ret = general_printf(output, fd, format, ptr);
  // // fflush(fd);
  // return 0;
}

int main(){
	size_t a = 0x1020304050607080UL;
	void* ptr = &a;

	printf("%p, %zu\n", ptr, a);
	gprintf(stderr, "%p, %zu\n", ptr, 0x1020304050607080UL);
	// f(1, 0x1020304050607080UL, 0x1020304050607090UL, 0x10203040506070A0UL, 0x10203040506070B0UL, 0x10203040506070C0UL);
}