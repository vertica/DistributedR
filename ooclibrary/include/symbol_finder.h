#ifndef __SYMBOL_FINDER_H_
#define __SYMBOL_FINDER_H_

#define _GNU_SOURCE

#include <stdio.h>
#include <dlfcn.h>

#define CALL_LIB_FUNC(name) do { \
	void (*name ## _func)(); \
	name ## _func = (void (*)()) dlsym(RTLD_DEFAULT, #name); \
	printf("%p\n", (void*) name ## _func); \
	if (name ## _func != NULL){ \
		name ## _func(); \
	} \
} while (0)


#define CALL_LIB_FUNC_SIGN(name, arg_sign, arg_to_call) do { \
	void (*name ## _func)(arg_sign); \
	name ## _func = (void (*)(arg_sign)) dlsym(RTLD_DEFAULT, #name); \
	printf("%p\n", (void*) name ## _func); \
	if (name ## _func != NULL){ \
		name ## _func(arg_to_call); \
	} \
} while (0)

#define DEF_CALL_LIB_FUNC(name, to_call) void name(){ \
	void (*name ## _func)(); \
	name ## _func = (void (*)()) dlsym(RTLD_DEFAULT, #to_call); \
	printf("%p\n", (void*) name ## _func); \
	if (name ## _func != NULL){ \
		name ## _func(); \
	} \
}

#endif