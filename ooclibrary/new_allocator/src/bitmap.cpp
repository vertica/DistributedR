#include "bitmap.h"

#include <stdlib.h>

bitmap::bitmap(size_t size){
	_bytes = malloc((size + 8 - 1) / 8);
}

bitmap::~bitmap(){
	free(_bytes);
}

bitmap::get(size_t index){
	return _bytes[index / 8] >> (index & 0x7);
}

bitmap::set(size_t index, int bit){
	_bytes[index / 8] &= ~ (1 << (index & 0x7));
	_bytes[index / 8] |= ((bit && 1) << (index & 0x7));
}