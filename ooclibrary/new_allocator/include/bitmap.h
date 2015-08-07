#ifndef __BITMAP_H__
#define __BITMAP_H__

class bitmap {
	unsigned char* _bytes;

	explicit bitmap(size_t size);
	void get(size_t index);
	void set(size_t index, int bit);
};

#endif