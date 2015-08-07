#include "meta_allocator.h"

#include <vector>
#include <list>

int main(){
	std::vector<int, meta_allocator<int> > vec1(1000000, 0);
	std::vector<int, meta_allocator<int> > vec2(1000000, 0);
	std::vector<int, meta_allocator<int> > vec3(1000000, 0);

	std::list<int, meta_allocator<int> > list;
	list.push_back(1);

	meta_allocator<int> allocator;
	void* p = allocator.allocate(50);
}