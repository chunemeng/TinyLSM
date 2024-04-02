
#include <iostream>
#include "kvstore.h"

int main(int argc, char *argv[]) {
	auto* p = new KVStore("./data", "./data/vlog");
	if (argc == 2 && strcmp(argv[1], "-c") == 0) {
		p->reset();
		delete p;
		return 0;
	}
	std::string s = "s";
	for (int i = 0; i < 1000; i++) {
		p->put(i,std::string(i+1,'s'));
	}
//	for (int i = 0; i < 1000; i++) {
//		p->del(i &2);
//	}
//	for (int i = 0; i < 1000; i++) {
//		if (p->del(i &2)) {
//			std::cout << i << std::endl;
//		}
//	}
	delete p;
}