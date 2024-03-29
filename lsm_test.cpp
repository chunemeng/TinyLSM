
#include <iostream>
#include "kvstore.h"

int main(int argc, char *argv[]) {
	auto* p = new KVStore("./data", "./data/vlog");
	if (argc == 2 && strcmp(argv[1], "-c") == 0) {
		p->reset();
		return 0;
	}
	std::string s = "s";
	for (int i = 0; i < 1000; i++) {
		p->put(i,s);
	}
	for (int i = 0; i < 1000; i++) {
		p->get(i);
	}
	delete p;
}