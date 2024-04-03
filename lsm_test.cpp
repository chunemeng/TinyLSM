
#include <iostream>
#include "kvstore.h"
#include <thread>
int main(int argc, char *argv[]) {
	auto* p = new KVStore("./data", "./data/vlog");
	if (argc == 2 && strcmp(argv[1], "-c") == 0) {
		p->reset();
		delete p;
		return 0;
	}
    if (argc == 2 && strcmp(argv[1],"-p") == 0) {
        for (int i = 0; i < 100000;++i) {
            if ((i&1) && !(p->get(i)=="s")) {
                auto k = p->get(i);
                std::cout<<i<<std::endl;
            }
        }
        delete p;
        return  0;
    }
	std::string s = "s";
//    for (int i = 0; i < 409;++i) {
//        p->put(i, s);
//    }
//    for (int i = 0; i < 409;++i) {
//        p->put(i, s);
//    }
//    for (int i = 0; i < 409;++i) {
//        p->put(i, s);
//    }
	for (int i = 0; i < 100000; i++) {
		p->put(i,s);
	}
	for (int i = 0; i < 100000; i+=2) {
		auto f = p->del(i);

        if (i > 2448 && !p->get(2448).empty() || i > 5304 && !p->get(5304).empty() || i > 1632 && !p->get(1632).empty()) {
            int h = 8;
        }
	}
	for (int i = 0; i < 100000; i++) {
		if (!(i & 1) && !p->get(i).empty()) {
            p->get(i);
			std::cout << i << std::endl;
		}
	}
	delete p;
}