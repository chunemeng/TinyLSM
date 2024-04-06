
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
//        LSMKV::WritableFile *file;
//        LSMKV::NewWriteAtStartFile("./data/.current",&file);
//        file->Append(LSMKV::Slice("Hello World!Hello World!Hello World!Hello World!Hello World!",40));
//        file->Close();
//        delete file;
//
//        utils::de_alloc_file("./data/.current",3,7);
//        delete p;
        p->reset();
        for (int i = 0; i < 1000;++i) {
            p->put(i,"s");
        }
        p->gc(1024);
//        delete p;
        for (int i = 0; i < 1000;++i) {
            if (p->get(i) != "s") {
                int f = 7;
            }
        }
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
    for (int i = 0; i < 100000; i++) {
        p->get(i);
    }
	for (int i = 0; i < 100000; i+=2) {
		auto f = p->del(i);

	}
	for (int i = 0; i < 100000; i++) {
		if (!(i & 1) && !p->get(i).empty()) {
            p->get(i);
			std::cout << i << std::endl;
		}
	}
	delete p;
}