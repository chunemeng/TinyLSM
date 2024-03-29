
#include <iostream>
#include "kvstore.h"

int main() {
	KVStoreAPI* p = new KVStore("./data", "./data/vlog");
	p->reset();
}