#ifndef BUILDER_H
#define BUILDER_H


//	struct Options;

#include <string>
class Iterator;
//class TableCache;

int BuildTable(const std::string& dbname, Iterator* iter);

#endif //BUILDER_H
