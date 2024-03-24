#ifndef BUILDER_H
#define BUILDER_H

#include <string>
#include "status.h"
class Iterator;

Status BuildTable(const std::string& dbname, Iterator* iter);

#endif //BUILDER_H
