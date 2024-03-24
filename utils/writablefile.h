#ifndef WRITABLEFILE_H
#define WRITABLEFILE_H

#include "slice.h"
#include "../src/status.h"
class WritableFile {
public:
	WritableFile() = default;

	WritableFile(const WritableFile&) = delete;
	WritableFile& operator=(const WritableFile&) = delete;

	~WritableFile();

	Status Append(const Slice& data);
	Status Close();
	Status Flush();
	Status Sync();
};
#endif //WRITABLEFILE_H
