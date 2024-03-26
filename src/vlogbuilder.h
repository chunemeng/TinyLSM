#ifndef VLOGBUILDER_H
#define VLOGBUILDER_H

#include <string>
#include "../utils/slice.h"
#include "../utils.h"
#include "../utils/coding.h"
namespace lsm{
class VLogBuilder {

public:
	void Append(Slice& key, Slice& value) {
		vlog.append(magic,1);
		size_t offset = vlog.size();
		vlog.append(2, '\0');
		vlog.append(key.data(), 8);
		vlog.append(4, '\0');
		vlog.append(value.data());
		EncodeFixed32(&vlog[offset + 10], key.size());
		EncodeFixed16(&vlog[offset], utils::crc16(&vlog[offset + 2], vlog.size() - offset - 2));
	}
	Slice plain_char() {
		return { vlog.data(), vlog.size() };
	}
private:
	// magic 0xff
	const char* magic = "\377";
	std::string vlog;
};
}
#endif //VLOGBUILDER_H
