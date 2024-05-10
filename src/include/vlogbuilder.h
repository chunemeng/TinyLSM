#ifndef VLOGBUILDER_H
#define VLOGBUILDER_H

#include <string>
#include "../../utils/slice.h"
#include "../../utils.h"
#include "../../utils/coding.h"
namespace LSMKV{
class VLogBuilder {

public:
	void Append(uint64_t& key, Slice& value) {
		vlog.append(magic,1);
		size_t offset = vlog.size();
		vlog.append(14, '\0');
		vlog.append(value.data(), value.size());
		EncodeFixed64(&vlog[offset + 2], key);
		EncodeFixed32(&vlog[offset + 10], value.size());
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
