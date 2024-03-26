#ifndef VERSION_H
#define VERSION_H

#include <string>
#include <cstdint>
#include "../utils/file.h"
#include "../utils/coding.h"
#include "../utils/filename.h"
namespace LSMKV {
	class Version {
	public:
		Version(const std::string& dbname) : filename(VersionFileName(dbname)) {
			// read from current file
			if (FileExists(filename)) {
				RandomReadableFile* file;
				NewRandomReadableFile(filename, &file);
				Slice slice;
				char buf[32];
				file->Read(0, 32, &slice, buf);
				if (slice.size() == 32) {
					fileno = DecodeFixed64(buf);
					timestamp = DecodeFixed64(buf + 8);
					head = DecodeFixed64(buf + 16);
					tail = DecodeFixed64(buf + 24);
				}
			}
		}
		~Version(){
			// write into current file
			WritableFile* file;
			NewWritableFile(filename, &file);
			char buf[32];
			EncodeFixed64(buf, fileno);
			EncodeFixed64(buf + 8, timestamp);
			EncodeFixed64(buf + 16, head);
			EncodeFixed64(buf + 24, tail);
			file->Append(buf);
			file->Close();
			delete file;
		}
		uint64_t Fileno() const {
			return fileno;
		}
		uint64_t Timestamp() const {
			return timestamp;
		}
		uint64_t Head() const {
			return head;
		}
		uint64_t Tail() const {
			return tail;
		}
		void reset() {
			fileno = 0;
			timestamp = 0;
			head = 0;
			tail = 0;
		}

	private:
		std::string filename;
		uint64_t fileno = 0;
		uint64_t timestamp = 0;
		uint64_t head = 0;
		uint64_t tail = 0;
	};
}

#endif //VERSION_H
