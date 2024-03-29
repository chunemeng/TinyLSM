#ifndef VERSION_H
#define VERSION_H

#include <string>
#include <cstdint>
#include "../utils/file.h"
#include "../utils/coding.h"
#include "../utils/filename.h"
namespace LSMKV {
	struct Version {
		explicit Version(const std::string& dbname) : filename(VersionFileName(dbname)) {
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
			close();
		}

		inline void close() const {
			WritableFile* file;
			NewWritableFile(filename, &file);
			char buf[32];
			EncodeFixed64(buf, fileno);
			EncodeFixed64(buf + 8, timestamp);
			EncodeFixed64(buf + 16, head);
			EncodeFixed64(buf + 24, tail);
			file->Append(Slice(buf,32));
			file->Close();
			delete file;
		}

		void reset() {
			fileno = 0;
			timestamp = 0;
			head = 0;
			tail = 0;
		}

		std::string filename;
		uint64_t fileno = 0;
		uint64_t timestamp = 0;
		uint64_t head = 0;
		uint64_t tail = 0;
	};
}

#endif //VERSION_H
