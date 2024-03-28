#include "builder.h"
#include "../utils/slice.h"
#include "../utils/iterator.h"
#include "../utils/filename.h"
#include "../utils/file.h"
#include "../utils/coding.h"
#include "version.h"
#include "vlogbuilder.h"
#include "../utils/bloomfilter.h"
namespace LSMKV {
	Status BuildTable(const std::string& dbname, Version* v, Iterator* iter) {
		//	meta->file_size = 0;
		iter->seekToFirst();
		FileMeta meta;

		std::string key_buf, vlog_buf;
		key_buf.reserve(1024 * 8);
		auto vLogBuilder = new VLogBuilder();
		std::string fname = SSTFileName(LevelDirName(dbname, FindLevels(dbname,v)), v->fileno);
		uint64_t head_offset = v->head;
		char tmp[10] = "~DELETED~";
		Slice tombstone = Slice(tmp,10);
		if (iter->hasNext()) {
			WritableFile* file;
			WritableFile* vlog;
			Status s = NewWritableFile(fname, &file);
			NewAppendableFile(VLogFileName(dbname), &vlog);

			if (!s.ok()) {
				return s;
			}

			meta.smallest = iter->key();
			uint64_t key;
			uint64_t value_size = 0;
			Slice val;
			for (; iter->hasNext(); iter->next()) {
				key = iter->key();
				val = iter->value();
				if (val == tombstone) {
					value_size = 0;
				} else {
					vLogBuilder->Append(key, val);
					value_size = val.size();
				}
				size_t key_offset = key_buf.size();
				key_buf.append(20,'\0');
				EncodeFixed64(&key_buf[key_offset], key);
				EncodeFixed64(&key_buf[key_offset + 8], head_offset);
				EncodeFixed32(&key_buf[key_offset + 16], value_size);
				head_offset += value_size ? value_size + 15 : 0;
			}
			meta.largest = key;

			char* sst_meta = new char[8224];
			EncodeFixed64(sst_meta, v->timestamp++);
			EncodeFixed64(sst_meta + 8, key_buf.size() / 20);
			EncodeFixed64(sst_meta + 16, meta.smallest);
			EncodeFixed64(sst_meta + 24, meta.largest);
			CreateFilter(&key_buf[0], key_buf.size() / 20, 20, sst_meta + 32);

			// need thread
			vlog->Append(vLogBuilder->plain_char());
			vlog->Close();

			file->Append(Slice(sst_meta, 8224));
			file->Append(Slice(key_buf));
			file->Close();
			v->head = head_offset;
			delete file;
			delete vlog;
			delete vLogBuilder;
			delete[] sst_meta;

			return Status::OK();
		}
		return Status::IOError("Iter is empty");
	}
	uint64_t FindLevels(const std::string& dbname, Version* v) {
		return 0;
	}
}

