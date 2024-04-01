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
	Status BuildTable(const std::string& dbname, Version* v, Iterator* iter, FileMeta& meta, KeyCache* kc) {
		//	meta->file_size = 0;
		iter->seekToFirst();

		std::string vlog_buf;
//		key_buf.reserve(1024 * 8);
		auto vLogBuilder = new VLogBuilder();\
        char* key_buf;
		uint64_t value_size = 0, key_offset;
		uint64_t level = FindLevels(dbname, v);
		Option op;
		bool compaction = false;
		std::string fname = SSTFileName(LevelDirName(dbname, level), v->fileno);
		uint64_t head_offset = v->head;
		const char* tmp = "~DELETED~";

		Slice tombstone = Slice(tmp, 9);
		if (iter->hasNext()) {
			WritableFile* file;
			WritableFile* vlog;
			Status s = NewWritableFile(fname, &file);
			NewAppendableFile(VLogFileName(dbname), &vlog);

			if (!s.ok()) {
				return s;
			}
			v->AddNewLevelStatus(level, v->fileno);
			if (v->LevelFull(level)) {
				key_buf = kc->ReserveCache(meta.size * 20 + 32, v->fileno);
				key_offset = 32;
				op.isFilter = false;
				compaction = true;
			} else {
				key_buf = kc->ReserveCache(meta.size * 20 + 8224, v->fileno);
				key_offset = 8224;
			}

			meta.smallest = iter->key();
			uint64_t key;
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
//				size_t key_offset = key_buf.size();
//				key_buf.append(20,'\0');
				EncodeFixed64(key_buf + key_offset, key);
				EncodeFixed64(key_buf + key_offset + 8, head_offset);
				EncodeFixed32(key_buf + key_offset + 16, value_size);
				head_offset += value_size ? value_size + 15 : 0;
				key_offset += 20;
			}
			meta.largest = key;

			EncodeFixed64(key_buf, v->timestamp++);
			EncodeFixed64(key_buf + 8, meta.size);
			EncodeFixed64(key_buf + 16, meta.smallest);
			EncodeFixed64(key_buf + 24, meta.largest);
			if (!compaction) {
				CreateFilter(key_buf + 8224, meta.size, 20, key_buf + 32);
				file->Append(Slice(key_buf, 8224 + meta.size * 20));
			}

			kc->PushCache(key_buf, op);

			// need multi thread
			vlog->Append(vLogBuilder->plain_char());
			vlog->Close();

			file->Close();
			v->head = head_offset;
			delete file;
			delete vlog;
			delete vLogBuilder;
//			delete[] sst_meta;
			if (compaction) {
				SSTCompaction(level, v->fileno, v, kc);
			}
			v->fileno++;
			return Status::OK();
		}
		return Status::IOError("Iter is empty");
	}
	Status SSTCompaction(uint64_t level, uint64_t file_no, Version* v, KeyCache* kc) {
		// Need To Add New level
		std::vector<uint64_t> new_files;

		//Need to be rm and earse in version
		std::vector<uint64_t> old_files[2] = { std::vector<uint64_t>(), std::vector<uint64_t>() };
		std::vector<Slice> need_to_write;
		if (v->NeedNewLevel(level)) {
			v->AddNewLevel();
		}
		kc->CompactionSST(level,
			file_no,
			old_files,
			new_files,
			need_to_write,
			v->GetLevelStatus(level),
			v->GetLevelStatus(level + 1));
		v->ClearLevelStatus(level, old_files);

	}
	Status MoveToNewLevel(uint64_t level, std::vector<uint64_t>& new_files, Version* v) {
		std::string dbname = v->DBName();
		for (auto& it : new_files) {
			utils::mvfile(SSTFilePath(dbname, level, it)
				, SSTFilePath(dbname, level + 1, it));
		}
	}

	uint64_t FindLevels(const std::string& dbname, Version* v) {
		return 0;
	}

}

