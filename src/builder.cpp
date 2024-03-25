#include "builder.h"
#include "../utils/slice.h"
#include "../utils/iterator.h"
#include "../utils/filename.h"
#include "../utils/writablefile.h"
#include "../utils/coding.h"
#include "version.h"
#include "vlogbuilder.h"
#include "../utils/bloomfilter.h"

Status BuildTable(const std::string& dbname, Version* v, Iterator* iter, FileMeta* meta) {
//	meta->file_size = 0;
	iter->seekToFirst();
	// magic 0xff
	char* magic = "\377";

	std::string key_buf, vlog_buf;
	key_buf.reserve(1024 * 8);
	auto* vLogBuilder = new VLogBuilder();
	std::string fname = SSTFileName(dbname, v->fileno);
	uint64_t head_offset = v->head;
	if (iter->hasNext()) {
		WritableFile* file;
		WritableFile* vlog;
		Status s = NewWritableFile(fname, &file);
		NewWritableFile(fname, &vlog);

		if (!s.ok()) {
			return s;
		}

		meta->smallest = iter->key().data();
		Slice key;
		Slice val;
		for (; iter->hasNext(); iter->next()) {
			key = iter->key();
			val = iter->value();
			vLogBuilder->Append(key, val);
			key_buf.append(key.data(), key.size());
			size_t offset = key.size();
			key_buf.append(12, '\0');
			EncodeFixed64(&key_buf[offset], head_offset);
			EncodeFixed32(&key_buf[offset + 8], val.size());
			head_offset += val.size() + 15;
		}
		if (!key.empty()) {
			meta->largest = key.data();
		}

		char* sst_meta = new char[8224];
		EncodeFixed64(sst_meta, v->timestamp++);
		EncodeFixed64(sst_meta + 8, key_buf.size() / 20);
		strncpy(sst_meta + 16, meta->smallest, 8);
		strncpy(sst_meta + 24, meta->largest, 8);
		bloomfilter::CreateFilter(&key_buf[0], key_buf.size() / 20, 20, sst_meta + 32);

		// need threa
		vlog->Append(vLogBuilder->plain_char());
		vlog->Close();

		file->Append(Slice(sst_meta, 8224));
		file->Append(Slice(key_buf));
		file->Close();
		delete vLogBuilder;
		delete[] sst_meta;

		return Status::OK();
	}
	return Status::IOError("Iter is empty");
}
