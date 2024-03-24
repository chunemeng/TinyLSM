#include "builder.h"
#include "../utils/slice.h"
#include "../utils/iterator.h"
#include "../utils/filemeta.h"
#include "../utils/filename.h"

int BuildTable(const std::string& dbname, Iterator* iter,FileMeta *meta) {
//	meta->file_size = 0;
	iter->seekToFirst();

	std::string fname = SSTFileName(dbname, meta->number);
	if (iter->hasNext()) {
//		WritableFile* file;
//		s = env->NewWritableFile(fname, &file);
//		if (!s.ok()) {
//			return s;
//		}

//		TableBuilder* builder = new TableBuilder(options, file);
		meta->smallest.DecodeFrom(iter->key());
		Slice key;
		for (; iter->hasNext(); iter->next()) {

			key = iter->key();
//			builder->Add(key, iter->value());
		}
		if (!key.empty()) {
//			meta->largest.DecodeFrom(key);
		}

		// Finish and check for builder errors
//		s = builder->Finish();
//		if (s.ok()) {
//			meta->file_size = builder->FileSize();
//			assert(meta->file_size > 0);
//		}
//		delete builder;
//
//		// Finish and check for file errors
//		if (s.ok()) {
//			s = file->Sync();
//		}
//		if (s.ok()) {
//			s = file->Close();
//		}
//		delete file;
//		file = nullptr;
//
//		if (s.ok()) {
//			// Verify that the table is usable
//			Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
//				meta->file_size);
//			s = it->status();
//			delete it;
//		}
//	}
//
//	// Check for input iterator errors
//	if (!iter->status().ok()) {
//		s = iter->status();
//	}


}
