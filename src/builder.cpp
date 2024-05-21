#include "include/builder.h"
#include "include/version.h"
#include "include/vlogbuilder.h"

namespace LSMKV {
#define CHUNK_SIZE (1024 * 1024 * 64)

    bool BuildTable(const std::string &dbname, Version *v, Iterator *iter, size_t size, KeyCache *kc) {
        FileMeta meta{};
        meta.size = size;
        iter->seekToFirst();
        std::string vlog_buf;
        char *key_buf;
        uint64_t value_size{}, key_offset{};
        uint64_t level = FindLevels(dbname, v);

        bool compaction = false;
        std::string fname = SSTFileName(LevelDirName(dbname, level), v->fileno);
        uint64_t head_offset = v->head;
        const char *tmp = "~DELETED~";

        Slice tombstone = Slice(tmp, 9);
        if (iter->hasNext()) {
            WritableNoBufFile *file;
            WritableFile *vlog;

            bool s = NewWritableNoBufFile(fname, &file);
            NewAppendableFile(VLogFileName(dbname), &vlog);
            VLogBuilder vLogBuilder{vlog};

            if (!s) {
                return s;
            }

            v->AddNewLevelStatus(level, v->fileno, 1);
            if (v->LevelOver(level)) {
                compaction = true;
            }
            key_buf = kc->ReserveCache(meta.size * 20 + 8224, v->fileno);

            // NEED TO CLEAR FOR BLOOM_FILTER
            memset(key_buf + 32, 0, 8192);

            key_offset = 8224;

            meta.smallest = iter->key();
            uint64_t key;
            Slice val;
            for (; iter->hasNext(); iter->next()) {
                key = iter->key();
                val = iter->value();
                if (val == tombstone) {
                    value_size = 0;
                } else {
                    vLogBuilder.Append(key, val);
                    value_size = val.size();
                }

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

            CreateFilter(key_buf + 8224, meta.size, 20, key_buf + 32);
            file->WriteUnbuffered(key_buf, 8224 + meta.size * 20);

            kc->PushCache(key_buf, Option::getInstance());

            // need multi thread
            vLogBuilder.Drop();

            v->head = head_offset;
            delete file;
            v->fileno++;

//            kc->LogLevel(0, 5);
            if (compaction) {
                SSTCompaction(level, v->fileno, v, kc);
            }

            Version::WriteToFile(v);
            return true;
        }
        return false;
    }

    bool SSTCompaction(uint64_t level, uint64_t file_no, Version *v, KeyCache *kc) {
        std::vector<uint64_t> need_to_move;
        //Need to be rm and earse in version
        std::vector<uint64_t> old_files[2] = {std::vector<uint64_t>(), std::vector<uint64_t>()};
        std::vector<Slice> need_to_write;
        if (v->NeedNewLevel(level)) {
            v->AddNewLevel(1);
        }
        auto size = v->LevelSize(level)
                    - ((level == 0) ? 0 : (1 << (level + 1)));
        uint64_t timestamp
                = kc->CompactionSST(level, file_no, size,
                                    old_files,
                                    need_to_move,
                                    need_to_write);
        // move
        MoveToNewLevel(level, timestamp, need_to_move, v);
        v->MoveLevelStatus(level, need_to_move);
        // write
        WriteSlice(need_to_write, level, v);
        // update level status
        v->AddNewLevelStatus(level + 1, v->fileno - need_to_write.size(), need_to_write.size());
        // remove old sst
        v->ClearLevelStatus(level, old_files);

        // PASS THE COMPACTION
        if (v->LevelOver(level + 1)) {
            SSTCompaction(level + 1, v->fileno, v, kc);
        }
        return true;
    }

    bool WriteSlice(std::vector<Slice> &need_to_write, uint64_t level, Version *v) {
        WritableNoBufFile *file;
        auto dbname = v->DBName();
        const char *tmp;
        for (auto &s: need_to_write) {
            NewWritableNoBufFile(SSTFilePath(dbname, level + 1, v->fileno++), &file);
//            CreateFilter(s.data() + 8224, DecodeFixed64(s.data() + 8), 20, const_cast<char *>(s.data()) + 32);
            size_t num_chunks = (s.size() + CHUNK_SIZE - 1) / CHUNK_SIZE;
            size_t pod = s.size() - (num_chunks - 1) * CHUNK_SIZE;
            tmp = s.data();
            for (auto index = 1; index < num_chunks; ++index) {
                file->WriteUnbuffered(tmp, CHUNK_SIZE);
                tmp += CHUNK_SIZE;
            }
            assert(pod != 0);
            if (pod != 0) {
                file->WriteUnbuffered(tmp, pod);
            }

//            bool f = file->WriteUnbuffered(s.data(), s.size());
            delete file;
        }
        return true;
    }

    bool MoveToNewLevel(uint64_t level, const uint64_t &timestamp, std::vector<uint64_t> &new_files, Version *v) {
        std::string dbname = v->DBName();
        if (v->NeedNewLevel(level)) {
            v->AddNewLevel(1);
        }

        for (auto &it: new_files) {
            utils::mvfile(SSTFilePath(dbname, level, it), SSTFilePath(dbname, level + 1, it));
        }

        // todo move may faster than rewrite(?)
        WritableNoBufFile *file;
        char buf[8];
        // Change the timestamp
        for (auto &it: new_files) {
            NewWriteAtStartFile(SSTFilePath(dbname, level + 1, it), &file);
            EncodeFixed64(buf, timestamp);
            file->WriteUnbuffered(Slice(buf, 8));
            delete file;
        }
        return true;
    }

    uint64_t FindLevels(const std::string &dbname, Version *v) {
        return 0;
    }

}

