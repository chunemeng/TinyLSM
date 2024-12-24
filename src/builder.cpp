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
            int bloom_length = bloom_size + 32;
            key_buf = kc->ReserveCache(meta.size * 20 + bloom_length, v->fileno);

            // NEED TO CLEAR FOR BLOOM_FILTER
            memset(key_buf + 32, 0, bloom_length - 32);

            key_offset = bloom_length;

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

            CreateFilter(key_buf + bloom_length, meta.size, 20, key_buf + 32);
            KeyMayMatch(0, key_buf + 32);
            file->WriteUnbuffered(key_buf, bloom_length + meta.size * 20);

            kc->PushCache(key_buf);

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
        // todo may no need lock in here

        //Need to be rm and earse in version
        std::vector<uint64_t> old_files[2] = {std::vector<uint64_t>(), std::vector<uint64_t>()};
        std::vector<class WriteSlice> need_to_write;
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

    bool WriteSlice(std::vector<class WriteSlice> &need_to_write, uint64_t level, Version *v) {
        auto dbname = v->DBName();
        auto &scheduler = v->write_scheduler_;
        std::vector<std::future<void>> tasks;
        tasks.reserve(need_to_write.size());
        std::promise<void> promise;
        for (auto &s: need_to_write) {
            promise = {};
            tasks.emplace_back(promise.get_future());
            scheduler.Schedule({true,SSTFilePath(dbname, level + 1, v->fileno++), s, std::move(promise)});
        }
        for (auto &fu: tasks) {
            fu.get();
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

        auto level_dir = LevelDirName(dbname, level);

        if (!utils::dirExists(level_dir)) [[unlikely]] {
            utils::mkdir(level_dir);
        }


        // Change the timestamp
        for (auto &it: new_files) {
            NewWriteAtStartFile(SSTFilePath(level_dir, it), &file);
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

