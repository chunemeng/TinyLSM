#ifndef VLOGBUILDER_H
#define VLOGBUILDER_H

#include <string>
#include "../../utils/slice.h"
#include "../../utils.h"
#include "../../utils/coding.h"

namespace LSMKV {
    class VLogBuilder {

    public:
        VLogBuilder(WritableFile *file) : file_(file) {};

        void Append(uint64_t &key, Slice &value) {
            auto size = 15 + value.size();
            if (size <= 65536) [[likely]] {
                auto buf = file_->WriteToBuffer(size);
                buf[0] = magic;
                memcpy(buf + 15, value.data(), value.size());

                EncodeFixed64(buf + 3, key);
                EncodeFixed32(buf + 11, value.size());
                EncodeFixed16(buf + 1, utils::crc16(buf + 3, size - 3));
                return;
            }
            char *buf = new char[size];
            buf[0] = magic;
            memcpy(buf + 15, value.data(), value.size());

            EncodeFixed64(buf + 3, key);
            EncodeFixed32(buf + 11, value.size());
            EncodeFixed16(buf + 1, utils::crc16(buf + 3, size - 3));
            file_->Flush();
            file_->WriteUnbuffered(buf, size);

//            size_t offset = vlog.size();
//            vlog.append(14, '\0');
//            vlog.append(value.data(), value.size());
//            EncodeFixed64(&vlog[offset + 2], key);
//            EncodeFixed32(&vlog[offset + 10], value.size());
//            EncodeFixed16(&vlog[offset], utils::crc16(&vlog[offset + 2], vlog.size() - offset - 2));
        }

        void Drop() {
            file_->Close();
            delete file_;
        }

        Slice plain_char() {
            return {vlog.data(), vlog.size()};
        }

    private:
        // magic 0xff
        static constexpr const char magic = '\377';
        WritableFile *file_;
        std::string vlog;
    };
}
#endif //VLOGBUILDER_H
