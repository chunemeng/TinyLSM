#ifndef VLOGBUILDER_H
#define VLOGBUILDER_H

#include <string>
#include "../../utils/slice.h"
#include "../../utils.h"
#include "../../utils/coding.h"

namespace LSMKV {
    class VLogBuilder {

    public:
        explicit VLogBuilder(WritableFile *file) : file_(file) {};

        void Append(uint64_t key, const Slice &value) {
            auto size = 15 + value.size();
            if (size <= 32768) [[likely]] {
                auto buf = file_->WriteToBuffer(size);
                buf[0] = magic;
                memcpy(buf + 15, value.data(), value.size());

                EncodeFixed64(buf + 3, key);
                EncodeFixed32(buf + 11, value.size());
                EncodeFixed16(buf + 1, utils::crc16(buf + 3, size - 3));
                return;
            }
            auto buf = file_->WriteToBuffer(15);
            buf[0] = magic;
            EncodeFixed64(buf + 3, key);
            EncodeFixed32(buf + 11, value.size());
            EncodeFixed16(buf + 1, utils::crc16_prefix(buf + 3, 12, value.data(), size - 15));
            file_->Flush();
            (file_->WriteUnbuffered(value.data(), size - 15));
        }

        void Drop() {
            file_->Close();
            delete file_;
        }


    private:
        // magic 0xff
        static constexpr const char magic = '\377';
        WritableFile *file_;
    };
}
#endif //VLOGBUILDER_H
