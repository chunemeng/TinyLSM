#ifndef OPTION_H
#define OPTION_H

#include "./bloomfilter.h"

namespace LSMKV {
    struct Option {
        Option() = default;

        ~Option() = default;

        static constexpr bool isIndex = true;
        static constexpr bool isFilter = true;
        static constexpr int bloom_size_ = bloom_size;
        static constexpr int key_size_ = 16 * 1024 - bloom_size_ - 32;
        static_assert((key_size_) % 20 == 0);
        static constexpr int pair_size_ = (key_size_) / 20;

        [[nodiscard]] static bool getIsFilter() {
            return isFilter;
        }

        [[nodiscard]] static bool getIsIndex() {
            return isIndex;
        }

        [[nodiscard]] static int getBloomSize() {
            return bloom_size;
        }

        Option &operator=(const Option &option) = delete;

        Option(const Option &option) = delete;

        static Option &getInstance() {
            static Option option;
            return option;
        }

    };
}

#endif //OPTION_H
