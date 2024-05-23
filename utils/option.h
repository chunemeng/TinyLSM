#ifndef OPTION_H
#define OPTION_H

namespace LSMKV {
    struct Option {
        Option() = default;

        ~Option() = default;

        // warning: this only control the index use in get
        // because no index is complex when compaction
        static constexpr bool isIndex = true;
        static constexpr bool isFilter = true;
        static constexpr int pair_size_ = 408;
        static constexpr int key_size_ = pair_size_ * 20;
        static constexpr int bloom_size_ = 16 * 1024 - key_size_ - 32;
        static_assert(bloom_size_ > 0);

        [[nodiscard]] static bool getIsFilter() {
            return isFilter;
        }

        [[nodiscard]] static bool getIsIndex() {
            return isIndex;
        }

        [[nodiscard]] static int getBloomSize() {
            return bloom_size_;
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
