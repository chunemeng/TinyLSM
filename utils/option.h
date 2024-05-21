#ifndef OPTION_H
#define OPTION_H
namespace LSMKV {
    class Option {
    private:
        explicit Option(bool isFilter = true) : isFilter(isFilter) {}

        ~Option() = default;

        bool isIndex = true;
        bool isFilter = true;
        static constexpr int bloom_size = 8192;
    public:
        [[nodiscard]] bool getIsFilter() const {
            return isFilter;
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
