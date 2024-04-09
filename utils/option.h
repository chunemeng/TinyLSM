#ifndef OPTION_H
#define OPTION_H
namespace LSMKV {
    class Option {
    private:
        explicit Option(bool isFilter = true) : isFilter(isFilter) {}
        ~Option() = default;
        Option& operator=(const Option& option) = delete;
        Option(const Option& option) = delete;
        bool isFilter = true;
    public:
        [[nodiscard]] bool getIsFilter() const{
            return isFilter;
        }

        static Option &getInstance() {
            static Option option;
            return option;
        }

    };
}

#endif //OPTION_H
