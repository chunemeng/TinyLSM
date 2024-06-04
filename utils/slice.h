#ifndef SLICE_H
#define SLICE_H

#include <cstddef>
#include <string>
#include <cstring>
#include <cassert>

namespace LSMKV {
    class WriteSlice {
    public:
        WriteSlice(char *data, size_t size) : _data(data), _size(size) {
        }

        char *data() {
            return _data;
        }

        size_t size() const {
            return _size;
        }

    private:
        char *_data;
        size_t _size;

    };


    class Slice : public std::string_view {
        using std::string_view::string_view;
    public:
        Slice() : std::string_view() {
        }

        Slice(const char *data, size_t size) : std::string_view(data,size) {
        }

        Slice(const std::string &str) : std::string_view(str) {
        }

        Slice(const char *data) : std::string_view(data) {
        }

        Slice(const Slice &) = default;

        Slice &operator=(const Slice &) = default;


        void clear() {
            this->operator=("");
        }


        std::string toString() const {
            return {this->data(), this->size()};
        }

    };

}

#endif //SLICE_H
