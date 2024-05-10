#ifndef LSMKV_THREAD_H
#define LSMKV_THREAD_H

#include <thread>

namespace LSMKV {
    class Thread {
    public:
        using Function = void (*)();

        explicit Thread(Function function) {
            this->func_ = function;
        };

        ~Thread() {
            stop();
        }

        void start() {
            this->thread_ = std::thread(func_);
        }

        void stop() {
            if (this->thread_.joinable()) {
                this->thread_.join();
            }
        }

    private:
        Function func_;
        std::thread thread_;
    };
}

#endif //LSMKV_THREAD_H
