#ifndef LSMKV_SCHEDULER_HPP
#define LSMKV_SCHEDULER_HPP

#include <future>
#include <optional>
#include "queue.hpp"
#include "builder.h"

namespace LSMKV {
    class Scheduler {
    public:
        using Task = LSMKV::Builder;
        struct Request {
            Task task;
            std::promise<void> callback_;
        };

        Scheduler() {
            background_thread_.emplace([&] { StartSchedule(); });
        }

        ~Scheduler() {
            request_queue_.push(std::nullopt);
            if (background_thread_.has_value()) {
                background_thread_->join();
                background_thread_.reset();
            }
        }

        void Schedule(Request r) {
            request_queue_.push(std::move(r));
        }

        void StartSchedule() {
            std::optional<Request> req;

            while ((req = request_queue_.pop()) != std::nullopt) {
                req->task();
                req->callback_.set_value();
            }
        };

    private:
        Queue<std::optional<Request>> request_queue_;
        /** The background thread responsible for issuing scheduled requests to the disk manager. */
        std::optional<std::thread> background_thread_;
    };
}

#endif //LSMKV_SCHEDULER_HPP
