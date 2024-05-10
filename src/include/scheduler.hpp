#ifndef LSMKV_SCHEDULER_HPP
#define LSMKV_SCHEDULER_HPP

#include <future>
#include <optional>

namespace LSMKV {
    class Scheduler {
    public:
        using Task = std::function<void()>;
        struct Request {
            Task task;
            std::promise<bool> callback_;
        };

        Scheduler();

        ~Scheduler();

        void Schedule(Request r);

        void StartSchedule() {
            std::optional<Request> req;

            while ((req = request_queue_.pop()) != std::nullopt) {
                req->task();
                req->callback_.set_value(true);
            }
        };

    private:
        Queue<std::optional<Request>> request_queue_;
        /** The background thread responsible for issuing scheduled requests to the disk manager. */
        std::optional<std::thread> background_thread_;
    };

    void Scheduler::Schedule(Scheduler::Request r) {
        request_queue_.push(std::move(r));
    }

    Scheduler::Scheduler(){
        background_thread_.emplace([&] { StartSchedule(); });
    }

    Scheduler::~Scheduler() {
        request_queue_.push(std::nullopt);
        if (background_thread_.has_value()) {
            background_thread_->join();
        }
    }
}

#endif //LSMKV_SCHEDULER_HPP
