//
// Created by chunemeng on 24-5-21.
//

#include "thread.h"
#include "test.h"

class ThroughputTest : public Test {
public:
    ThroughputTest(const std::string &dir, const std::string &vlog, bool v = true) : Test(dir, vlog, v),
                                                                                     put([this] { simulate_put(); }),
                                                                                     counter([this] { simulate_counter(); }) {
        std::random_device rd;
        gen = std::mt19937(rd());
        running = true;
        vec = std::string(value_size, 's');
        for (int i = 0; i < 500000; ++i) {
            store.put(i, vec);
        }
    }


    void start_test(void *args = NULL) override {
        std::cout << "KVStore ThroughPut Test" << std::endl;

        store.reset();

        regular_test();
    }

private:
    const int value_size = 10000;
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1000000;
    const uint64_t GC_TEST_MAX = 1024 * 48;
    LSMKV::Thread put, counter;
    std::atomic<uint64_t> puts{};
    static constexpr int fixed = 2;
    static constexpr int nums = 60;
    std::atomic<bool> running;
    std::mt19937 gen;
    std::string vec;

    void simulate_put() {
        while (running) {
            store.put(gen(), vec);
            puts++;
        }
    }

    void simulate_counter() {
        puts = 0;
        std::string s;
        auto start = std::chrono::steady_clock::now();
        long sum = 0;
        for (int i = 0; i < nums; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            auto count = puts.load(std::memory_order_acquire);
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = end - start;
            s.append("Throughput: ");
            std::string str = std::to_string((double) count / elapsed.count());
            s.append(str.substr(0, str.find('.') + fixed + 1));
            s.append(" PUT requests per second\n");
            sum += count;
            puts.store(0, std::memory_order_relaxed);
            start = std::chrono::steady_clock::now();
        }
        running = false;
        std::cout << s<<std::endl;
        std::cout<<sum<<std::endl;
    }

    void regular_test() {
        put.start();
        counter.start();
    }
};


int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    ThroughputTest test("/hone/data", "/hone/data/vlog", verbose);

    test.start_test();

    return 0;
}