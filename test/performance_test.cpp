#include "../test.h"

class SmallTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1000000;
    const int value_size = 5120;
    long aver_value_size = 0;
    std::mt19937 gen;

    void regular_test(uint64_t max) {
        uint64_t i;
        std::vector<std::string> s;
        if (value_size == 0) {
            for (int i = 0; i < max; ++i) {
                auto size = gen() % 20000 + 1;
                aver_value_size += size;
                store.put(i, std::string(size, 's'));
            }
            aver_value_size /= 1000000;
            std::cout << aver_value_size;
        } else {
            for (i = 0; i < max; ++i) {
                store.put(i, std::string(value_size, 's'));
            }
        }

        phase();

        for (i = 0; i < max; ++i) {
            store.get(i);
        }
        phase();

        // Test scan
        std::list<std::pair<uint64_t, std::string>> list_stu;

        store.scan(0, max / 2 - 1, list_stu);
        list_stu.clear();
        for (int i = 0; i < 20; ++i) {
            auto start = gen() % 500000;
            auto len = gen() % 100000 + 1;
            store.scan(start, start + len, list_stu);
            list_stu.clear();
        }

        phase();

        for (i = max; i > 0; --i) {
            EXPECT(1, store.del(i - 1));
        }
        phase();

        report();
    }

    void put_test() {
        for (int j = 1968; j < 5120; j += 16) {
            store.reset();
            auto start = std::chrono::steady_clock::now();
            std::string s(j, 's');
            for (int i = 0; i < LARGE_TEST_MAX; ++i) {
                store.put(i, s);
            }
            auto end = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = end - start;
            std::cout << elapsed.count() << std::endl;
        }
    }

public:
    SmallTest(const std::string &dir, const std::string &vlog, bool v = true)
            : Test(dir, vlog, v) {
        std::random_device rd;
        gen = std::mt19937(rd());
    }

    void start_test(void *args = NULL) override {
        std::cout << "KVStore Performance Test" << std::endl;

        store.reset();

        put_test();

        // std::cout << "[Simple Test]" << std::endl;
        // regular_test(SIMPLE_TEST_MAX);

        // store.reset();

        // std::cout << "[Large Test]" << std::endl;
        // regular_test(LARGE_TEST_MAX);
    }
};

int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    SmallTest test("./data", "./data/vlog", verbose);

    test.start_test();

    return 0;
}
