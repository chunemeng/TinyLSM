
#include "test.h"

class RandomGetTest : public Test {
private:
    const uint64_t SIMPLE_TEST_MAX = 512;
    const uint64_t LARGE_TEST_MAX = 1000000;
    const int value_size = 5120;
    std::mt19937 gen;


    void regular_test(uint64_t max) {
        uint64_t i;
        std::string s(value_size, 's');
        {
            for (i = 0; i < max; ++i) {
                store.put(i * 10, s);
            }
        }
        EXPECT(true, true);
        phase();

        for (i = 0; i < 100000; ++i) {
            auto rand = gen();
            auto gnn = rand % 1000000;
            if (!(rand & 10)) {
                store.get(gnn * 1000);
            } else {
                store.get((gnn % 1000 ? gnn : gnn + 1) * 1000);
            }
        }
        EXPECT(true, true);
        phase();

        report();
    }

public:
    RandomGetTest(const std::string &dir, const std::string &vlog, bool v = true)
            : Test(dir, vlog, v) {
        std::random_device rd;
        gen = std::mt19937(rd());
    }

    void start_test(void *args = NULL) override {
        std::cout << "KVStore Performance Test" << std::endl;

        store.reset();

        std::cout << "[Simple Test]" << std::endl;
        regular_test(SIMPLE_TEST_MAX);

        store.reset();

        std::cout << "[Large Test]" << std::endl;
        regular_test(LARGE_TEST_MAX);
    }
};

int main(int argc, char *argv[]) {
    bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

    std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
    std::cout << "  -v: print extra info for failed tests [currently ";
    std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
    std::cout << std::endl;
    std::cout.flush();

    RandomGetTest test("/hone/data", "/hone/data/vlog", verbose);

    test.start_test();

    return 0;
}