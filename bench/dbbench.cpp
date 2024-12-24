//
// Created by chunemeng on 24-5-23.
//
#include "../kvstore.h"
#include "../test/testutil.h"
#include "benchmark/benchmark.h"

class perf {
public:
    perf() : store("/home/data", "/home/data/vlog") {
    }
    void reset() {
        store.reset();
    }

    void get() {
        uint64_t key = gen();
        store.get(key);
    }

    void put(int length, int rate = 1) {
        std::string  s(value_size,'a');
        for (int i = 0; i < length; i++) {
            store.put(curr * rate, s);
        }
        curr += length;
    }

private:
    static constexpr int value_size = 8;
    uint64_t curr = 0;
    std::mt19937 gen{std::random_device{}()};

    class KVStore store;
};

static void bench_db_put(benchmark::State &state) {
    perf test;
    test.reset();
    constexpr int length = 10;
    for (auto _: state) {
        test.put(length);
    }
}

BENCHMARK(bench_db_put);

static void bench_db_get(benchmark::State &state) {
    perf test;
    test.reset();
    constexpr int length = 100000;
    test.put(length, 1000);
    for (auto _: state) {
        test.get();
    }
}

//BENCHMARK(bench_db_get);

BENCHMARK_MAIN();