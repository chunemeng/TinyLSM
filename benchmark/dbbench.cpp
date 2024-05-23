//
// Created by chunemeng on 24-5-23.
//
#include "benchmark/benchmark.h"
#include "../kvstore.h"
#include "../test/testutil.h"

class perf {
public:
    perf() : store("./data", "./data/vlog") {
        std::random_device rd;
        gen = std::mt19937(rd());
    }
    void reset() {
        store.reset();
    }

    void get() {
        uint64_t key = gen();
        store.get(key);
    }

    void put(int length, int rate = 1) {
        for (int i = 0; i < length; i++) {
            store.put(curr * rate, ss);
        }
        curr += length;
    }

private:
    static constexpr int value_size = 8;
    uint64_t curr = 0;
    std::string ss{value_size, 's'};
    std::mt19937 gen;

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