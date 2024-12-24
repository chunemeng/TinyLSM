//
// Created by chunemeng on 24-4-3.
//

#ifndef LSMKV_HANDOUT_PERFORMANCE_H
#define LSMKV_HANDOUT_PERFORMANCE_H

#include "utils/file.h"
#include <chrono>
#include <map>
#include <string>
#include <utility>

class Performance {
public:

    explicit Performance(std::string dbname) : dbname(std::move(dbname)) {
        startd = std::chrono::system_clock::now();
    }

    void AddPerformanceTest(const std::string &name) {
        times.emplace(name, 0);
    }

    void StartTest(const std::string &name) {
        if (!times.contains(name)) {
            times.emplace(name, 0);
            num.emplace(name, 1);
        } else {
            num[name] = num[name] + 1;
        }
        start[name] = std::chrono::high_resolution_clock::now();
    }

    void EndTest(const std::string &name) {
        auto end = std::chrono::high_resolution_clock::now();
        auto it = times.find(name);

        auto duration = duration_cast<std::chrono::duration<double, std::micro>>(end - start[name]);
        it->second = it->second + duration.count();
    }

    ~Performance() {
        LSMKV::WritableFile *file;
        double total = 0;
        LSMKV::NewWritableFile(dbname + "/" + ".log", &file);
        for (auto &it: times) {
            std::string tmp = it.first + ": \n";
            total += it.second;
            tmp += "    total: " + std::to_string(it.second) + " μs\n";
            tmp += "    op   : " + std::to_string(num[it.first]) + "  \n";
            tmp += "    per  : " + std::to_string(it.second / num[it.first]) + " μs\n";
            file->Append(tmp);
        }
        delete file;
    }


private:
    std::string dbname;
    decltype(std::chrono::high_resolution_clock::now()) startd;
    std::map<std::string, int> num;
    std::map<std::string, decltype(std::chrono::high_resolution_clock::now())> start;
    std::map<std::string, double> times;
};

struct PerformanceGuard {
    PerformanceGuard(Performance *p, std::string name) : p_(p), n(std::move(name)) {
        p_->StartTest(n);
    }

    ~PerformanceGuard() {
        if (p_ != nullptr) {
            p_->EndTest(n);
        }
    }

    void drop() {
        p_->EndTest(n);
        p_ = nullptr;
    }

    Performance *p_;
    const std::string n;
};


#endif //LSMKV_HANDOUT_PERFORMANCE_H
