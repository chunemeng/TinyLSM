//
// Created by chunemeng on 24-4-3.
//

#ifndef LSMKV_HANDOUT_PERFORMANCE_H
#define LSMKV_HANDOUT_PERFORMANCE_H
#include <map>
#include <string>
#include <chrono>
#include "../utils/file.h"

class Performance {
public:
    Performance(const std::string& dbname):dbname(dbname) {
        startd = std::chrono::system_clock::now();
    }
    void AddPerformanceTest(const std::string & name) {
        times.emplace(name,0);
    }
    void StartTest(const std::string &name) {
        if (!times.contains(name)) {
            times.emplace(name,0);
            num.emplace(name,1);
        } else {
            num[name] = num[name] + 1;
        }
        start[name] = std::chrono::system_clock::now();
    }
    void EndTest(const std::string& name) {
        auto end = std::chrono::system_clock::now();
        auto it = times.find(name);

        auto duration = duration_cast<std::chrono::duration<double,std::micro>>(end - start[name]);
        it->second = it->second + duration.count();
    }
    ~Performance(){
        LSMKV::WritableFile *file;
        LSMKV::NewWritableFile(dbname + "/"+ ".log",&file);
        for (auto& it:times) {
            std::string tmp = it.first+": \n" ;
            tmp += "total: " + std::to_string(it.second) + " ms\n";
            tmp += "op   : " + std::to_string(num[it.first]) + "  \n";
            tmp += "per  : " + std::to_string(it.second/num[it.first]) + " ms\n";
            file->Append(tmp);
        }
        delete file;
    }
private:
    std::string dbname;
    decltype(std::chrono::system_clock::now()) startd;
    std::map<std::string, int> num;
    std::map<std::string, decltype(std::chrono::system_clock::now())> start;
    std::map<std::string, double> times;
};


#endif //LSMKV_HANDOUT_PERFORMANCE_H
