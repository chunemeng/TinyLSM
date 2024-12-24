add_executable(bench dbbench.cpp)
target_link_libraries(bench LSMKV)
target_link_libraries(bench benchmark::benchmark)
