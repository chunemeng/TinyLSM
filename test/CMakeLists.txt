include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(TEST performance_test.cpp)
target_link_libraries(TEST LSMKV)

add_executable(LSMKVC correctness.cc)
target_link_libraries(LSMKVC LSMKV)

add_executable(LSMKVP persistence.cc)
target_link_libraries(LSMKVP LSMKV)

add_executable(Put throughput_test.cpp)
add_executable(Get random_get_test.cpp)
target_link_libraries(Get LSMKV)
target_link_libraries(Put LSMKV)

set(TEST_UTIL_SOURCES testutil.cc testutil.h)

add_executable(Bloom bloom_test.cc ${TEST_UTIL_SOURCES} same_timestamp_test.cpp)
target_link_libraries(Bloom gtest gmock)
target_link_libraries(Bloom gtest_main)