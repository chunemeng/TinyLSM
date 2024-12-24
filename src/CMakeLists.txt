

file(GLOB_RECURSE SRC_FILES "*.cpp")

target_sources(LSMKV PRIVATE ${SRC_FILES})

target_include_directories(LSMKV PRIVATE "${CMAKE_SOURCE_DIR}/utils" "${CMAKE_SOURCE_DIR}/src/include")



