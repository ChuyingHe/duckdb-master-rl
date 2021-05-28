cmake_minimum_required(VERSION 2.8.12)
project(chuying)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

add_executable(chuying main.cpp imdb_constants.hpp)
target_link_libraries(chuying duckdb)
