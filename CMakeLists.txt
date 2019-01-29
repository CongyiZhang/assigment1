cmake_minimum_required(VERSION 3.13)
project(assignment1)

set(CMAKE_CXX_STANDARD 14)

add_executable(assignment1 main.cpp dberror.cpp dberror.h storage_mgr.h test_helper.h storage_mgr.cpp)