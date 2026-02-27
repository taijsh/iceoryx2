# Copyright (c) 2024 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache Software License 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
# which is available at https://opensource.org/licenses/MIT.
#
# SPDX-License-Identifier: Apache-2.0 OR MIT

cmake_minimum_required(VERSION 3.22)
project(example_c_publish_subscribe LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

find_package(iceoryx2-c 0.8.999 REQUIRED)

add_executable(example_c_publisher src/publisher.c)
target_link_libraries(example_c_publisher iceoryx2-c::static-lib)

add_executable(example_c_subscriber src/subscriber.c)
target_link_libraries(example_c_subscriber iceoryx2-c::static-lib)

add_executable(example_c_shared_memory_app src/shm_communicator.c src/shared_memory_communicator_app.c)
target_link_libraries(example_c_shared_memory_app iceoryx2-c::static-lib)

# 编译 shm_communicator 为动态链接库 (.so)
add_library(shm_communicator SHARED src/shm_communicator.c)
target_link_libraries(shm_communicator iceoryx2-c::static-lib)

# 新增 C++11 执行应用，并链接上面的动态库
add_executable(example_cxx_shared_memory_app src/shared_memory_communicator_app_cxx.cpp)
target_link_libraries(example_cxx_shared_memory_app shm_communicator)

# 新增压力测试程序
add_executable(shm_stress_test src/shm_stress_test.cpp)
target_link_libraries(shm_stress_test shm_communicator)

# 新增吞吐量测试程序
add_executable(shm_throughput_test src/shm_throughput_test.cpp)
target_link_libraries(shm_throughput_test shm_communicator)

# ======= C++ 类封装及其测试程序 =======

# 编译 C++ 封装类为动态库 (.so/.dll)
add_library(shm_communicator_cxx SHARED src/SharedMemoryCommunicator.cpp)
# 链接 shm_communicator 动态库
target_link_libraries(shm_communicator_cxx shm_communicator)

# 编译 C++ 封装类的专用测试用例
add_executable(SharedMemoryCommunicatorTest src/SharedMemoryCommunicatorTest.cpp)
# 链接 SharedMemoryCommunicator 动态库
target_link_libraries(SharedMemoryCommunicatorTest shm_communicator_cxx)
