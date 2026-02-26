// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <chrono>
#include <csignal>
#include <atomic>
#include <iomanip>

#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
#include <windows.h>
#define SLEEP_MS(ms) Sleep(ms)
#else
#include <unistd.h>
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

// 引入原有的 C 语言通信器头文件
extern "C" {
#include "shm_communicator.h"
#include "transmission_data.h"
}

// 全局运行状态标志
std::atomic<bool> keep_running{true};

// 信号处理
void signal_handler(int sig) {
    std::cout << "接收到信号: " << sig << std::endl;
    keep_running = false;
}

// 提取当前时间的纳秒 (Wall Clock, 用于跨进程延迟统计)
uint64_t app_get_time_ns() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

// 提取当前时间的纳秒 (Steady Clock, 用于内部耗时统计)
uint64_t app_get_steady_ns() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

// 订阅者的回调函数
void app_on_receive(const char* topic, const void* data, size_t size, void* user_context) {
    if (size >= sizeof(uint64_t)) {
        uint64_t current_time_ns = app_get_time_ns();
        uint64_t send_time_ns;
        std::memcpy(&send_time_ns, data, sizeof(uint64_t));
        
        uint64_t latency_ns = current_time_ns - send_time_ns;
        double latency_us = latency_ns / 1000.0;
        
        std::cout << "[" << topic << "] 接收到 " << size << " 字节, 延迟: " 
                  << std::fixed << std::setprecision(2) << latency_us << " us" << std::endl;
    } else {
        std::cout << "[" << topic << "] 接收到 " << size << " 字节" << std::endl;
    }
}

// 配置结构
struct AppConfig {
    std::vector<std::string> pub_topics;
    std::vector<std::string> sub_topics;
    size_t payload_size = 1024;
    size_t interval_ms = 1000;
    int cpu_core_id = -1;
    bool enable_waitset = true;
    bool use_copy_mode = false;
    shm_data_type_e data_type = SHM_DATA_DYNAMIC;
    std::string config_path = "/home/taijsh/ljtx/ljtx_component.toml";
};

void print_help() {
    std::cout << "用法: shared_memory_communicator_app_cxx [options]\n"
              << "选项:\n"
              << "  --pub <topic>       发布到主题 (可多次使用)\n"
              << "  --sub <topic>       订阅到主题 (可多次使用)\n"
              << "  --size <bytes>      负载大小字节数 (默认: 1024)\n"
              << "  --interval <ms>     发布间隔毫秒数 (默认: 1000)\n"
              << "  --waitset <0|1>     是否启用系统级 WaitSet 事件驱动机制 (默认: 1)\n"
              << "  --mode <zero|copy>  发送模式: zero 为零拷贝, copy 为拷贝模式 (默认: zero)\n"
              << "  --cpu <id>          绑定接收线程到指定 CPU 核心 (默认: -1 不绑定)\n"
              << "  --type <dyn|common|large|stream> 数据类型 (默认: dyn)\n"
              << "  --config <path>     配置文件路径 (默认: /home/taijsh/ljtx/ljtx_component.toml)\n"
              << "  --help              显示本帮助信息\n";
}

AppConfig parse_args(int argc, char** argv) {
    AppConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--pub") {
            if (i + 1 < argc) {
                config.pub_topics.push_back(argv[++i]);
            } else {
                std::cerr << "错误: --pub 需要一个有效主题参数\n";
                std::exit(1);
            }
        } else if (arg == "--sub") {
            if (i + 1 < argc) {
                config.sub_topics.push_back(argv[++i]);
            } else {
                std::cerr << "错误: --sub 需要一个有效主题参数\n";
                std::exit(1);
            }
        } else if (arg == "--size") {
            if (i + 1 < argc) {
                config.payload_size = std::stoull(argv[++i]);
            } else {
                std::cerr << "错误: --size 需要指定字节数\n";
                std::exit(1);
            }
        } else if (arg == "--interval") {
            if (i + 1 < argc) {
                config.interval_ms = std::stoull(argv[++i]);
            } else {
                std::cerr << "错误: --interval 需要指定毫秒数\n";
                std::exit(1);
            }
        } else if (arg == "--waitset") {
            if (i + 1 < argc) {
                config.enable_waitset = (std::stoi(argv[++i]) != 0);
            } else {
                std::cerr << "错误: --waitset 需要 0 或 1\n";
                std::exit(1);
            }
        } else if (arg == "--cpu") {
            if (i + 1 < argc) {
                config.cpu_core_id = std::stoi(argv[++i]);
            } else {
                std::cerr << "错误: --cpu 需要指定核心ID\n";
                std::exit(1);
            }
        } else if (arg == "--mode") {
            if (i + 1 < argc) {
                std::string mode = argv[++i];
                if (mode == "copy") {
                    config.use_copy_mode = true;
                } else {
                    config.use_copy_mode = false;
                }
            } else {
                std::cerr << "错误: --mode 需要指定 zero 或 copy\n";
                std::exit(1);
            }
        } else if (arg == "--type") {
            if (i + 1 < argc) {
                std::string type_str = argv[++i];
                if (type_str == "common") config.data_type = SHM_DATA_COMMON;
                else if (type_str == "large") config.data_type = SHM_DATA_LARGE;
                else if (type_str == "stream") config.data_type = SHM_DATA_STREAM;
                else config.data_type = SHM_DATA_DYNAMIC;
            } else {
                std::cerr << "错误: --type 需要指定 dyn, common, large 或 stream\n";
                std::exit(1);
            }
        } else if (arg == "--config") {
            if (i + 1 < argc) {
                config.config_path = argv[++i];
            } else {
                std::cerr << "错误: --config 需要配置文件路径\n";
                std::exit(1);
            }
        } else if (arg == "--help") {
            print_help();
            std::exit(0);
        } else {
            std::cerr << "未知参数: " << arg << "\n";
            print_help();
            std::exit(1);
        }
    }
    return config;
}

int main(int argc, char** argv) {
    AppConfig config = parse_args(argc, argv);

    // 1. 创建通信器
    shm_communicator_t* comm = shm_communicator_create();
    if (!shm_communicator_init(comm, config.config_path.c_str(), config.enable_waitset, app_on_receive, nullptr)) {
        std::cerr << "无法初始化 SharedMemoryCommunicator\n";
        return 1;
    }
    std::cout << "初始化成功" << std::endl;

    // 根据类型调整 Payload Size
    if (config.data_type == SHM_DATA_COMMON) config.payload_size = sizeof(struct TransmissionCommonData);
    else if (config.data_type == SHM_DATA_LARGE) config.payload_size = sizeof(struct TransmissionLargeData);
    else if (config.data_type == SHM_DATA_STREAM) config.payload_size = sizeof(struct TransmissionStreamData);

    // 设置信号
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // 2. 注册发布者
    for (const auto& topic : config.pub_topics) {
        if (!shm_communicator_register_publisher_ext(comm, topic.c_str(), config.data_type)) {
            std::cerr << "无法注册发布者主题: " << topic << std::endl;
            return 1;
        }
    }
    if (!config.pub_topics.empty()) {
        std::cout << "发布者注册成功" << std::endl;
    }

    // 3. 注册订阅者
    for (const auto& topic : config.sub_topics) {
        std::cout << "订阅者注册:" << topic << std::endl;
        if (!shm_communicator_register_subscriber_ext(comm, topic.c_str(), config.data_type)) {
            std::cerr << "无法注册订阅者主题: " << topic << std::endl;
            return 1;
        }
        std::cout << "订阅者注册:" << topic << "成功" << std::endl;
    }
    if (!config.sub_topics.empty()) {
        std::cout << "订阅者注册成功" << std::endl;
    }

    std::cout << "应用程序已启动。\n"
              << "配置校验:\n"
              << "  负载大小: " << config.payload_size << " 字节\n"
              << "  间隔: " << config.interval_ms << " 毫秒\n"
              << "  启用 WaitSet 事件机制: " << (config.enable_waitset ? "是 (微秒级延迟,低CPU)" : "否 (纯忙轮询,纳秒级延迟,高CPU)") << "\n"
              << "  发送模式: " << (config.use_copy_mode ? "拷贝模式 (shm_communicator_publish_copy)" : "零拷贝模式 (shm_communicator_loan/send)") << "\n"
              << "  数据类型: " << config.data_type << " (0:dyn, 1:common, 2:large, 3:stream)\n"
              << "  绑定 CPU 核心: " << config.cpu_core_id << "\n"
              << "  配置路径: " << config.config_path << std::endl;

    // 4. 启动后台处理线程
    if (!shm_communicator_start(comm, config.cpu_core_id)) {
        std::cerr << "警告: 无法启动后台处理线程或平台不支持。" << std::endl;
    }

    // 创建虚拟数据负载 (仅作为填充)
    std::vector<uint8_t> dummy_data(config.payload_size);
    for (size_t i = 0; i < config.payload_size; ++i) {
        dummy_data[i] = static_cast<uint8_t>(i % 255);
    }

    while (keep_running) {
        // 4. 对所有注册的发布者发送带有当前时间戳的 Payload (使用真零拷贝接口)
        for (const auto& topic : config.pub_topics) {
            uint64_t t1 = app_get_steady_ns();
            uint64_t t2, t3, t4;
            bool success = false;

            if (config.use_copy_mode) {
                // 使用拷贝模式发送
                uint64_t current_time_ns = app_get_time_ns();
                if (config.payload_size >= sizeof(uint64_t)) {
                    std::memcpy(dummy_data.data(), &current_time_ns, sizeof(uint64_t));
                }
                
                t2 = app_get_steady_ns();
                t3 = app_get_steady_ns();
                
                success = shm_communicator_publish_copy(comm, topic.c_str(), dummy_data.data(), config.payload_size);
                
                t4 = app_get_steady_ns();
            } else {
                // 使用零拷贝模式发送
                void* sample_h = nullptr;
                void* payload = shm_communicator_loan_uninit(comm, topic.c_str(), config.payload_size, &sample_h);
                
                t2 = app_get_steady_ns();
                
                if (payload && sample_h) {
                    if (config.payload_size >= sizeof(uint64_t)) {
                        uint64_t current_time_ns = app_get_time_ns();
                        std::memcpy(payload, &current_time_ns, sizeof(uint64_t));
                    }
                    
                    t3 = app_get_steady_ns();
                    
                    success = shm_communicator_send(comm, topic.c_str(), sample_h);
                    
                    t4 = app_get_steady_ns();
                    
                    shm_communicator_ensure_pre_loan(comm, topic.c_str());
                }
            }

            if (success) {
                // 打印各项操作耗时
                if (config.use_copy_mode) {
                    std::cout << "[" << topic << "] 发送统计 (Copy Mode): total: " 
                              << std::fixed << std::setprecision(2) << (t4 - t1) / 1000.0 << " us" << std::endl;
                } else {
                    std::cout << "[" << topic << "] 发送统计 (Zero Copy): loan: " 
                              << std::fixed << std::setprecision(2) << (t2 - t1) / 1000.0 << " us, copy: "
                              << (t3 - t2) / 1000.0 << " us, send: "
                              << (t4 - t3) / 1000.0 << " us" << std::endl;
                }
            }
        }

        // 5. 数据接收由后台线程自动通过 app_on_receive 回调处理。
        // 主线程只需按照业务要求的频率休眠。
        SLEEP_MS(config.interval_ms);
    }

    std::cout << "应用程序正在停止..." << std::endl;

    shm_communicator_destroy(comm);

    return 0;
}
