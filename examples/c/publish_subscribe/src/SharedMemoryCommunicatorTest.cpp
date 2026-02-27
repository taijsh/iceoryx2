/**
 * @file SharedMemoryCommunicatorTest.cpp
 * @brief 使用 SharedMemoryCommunicator C++ 类实现的测试程序
 * 
 * 该程序演示了如何继承并使用 SharedMemoryCommunicator 类进行发布和订阅，
 * 行为上与 shared_memory_communicator_app_cxx 保持一致。
 */

#include "SharedMemoryCommunicator.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <chrono>
#include <csignal>
#include <atomic>
#include <iomanip>
#include <thread>

// 全局运行状态标志
std::atomic<bool> keep_running{true};

// 信号处理
void signal_handler(int sig) {
    std::cout << "\n接收到信号: " << sig << "，正在退出..." << std::endl;
    keep_running = false;
}

// 提取当前时间的纳秒 (Wall Clock)
uint64_t get_time_ns() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

/**
 * @brief 自定义通信器类，实现回调逻辑
 */
class MyCommunicator : public SharedMemoryCommunicator {
public:
    using SharedMemoryCommunicator::SharedMemoryCommunicator;

protected:
    /**
     * @brief 收到消息后的处理逻辑
     * @param data 负荷数据指针
     * @param header 解析出的头部元数据
     */
    void on_message_received(const void* data, const DataHeader* header) override {
        if (!header) return;

        uint64_t current_time_ns = get_time_ns();
        uint64_t latency_ns = current_time_ns - header->timestamp;
        double latency_us = latency_ns / 1000.0;

        std::cout << "[" << header->topic_id << "] 接收到来自 " << header->component_id 
                  << " 的消息, 序列号: " << header->seq 
                  << ", 大小: " << header->len << " 字节"
                  << ", 延迟: " << std::fixed << std::setprecision(2) << latency_us << " us" << std::endl;
    }
};

void print_user_help() {
    std::cout << "用法: SharedMemoryCommunicatorTest [options]\n"
              << "选项:\n"
              << "  --pub <topic>       发布主题\n"
              << "  --sub <topic>       订阅主题\n"
              << "  --size <bytes>      负载大小 (默认: 1024)\n"
              << "  --interval <ms>     发送间隔 (默认: 1000)\n"
              << "  --waitset <0|1>     启用 WaitSet (默认: 1)\n"
              << "  --config <path>     配置文件路径\n"
              << "  --help              显示帮助\n";
}

int main(int argc, char** argv) {
    std::vector<std::string> pub_topics;
    std::vector<std::string> sub_topics;
    size_t payload_size = 1024;
    size_t interval_ms = 1000;
    bool enable_waitset = true;
    std::string config_path = "";

    // 简易参数解析
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--pub" && i + 1 < argc) pub_topics.push_back(argv[++i]);
        else if (arg == "--sub" && i + 1 < argc) sub_topics.push_back(argv[++i]);
        else if (arg == "--size" && i + 1 < argc) payload_size = std::stoull(argv[++i]);
        else if (arg == "--interval" && i + 1 < argc) interval_ms = std::stoull(argv[++i]);
        else if (arg == "--waitset" && i + 1 < argc) enable_waitset = (std::stoi(argv[++i]) != 0);
        else if (arg == "--config" && i + 1 < argc) config_path = argv[++i];
        else if (arg == "--help") { print_user_help(); return 0; }
    }

    try {
        // 1. 构造通信器 (内部自动初始化 C API)
        MyCommunicator comm(config_path, enable_waitset);
        std::cout << "SharedMemoryCommunicator 已构造 (WaitSet: " << (enable_waitset ? "是" : "否") << ")" << std::endl;

        // 2. 注册主题
        for (const auto& t : pub_topics) {
            if (comm.registerPublisher(t, SHM_DATA_DYNAMIC)) {
                std::cout << "已注册发布者: " << t << std::endl;
            }
        }
        for (const auto& t : sub_topics) {
            if (comm.registerSubscriber(t, SHM_DATA_DYNAMIC)) {
                std::cout << "已注册订阅者: " << t << std::endl;
            }
        }

        // 3. 启动后台线程
        if (!comm.start()) {
            std::cerr << "通信器启动失败" << std::endl;
            return 1;
        }

        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        std::vector<uint8_t> dummy_data(payload_size, 0xAA);
        uint64_t sequence = 0;

        std::cout << "测试程序开始运行..." << std::endl;

        while (keep_running) {
            // 4. 发布消息
            for (const auto& t : pub_topics) {
                uint64_t now = get_time_ns();
                // 使用封装好的 publish 接口 (内部会自动填充 DataHeader)
                comm.publish(dummy_data.data(), dummy_data.size(), t.c_str(), now, sequence++, "AppCXX_Tester");
            }

            // 如果没启用 WaitSet，需要手动轮询
            if (!enable_waitset) {
                comm.poll();
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }

        comm.stop();
        std::cout << "进程已退出。" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "异常退出: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
