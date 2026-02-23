#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>
#include <cstring>
#include <csignal>
#include <iomanip>
#include <thread>
#include <mutex>
#include <map>
#include <random>
#include "shm_communicator.h"

#ifdef _WIN32
#include <windows.h>
#define SLEEP_MS(ms) Sleep(ms)
#else
#include <unistd.h>
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

// 配置
const int TOPIC_COUNT = 10;
const int PUB_SET_SIZE = 5;  // Topics 0-4
const int SUB_SET_SIZE = 5;  // Topics 5-9

struct TopicState {
    std::string name;
    std::atomic<bool> is_registered{false};
    std::atomic<uint64_t> sent_count{0};
    std::atomic<uint64_t> received_count{0};
    std::atomic<uint64_t> unexpected_count{0}; // 在注销状态下收到的数据
};

struct AppConfig {
    std::string mode = "sender";
    const char* config_path = nullptr;
};

std::vector<TopicState> g_topics(TOPIC_COUNT);
std::atomic<bool> keep_running(true);
std::mutex g_log_mutex;

void signal_handler(int) {
    keep_running = false;
}

// 接收回调
void on_receive(const char* topic_name, const void* data, size_t size, void* user_context) {
    for (int i = 0; i < TOPIC_COUNT; ++i) {
        if (g_topics[i].name == topic_name) {
            if (!g_topics[i].is_registered) {
                g_topics[i].unexpected_count++;
            } else {
                g_topics[i].received_count++;
            }
            return;
        }
    }
}

// Churn 线程：随机注册/注销主题
void churn_thread_func(shm_communicator_t* comm, int start_idx, int count, bool as_publisher) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis_idx(start_idx, start_idx + count - 1);
    std::uniform_int_distribution<> dis_sleep(1000, 5000); // 1-5秒

    while (keep_running) {
        int idx = dis_idx(gen);
        bool target_state = !g_topics[idx].is_registered;

        {
            std::lock_guard<std::mutex> lock(g_log_mutex);
            std::cout << "[CHURN] Topic " << g_topics[idx].name 
                      << " -> " << (target_state ? "REGISTER" : "UNREGISTER") 
                      << (as_publisher ? " (PUB)" : " (SUB)") << std::endl;
        }

        if (target_state) {
            bool ok = as_publisher ? 
                shm_communicator_register_publisher(comm, g_topics[idx].name.c_str()) :
                shm_communicator_register_subscriber(comm, g_topics[idx].name.c_str());
            if (ok) g_topics[idx].is_registered = true;
        } else {
            if (as_publisher) {
                shm_communicator_unregister_publisher(comm, g_topics[idx].name.c_str());
            } else {
                shm_communicator_unregister_subscriber(comm, g_topics[idx].name.c_str());
            }
            g_topics[idx].is_registered = false;
        }

        SLEEP_MS(dis_sleep(gen));
    }
}

// 发送线程：持续尝试在指定范围的主题上发送
void traffic_thread_func(shm_communicator_t* comm, int start_idx, int count) {
    uint8_t dummy[64] = {0};
    while (keep_running) {
        // 尝试在指定的主题范围内发送
        for (int i = start_idx; i < start_idx + count; ++i) {
            if (shm_communicator_publish(comm, g_topics[i].name.c_str(), dummy, sizeof(dummy))) {
                g_topics[i].sent_count++;
            }
        }
        std::this_thread::yield();
    }
}

void print_help() {
    std::cout << "用法: shm_stress_test [选项]\n"
              << "选项:\n"
              << "  --mode <sender|receiver>  运行模式 (默认: sender)\n"
              << "  --config <path>           iceoryx2 配置文件路径\n";
}

AppConfig parse_args(int argc, char** argv) {
    AppConfig config;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--mode" && i + 1 < argc) config.mode = argv[++i];
        else if (arg == "--config" && i + 1 < argc) config.config_path = argv[++i];
        else if (arg == "--help") { print_help(); std::exit(0); }
    }
    return config;
}

int main(int argc, char** argv) {
    AppConfig config = parse_args(argc, argv);
    std::signal(SIGINT, signal_handler);

    // 初始化 10 个主题名称
    for (int i = 0; i < TOPIC_COUNT; ++i) {
        g_topics[i].name = "StressTopic_" + std::to_string(i);
    }

    shm_communicator_t* comm = shm_communicator_create();
    if (!shm_communicator_init(comm, config.config_path, true, on_receive, nullptr)) {
        std::cerr << "初始化失败" << std::endl;
        return 1;
    }
    shm_communicator_start(comm, -1);

    std::cout << "压力测试启动模式: " << config.mode << " (按 Ctrl+C 停止)" << std::endl;

    std::thread churn_th;
    std::vector<std::thread> traffic_threads;
    const int APP_TRAFFIC_THREADS = 5; // 启动 5 个线程，每个线程负责 2 个主题
    int topics_per_thread = TOPIC_COUNT / APP_TRAFFIC_THREADS;

    if (config.mode == "sender") {
        // 为了让接收端能测到东西，发送端对主题 5-9 进行持久化注册
        for (int i = PUB_SET_SIZE; i < TOPIC_COUNT; ++i) {
            if (!shm_communicator_register_publisher(comm, g_topics[i].name.c_str())) {
                std::cerr << "[ERROR] Failed to register permanent publisher: " << g_topics[i].name << std::endl;
            } else {
                g_topics[i].is_registered = true;
            }
        }
        // 对 Topics 0-4 进行随机注册注销压力测试
        churn_th = std::thread(churn_thread_func, comm, 0, PUB_SET_SIZE, true);
    } else {
        // 为了让发送端能测到东西，接收端对主题 0-4 进行持久化监听
        for (int i = 0; i < PUB_SET_SIZE; ++i) {
            if (!shm_communicator_register_subscriber(comm, g_topics[i].name.c_str())) {
                std::cerr << "[ERROR] Failed to register permanent subscriber: " << g_topics[i].name << std::endl;
            } else {
                g_topics[i].is_registered = true;
            }
        }
        // 对 Topics 5-9 进行随机注册注销压力测试
        churn_th = std::thread(churn_thread_func, comm, PUB_SET_SIZE, SUB_SET_SIZE, false);
    }

    // 统一启动流量线程，分发不同的主题索引
    for (int t = 0; t < APP_TRAFFIC_THREADS; ++t) {
        traffic_threads.emplace_back(traffic_thread_func, comm, t * topics_per_thread, topics_per_thread);
    }

    while (keep_running) {
        SLEEP_MS(2000);
        std::lock_guard<std::mutex> lock(g_log_mutex);
        std::cout << "\n--- 进程模式: " << config.mode << " 统计 ---" << std::endl;
        for (int i = 0; i < TOPIC_COUNT; ++i) {
            std::cout << "Topic " << std::setw(15) << g_topics[i].name 
                      << " | Reg: " << (g_topics[i].is_registered ? "Y" : "N")
                      << " | Sent: " << std::setw(8) << g_topics[i].sent_count 
                      << " | Recv: " << std::setw(8) << g_topics[i].received_count
                      << " | Unexpected: " << g_topics[i].unexpected_count << std::endl;
        }
    }

    if (churn_th.joinable()) churn_th.join();
    for (auto& t : traffic_threads) {
        if (t.joinable()) t.join();
    }

    shm_communicator_destroy(comm);
    std::cout << "测试进程退出。" << std::endl;
    return 0;
}
