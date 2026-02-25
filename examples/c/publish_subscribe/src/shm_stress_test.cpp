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

    std::cout << "压力测试启动模式: " << config.mode << " (单线程模式, 按 Ctrl+C 停止)" << std::endl;

    // 随机数生成器用于 Churn
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis_idx_sender(0, PUB_SET_SIZE - 1);
    std::uniform_int_distribution<> dis_idx_receiver(PUB_SET_SIZE, TOPIC_COUNT - 1);
    std::uniform_int_distribution<> dis_sleep(1000, 5000); // 1-5秒

    // 准备发送数据
    uint8_t dummy[64];
    for(int i=0; i<64; ++i) dummy[i] = (uint8_t)i;

    if (config.mode == "sender") {
        // 为了让接收端能测到东西，发送端对主题 5-9 进行持久化注册
        for (int i = PUB_SET_SIZE; i < TOPIC_COUNT; ++i) {
            if (shm_communicator_register_publisher(comm, g_topics[i].name.c_str())) {
                g_topics[i].is_registered = true;
            }
        }
    } else {
        // 为了让发送端能测到东西，接收端对主题 0-4 进行持久化监听
        for (int i = 0; i < PUB_SET_SIZE; ++i) {
            if (shm_communicator_register_subscriber(comm, g_topics[i].name.c_str())) {
                g_topics[i].is_registered = true;
            }
        }
    }

    auto last_stats_time = std::chrono::steady_clock::now();
    auto last_churn_time = std::chrono::steady_clock::now();
    auto next_churn_interval = std::chrono::milliseconds(dis_sleep(gen));

    while (keep_running) {
        auto now = std::chrono::steady_clock::now();

        // 1. Churn 逻辑 (随机注册/注销)
        if (now - last_churn_time >= next_churn_interval) {
            int idx = (config.mode == "sender") ? dis_idx_sender(gen) : dis_idx_receiver(gen);
            bool target_state = !g_topics[idx].is_registered;

            std::cout << "[CHURN] Topic " << g_topics[idx].name 
                      << " -> " << (target_state ? "REGISTER" : "UNREGISTER") << std::endl;

            if (target_state) {
                bool ok = (config.mode == "sender") ? 
                    shm_communicator_register_publisher(comm, g_topics[idx].name.c_str()) :
                    shm_communicator_register_subscriber(comm, g_topics[idx].name.c_str());
                if (ok) g_topics[idx].is_registered = true;
            } else {
                if (config.mode == "sender") {
                    shm_communicator_unregister_publisher(comm, g_topics[idx].name.c_str());
                } else {
                    shm_communicator_unregister_subscriber(comm, g_topics[idx].name.c_str());
                }
                g_topics[idx].is_registered = false;
            }

            last_churn_time = now;
            next_churn_interval = std::chrono::milliseconds(dis_sleep(gen));
        }

        // 2. Traffic 逻辑 (发送)
        // 即使在 receiver 模式下，我们也尝试发送，但这取决于 user 逻辑。
        // 目前 stress_test 的 traffic 是在 0-9 全范围尝试发送？
        // 之前的代码是: for (int i = start_idx; i < start_idx + count; ++i) { ... }
        // 改为遍历所有主题，如果是 registered publisher 就会发送成功
        for (int i = 0; i < TOPIC_COUNT; ++i) {
            // 注意: publish 内部会查表，如果不匹配则直接返回 false
            if (shm_communicator_publish(comm, g_topics[i].name.c_str(), dummy, sizeof(dummy))) {
                g_topics[i].sent_count++;
            }
        }

        // 3. Stats 逻辑 (每 2 秒打印一次)
        if (now - last_stats_time >= std::chrono::seconds(2)) {
            std::cout << "\n--- 进程模式: " << config.mode << " 统计 (单线程) ---" << std::endl;
            for (int i = 0; i < TOPIC_COUNT; ++i) {
                std::cout << "Topic " << std::setw(15) << g_topics[i].name 
                          << " | Reg: " << (g_topics[i].is_registered ? "Y" : "N")
                          << " | Sent: " << std::setw(8) << g_topics[i].sent_count 
                          << " | Recv: " << std::setw(8) << g_topics[i].received_count
                          << " | Unexpected: " << g_topics[i].unexpected_count << std::endl;
            }
            last_stats_time = now;
        }

        // 让出 CPU，避免疯狂空转
        //std::this_thread::yield();
    }

    shm_communicator_destroy(comm);
    std::cout << "测试进程退出。" << std::endl;
    return 0;
}
