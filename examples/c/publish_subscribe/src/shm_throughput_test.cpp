#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <atomic>
#include <cstring>
#include <csignal>
#include <iomanip>
#include <thread>
#include "shm_communicator.h"

#ifdef _WIN32
#include <windows.h>
#define SLEEP_MS(ms) Sleep(ms)
#else
#include <unistd.h>
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

// 报文结构
struct PacketHeader {
    uint64_t sequence_number;
    uint64_t timestamp_ns;
};

// 全局状态
std::atomic<bool> keep_running(true);
std::atomic<uint64_t> g_received_count(0);
std::atomic<uint64_t> g_first_arrival_ns(0);
std::atomic<uint64_t> g_last_arrival_ns(0);
std::atomic<uint64_t> g_out_of_order_count(0);
uint64_t g_last_seq = 0;
uint64_t g_total_sent_by_sender = 0;
bool g_finished = false;

void signal_handler(int) {
    keep_running = false;
}

uint64_t get_time_ns() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

// 接收回调
void on_receive(const char* topic, const void* data, size_t size, void* user_context) {
    uint64_t now = get_time_ns();
    if (size < sizeof(PacketHeader)) return;

    const PacketHeader* header = static_cast<const PacketHeader*>(data);

    // 检查毒丸 (结束信号)
    if (header->sequence_number == 0xFFFFFFFFFFFFFFFFULL) {
        g_total_sent_by_sender = *reinterpret_cast<const uint64_t*>(static_cast<const uint8_t*>(data) + 16);
        g_finished = true;
        return;
    }

    if (g_received_count == 0) {
        g_first_arrival_ns = now;
    }
    g_last_arrival_ns = now;
    g_received_count++;

    if (header->sequence_number != g_last_seq + 1 && g_received_count > 1) {
        g_out_of_order_count++;
    }
    g_last_seq = header->sequence_number;
}

void print_help() {
    std::cout << "用法: shm_throughput_test [选项]\n"
              << "选项:\n"
              << "  --mode <sender|receiver>  运行模式 (默认: sender)\n"
              << "  --size <bytes>            负载大小 (默认: 1024)\n"
              << "  --count <number>          发送数量 (仅发送端, 默认: 1000000)\n"
              << "  --topic <name>            主题名 (默认: ThroughputTopic)\n"
              << "  --waitset <0|1>           是否开启 WaitSet (默认: 0, 使用 Polling 模式)\n"
              << "  --config <path>           iceoryx2 配置文件路径\n";
}

int main(int argc, char** argv) {
    std::string mode = "sender";
    size_t payload_size = 1024;
    uint64_t count = 1000000;
    std::string topic = "ThroughputTopic";
    bool enable_waitset = false;
    const char* config_path = nullptr;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--mode" && i + 1 < argc) mode = argv[++i];
        else if (arg == "--size" && i + 1 < argc) payload_size = std::stoul(argv[++i]);
        else if (arg == "--count" && i + 1 < argc) count = std::stoull(argv[++i]);
        else if (arg == "--topic" && i + 1 < argc) topic = argv[++i];
        else if (arg == "--waitset" && i + 1 < argc) enable_waitset = (std::stoi(argv[++i]) != 0);
        else if (arg == "--config" && i + 1 < argc) config_path = argv[++i];
        else if (arg == "--help") { print_help(); return 0; }
    }

    std::signal(SIGINT, signal_handler);

    shm_communicator_t* comm = shm_communicator_create();
    if (!shm_communicator_init(comm, config_path, enable_waitset, on_receive, nullptr)) {
        std::cerr << "初始化失败" << std::endl;
        return 1;
    }

    if (mode == "sender") {
        shm_communicator_register_publisher(comm, topic.c_str());
        shm_communicator_start(comm, -1);

        std::cout << "发送端启动: Topic=" << topic << ", Size=" << payload_size << ", Count=" << count << std::endl;
        
        // 预热
        SLEEP_MS(1000);

        auto start_time = get_time_ns();
        for (uint64_t i = 1; i <= count && keep_running; ++i) {
            uint8_t* payload = nullptr;
            void* handle = nullptr;
            
            // 使用零拷贝接口进行极致吞吐测试
            // 注意：这依赖于您的 shm_communicator.c 是否仍保有这些接口。
            // 如果只有 shm_communicator_publish，我们将降级使用它。
            
            // 这里为了通用性，我们先尝试直接使用 publish，
            // 稍后如果需要更高吞吐，可以切换回原始的 loan/send/pre-loan 组合。
            
            std::vector<uint8_t> buffer(payload_size, 0);
            PacketHeader* h = reinterpret_cast<PacketHeader*>(buffer.data());
            h->sequence_number = i;
            h->timestamp_ns = get_time_ns();
            
            if (!shm_communicator_publish(comm, topic.c_str(), buffer.data(), payload_size)) {
                std::cerr << "发送失败 at " << i << std::endl;
            }
        }
        auto end_time = get_time_ns();

        // 发送毒丸
        std::vector<uint8_t> poison(payload_size > 24 ? payload_size : 24, 0);
        PacketHeader* ph = reinterpret_cast<PacketHeader*>(poison.data());
        ph->sequence_number = 0xFFFFFFFFFFFFFFFFULL;
        *reinterpret_cast<uint64_t*>(poison.data() + 16) = count;
        shm_communicator_publish(comm, topic.c_str(), poison.data(), poison.size());

        double duration_s = (end_time - start_time) / 1e9;
        std::cout << "发送完成. 时长: " << duration_s << "s, 吞吐量: " << (count / duration_s) << " pkts/s" << std::endl;

    } else {
        shm_communicator_register_subscriber(comm, topic.c_str());
        shm_communicator_start(comm, -1);

        std::cout << "接收端启动: Topic=" << topic << ", 等待数据..." << std::endl;

        while (keep_running && !g_finished) {
            if (!enable_waitset) {
                shm_communicator_poll(comm);
            } else {
                SLEEP_MS(10);
            }
        }

        if (g_received_count > 0) {
            double duration_s = (g_last_arrival_ns - g_first_arrival_ns) / 1e9;
            double mps = g_received_count / duration_s;
            double gbps = (mps * payload_size) / (1024 * 1024 * 1024);
            
            std::cout << "\n--- 测试结果 ---" << std::endl;
            std::cout << "负载大小: " << payload_size << " bytes" << std::endl;
            std::cout << "接收数量: " << g_received_count << std::endl;
            if (g_total_sent_by_sender > 0) {
                std::cout << "发送总数: " << g_total_sent_by_sender << std::endl;
                std::cout << "丢包率:   " << std::fixed << std::setprecision(4) 
                          << (1.0 - (double)g_received_count / g_total_sent_by_sender) * 100.0 << "%" << std::endl;
            }
            std::cout << "总耗时:   " << duration_s << " s" << std::endl;
            std::cout << "每秒包数: " << (uint64_t)mps << " pkts/s" << std::endl;
            std::cout << "吞吐带宽: " << gbps << " GB/s (" << gbps * 8 << " Gbps)" << std::endl;
            std::cout << "乱序数量: " << g_out_of_order_count << std::endl;
        }
    }

    shm_communicator_destroy(comm);
    return 0;
}
