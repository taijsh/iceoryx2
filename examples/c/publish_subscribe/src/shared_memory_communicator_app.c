// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

#include "shm_communicator.h"

#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
#include <windows.h>
#define SLEEP_MS(ms) Sleep(ms)
#else
#include <unistd.h>
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

// 全局运行状态标志
volatile bool keep_running = true;

// 信号处理
void signal_handler(int sig) {
    printf("接收到信号: %d\n", sig);
    keep_running = false;
}

// 提取当前时间的纳秒
uint64_t app_get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

// 订阅者的回调函数 (对应 C++ 中的 on_receive)
void app_on_receive(const char* topic, const void* data, size_t size, void* user_context) {
    if (size >= sizeof(uint64_t)) {
        uint64_t current_time_ns = app_get_time_ns();
        uint64_t send_time_ns;
        memcpy(&send_time_ns, data, sizeof(uint64_t));
        
        uint64_t latency_ns = current_time_ns - send_time_ns;
        double latency_us = latency_ns / 1000.0;
        
        printf("[%s] 接收到 %zu 字节, 延迟: %.2f us\n", topic, size, latency_us);
    } else {
        printf("[%s] 接收到 %zu 字节\n", topic, size);
    }
}

#define MAX_CLI_TOPICS 16

// 配置结构
typedef struct {
    char pub_topics[MAX_CLI_TOPICS][256];
    int pub_count;
    char sub_topics[MAX_CLI_TOPICS][256];
    int sub_count;
    size_t payload_size;
    size_t interval_ms;
    int cpu_core_id;
    bool enable_waitset;
    char config_path[1024];
} AppConfig;

void print_help() {
    printf("用法: shared_memory_communicator_app [options]\n");
    printf("选项:\n");
    printf("  --pub <topic>       发布到主题 (可多次使用)\n");
    printf("  --sub <topic>       订阅到主题 (可多次使用)\n");
    printf("  --size <bytes>      负载大小字节数 (默认: 1024)\n");
    printf("  --interval <ms>     发布间隔毫秒数 (默认: 1000)\n");
    printf("  --waitset <0|1>     是否启用系统级 WaitSet 事件驱动机制 (默认: 1)\n");
    printf("  --cpu <id>          绑定接收线程到指定 CPU 核心 (默认: -1 不绑定)\n");
    printf("  --config <path>     配置文件路径 (默认: /home/taijsh/ljtx/ljtx_component.toml)\n");
    printf("  --help              显示本帮助信息\n");
}

AppConfig parse_args(int argc, char** argv) {
    AppConfig config;
    memset(&config, 0, sizeof(config));
    config.payload_size = 1024;
    config.interval_ms = 1000;
    config.cpu_core_id = -1;
    config.enable_waitset = true;
    strcpy(config.config_path, "/home/taijsh/ljtx/ljtx_component.toml");

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--pub") == 0) {
            if (i + 1 < argc && config.pub_count < MAX_CLI_TOPICS) {
                strncpy(config.pub_topics[config.pub_count++], argv[++i], 255);
            } else {
                fprintf(stderr, "错误: --pub 需要一个有效主题参数，或已达到最大数量\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--sub") == 0) {
            if (i + 1 < argc && config.sub_count < MAX_CLI_TOPICS) {
                strncpy(config.sub_topics[config.sub_count++], argv[++i], 255);
            } else {
                fprintf(stderr, "错误: --sub 需要一个有效主题参数，或已达到最大数量\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--size") == 0) {
            if (i + 1 < argc) {
                config.payload_size = (size_t)strtoul(argv[++i], NULL, 10);
            } else {
                fprintf(stderr, "错误: --size 需要指定字节数\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--interval") == 0) {
            if (i + 1 < argc) {
                config.interval_ms = (size_t)strtoul(argv[++i], NULL, 10);
            } else {
                fprintf(stderr, "错误: --interval 需要指定毫秒数\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--waitset") == 0) {
            if (i + 1 < argc) {
                config.enable_waitset = (atoi(argv[++i]) != 0);
            } else {
                fprintf(stderr, "错误: --waitset 需要 0 或 1\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--cpu") == 0) {
            if (i + 1 < argc) {
                config.cpu_core_id = (int)strtoll(argv[++i], NULL, 10);
            } else {
                fprintf(stderr, "错误: --cpu 需要指定核心ID\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--config") == 0) {
            if (i + 1 < argc) {
                strncpy(config.config_path, argv[++i], sizeof(config.config_path) - 1);
            } else {
                fprintf(stderr, "错误: --config 需要配置文件路径\n");
                exit(1);
            }
        } else if (strcmp(argv[i], "--help") == 0) {
            print_help();
            exit(0);
        } else {
            fprintf(stderr, "未知参数: %s\n", argv[i]);
            print_help();
            exit(1);
        }
    }
    return config;
}

int main(int argc, char** argv) {
    AppConfig config = parse_args(argc, argv);

    // 1. 创建通信器
    shm_communicator_t* comm = shm_communicator_create();
    if (!shm_communicator_init(comm, config.config_path, config.enable_waitset, app_on_receive, NULL)) {
        fprintf(stderr, "无法初始化 SharedMemoryCommunicator\n");
        return 1;
    }
    printf("初始化成功\n");

    // 设置信号 (iceoryx2 内部的信号拦截已被显式禁用，这里的原生信号监听可以正常工作)
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 2. 注册发布者
    for (int i = 0; i < config.pub_count; ++i) {
        if (!shm_communicator_register_publisher(comm, config.pub_topics[i])) {
            fprintf(stderr, "无法注册发布者主题: %s\n", config.pub_topics[i]);
            return 1;
        }
    }
    if (config.pub_count > 0) printf("发布者注册成功\n");

    // 3. 注册订阅者
    for (int i = 0; i < config.sub_count; ++i) {
        printf("订阅者注册:%s\n", config.sub_topics[i]);
        if (!shm_communicator_register_subscriber(comm, config.sub_topics[i])) {
            fprintf(stderr, "无法注册订阅者主题: %s\n", config.sub_topics[i]);
            return 1;
        }
        printf("订阅者注册:%s成功\n", config.sub_topics[i]);
    }
    if (config.sub_count > 0) printf("订阅者注册成功\n");

    printf("应用程序已启动。\n");
    printf("配置校验:\n");
    printf("  负载大小: %zu 字节\n", config.payload_size);
    printf("  间隔: %zu 毫秒\n", config.interval_ms);
    printf("  启用 WaitSet 事件机制: %s\n", config.enable_waitset ? "是 (微秒级延迟,低CPU)" : "否 (纯忙轮询,纳秒级延迟,高CPU)");
    printf("  绑定 CPU 核心: %d\n", config.cpu_core_id);
    printf("  配置路径: %s\n", config.config_path);

    // 4. 启动后台处理线程
    if (!shm_communicator_start(comm, config.cpu_core_id)) {
        fprintf(stderr, "警告: 无法启动后台处理线程或平台不支持。\n");
    }

    // 创建虚拟数据负载
    uint8_t* dummy_data = (uint8_t*)malloc(config.payload_size);
    for (size_t i = 0; i < config.payload_size; ++i) {
        dummy_data[i] = (uint8_t)(i % 255);
    }

    while (keep_running) {
        // 4. 对所有注册的发布者发送带有当前时间戳的 Payload
        for (int i = 0; i < config.pub_count; ++i) {
            if (config.payload_size >= sizeof(uint64_t)) {
                uint64_t current_time_ns = app_get_time_ns();
                memcpy(dummy_data, &current_time_ns, sizeof(uint64_t));
            }
            shm_communicator_publish(comm, config.pub_topics[i], dummy_data, config.payload_size);
        }

        // 5. 根据配置接收数据
        if (config.enable_waitset) {
            // WaitSet 模式下，后台已有专门线程处理数据接收。这里仅作为每次发送的定期间隔休眠。
            for (size_t elapsed_ms = 0; elapsed_ms < config.interval_ms && keep_running; elapsed_ms += 100) {
                shm_communicator_process_events(comm, 100);
            }
        } else {
            // 忙轮询模式：不再休眠，死循环主动探取，直到达到发送时间（或者用户自行将发送与接收跨线程拆分）。
            uint64_t start_time = app_get_time_ns();
            uint64_t interval_ns = config.interval_ms * 1000000ULL;
            while (keep_running && (app_get_time_ns() - start_time) < interval_ns) {
                shm_communicator_poll(comm);
            }
        }
    }

    printf("应用程序正在停止...\n");

    free(dummy_data);
    shm_communicator_destroy(comm);

    return 0;
}
