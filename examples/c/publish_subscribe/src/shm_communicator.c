// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

// 文件描述: shm_communicator.c
// 该文件实现了共享内存通信器的 C API 封装。

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "shm_communicator.h"
#include "iox2/iceoryx2.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#endif

#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
#include <windows.h>
#define alignof __alignof
#define SLEEP_MS(ms) Sleep(ms)
#else
#include <stdalign.h>
#define SLEEP_MS(ms) usleep((ms) * 1000)
#endif

// 忙等待辅助函数 (微秒级精度)
static void busy_wait_us(uint64_t microseconds) {
#ifdef __linux__
    struct timespec start, current;
    clock_gettime(CLOCK_MONOTONIC, &start);
    uint64_t start_ns = (uint64_t)start.tv_sec * 1000000000ULL + start.tv_nsec;
    uint64_t wait_ns = microseconds * 1000ULL;
    
    while (1) {
        clock_gettime(CLOCK_MONOTONIC, &current);
        uint64_t current_ns = (uint64_t)current.tv_sec * 1000000000ULL + current.tv_nsec;
        if (current_ns - start_ns >= wait_ns) break;
    }
#else
    // Windows 平台简单实现 (使用 QueryPerformanceCounter)
    LARGE_INTEGER frequency, start, current;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&start);
    int64_t wait_ticks = (microseconds * frequency.QuadPart) / 1000000;
    while (1) {
        QueryPerformanceCounter(&current);
        if (current.QuadPart - start.QuadPart >= wait_ticks) break;
    }
#endif
}

#define MAX_TOPICS 32

typedef struct shm_communicator_t shm_communicator_t;
typedef struct PublisherEntry PublisherEntry;
typedef struct SubscriberEntry SubscriberEntry;

static bool shm_communicator_ensure_pre_loan_internal(PublisherEntry* entry);
static void process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry, bool clear_events);

// 生命周期指令类型
typedef enum {
    LIFECYCLE_CMD_NONE = 0,
    LIFECYCLE_CMD_REG_PUB,
    LIFECYCLE_CMD_UNREG_PUB,
    LIFECYCLE_CMD_REG_SUB,
    LIFECYCLE_CMD_UNREG_SUB
} lifecycle_cmd_type_e;

// 生命周期指令结构 (用于异步管理)
typedef struct {
    volatile lifecycle_cmd_type_e type;
    char topic[256];
    volatile bool done;
    volatile bool result;
} lifecycle_command_t;

// 发布者封装
typedef struct PublisherEntry {
    char topic[256];
    iox2_service_name_h service_name;
    iox2_port_factory_pub_sub_h service;
    iox2_publisher_h publisher;

    // 事件服务通知器 (用于通知WaitSet)
    iox2_service_name_h event_service_name;
    iox2_port_factory_event_h event_service;
    iox2_notifier_h notifier;

    // 预借用机制 (Pre-loan mechanism)
    iox2_sample_mut_h pre_loaned_sample;
    size_t pre_loaned_size;
    size_t max_loaned_size; // 记录历史最大的借用尺寸 (High Watermark)

    volatile bool marked_for_unregistration; // 标记是否注销
} PublisherEntry;

// 订阅者封装
typedef struct SubscriberEntry {
    char topic[256];
    iox2_service_name_h service_name;
    iox2_port_factory_pub_sub_h service;
    iox2_subscriber_h subscriber;

    // 事件服务监听器
    iox2_service_name_h event_service_name;
    iox2_port_factory_event_h event_service;
    iox2_listener_h listener;
    
    // WaitSet 守卫句柄
    iox2_waitset_guard_h guard;

    volatile bool marked_for_unregistration; // 标记是否注销
} SubscriberEntry;

// 通信器结构体定义
struct shm_communicator_t {
    iox2_config_h config;
    iox2_node_h node_handle;
    iox2_waitset_h waitset;
    
    // 保护性虚拟事件，用于防止 WaitSet 为空时报错，也用于唤醒线程
    iox2_service_name_h dummy_service_name;
    iox2_port_factory_event_h dummy_service;
    iox2_listener_h dummy_listener;
    iox2_waitset_guard_h dummy_guard;
    iox2_notifier_h dummy_notifier;
    
    PublisherEntry publishers[MAX_TOPICS];
    SubscriberEntry subscribers[MAX_TOPICS];
    on_receive_cb user_callback;
    void* user_context;
    bool enable_waitset;

    // 异步生命周期管理指令槽
    lifecycle_command_t cmd;

#ifdef __linux__
    pthread_t waitset_thread;
    volatile bool thread_running;
#endif
};

// 内部同步操作封装 (供背景线程直接调用)
static bool internal_register_publisher(shm_communicator_t* comm, const char* topic);
static void internal_unregister_publisher(shm_communicator_t* comm, const char* topic);
static bool internal_register_subscriber(shm_communicator_t* comm, const char* topic);
static void internal_unregister_subscriber(shm_communicator_t* comm, const char* topic);

// 真正的资源释放逻辑
static void real_drop_publisher(PublisherEntry* entry);
static void real_drop_subscriber(SubscriberEntry* entry);

shm_communicator_t* shm_communicator_create(void) {
    shm_communicator_t* comm = (shm_communicator_t*)calloc(1, sizeof(shm_communicator_t));
    return comm;
}

bool shm_communicator_init(shm_communicator_t* comm,
                           const char* config_path,
                           bool enable_waitset,
                           on_receive_cb callback,
                           void* user_context) {
    if (!comm) return false;

    comm->user_callback = callback;
    comm->user_context = user_context;
    comm->enable_waitset = enable_waitset;

    // 默认加载指定配置文件或系统配置
    comm->config = NULL;
    if (config_path != NULL) {
        if (iox2_config_from_file(NULL, &comm->config, config_path) != IOX2_OK) {
            printf("无法加载配置文件: %s\n", config_path);
            return false;
        }
    }

    iox2_node_builder_h node_builder = iox2_node_builder_new(NULL);
    if (comm->config) {
        iox2_node_builder_set_config(&node_builder, &comm->config);
    }
    iox2_node_builder_set_signal_handling_mode(&node_builder, iox2_signal_handling_mode_e_DISABLED);

    if (iox2_node_builder_create(node_builder, NULL, iox2_service_type_e_IPC, &comm->node_handle) != IOX2_OK) {
        printf("无法创建节点。\n");
        return false;
    }

    if (comm->enable_waitset) {
        iox2_waitset_builder_h waitset_builder = NULL;
        iox2_waitset_builder_new(NULL, &waitset_builder);
        iox2_waitset_builder_set_signal_handling_mode(&waitset_builder, iox2_signal_handling_mode_e_DISABLED);
        if (iox2_waitset_builder_create(waitset_builder, iox2_service_type_e_IPC, NULL, &comm->waitset) != IOX2_OK) {
            printf("无法创建 WaitSet。\n");
            return false;
        }

        // 使用 PID 确保 Dummy Event 名字在多进程环境下唯一
        char dummy_name[256];
        snprintf(dummy_name, sizeof(dummy_name), "ShmCommunicator_Dummy_%d", (int)getpid());

        if (iox2_service_name_new(NULL, dummy_name, strlen(dummy_name), &comm->dummy_service_name) == IOX2_OK) {
            iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(comm->dummy_service_name));
            iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
            if (iox2_service_builder_event_open_or_create(ev_b, NULL, &comm->dummy_service) == IOX2_OK) {
                iox2_port_factory_listener_builder_h list_b = iox2_port_factory_event_listener_builder(&comm->dummy_service, NULL);
                if (iox2_port_factory_listener_builder_create(list_b, NULL, &comm->dummy_listener) == IOX2_OK) {
                    iox2_waitset_attach_notification(&comm->waitset, iox2_listener_get_file_descriptor(&comm->dummy_listener), NULL, &comm->dummy_guard);
                }
                iox2_port_factory_notifier_builder_h not_b = iox2_port_factory_event_notifier_builder(&comm->dummy_service, NULL);
                iox2_port_factory_notifier_builder_create(not_b, NULL, &comm->dummy_notifier);
            }
        }
    }

    return true;
}

// 指令提交辅助函数
static bool submit_lifecycle_command(shm_communicator_t* comm, lifecycle_cmd_type_e type, const char* topic) {
    if (!comm) return false;
    
    // 如果后台线程没启动，退化为直接执行 (仅限 init 阶段或单线程模式)
#ifdef __linux__
    if (!comm->thread_running) {
        switch (type) {
            case LIFECYCLE_CMD_REG_PUB: return internal_register_publisher(comm, topic);
            case LIFECYCLE_CMD_UNREG_PUB: internal_unregister_publisher(comm, topic); return true;
            case LIFECYCLE_CMD_REG_SUB: return internal_register_subscriber(comm, topic);
            case LIFECYCLE_CMD_UNREG_SUB: internal_unregister_subscriber(comm, topic); return true;
            default: return false;
        }
    }
#endif

    // 填充指令
    comm->cmd.done = false;
    comm->cmd.result = false;
    strncpy(comm->cmd.topic, topic, sizeof(comm->cmd.topic) - 1);
    
    // 强制同步屏障 (简单 volatile 写入，确保 topic 在 type 之前被背景线程可见)
    comm->cmd.type = type;

    // 唤醒后台线程
    if (comm->dummy_notifier) {
        iox2_notifier_notify(&comm->dummy_notifier, NULL);
    }

    // 等待执行完成 (Busy Wait)
    uint64_t wait_count = 0;
    while (!comm->cmd.done && comm->thread_running) {
        if (++wait_count > 1000000) { // 稍微慢一点的重试
            if (comm->dummy_notifier) iox2_notifier_notify(&comm->dummy_notifier, NULL);
            wait_count = 0;
        }
        sched_yield();
    }

    return comm->cmd.result;
}

bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic) {
    return submit_lifecycle_command(comm, LIFECYCLE_CMD_REG_PUB, topic);
}

void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    // 注销改为纯异步标记模式，无需等待背景线程，防止退出时死锁
    internal_unregister_publisher(comm, topic);
    // 同时也尝试唤醒背景线程去处理 GC
    if (comm->dummy_notifier) {
        iox2_notifier_notify(&comm->dummy_notifier, NULL);
    }
}

bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic) {
    return submit_lifecycle_command(comm, LIFECYCLE_CMD_REG_SUB, topic);
}

void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    // 注销改为纯异步标记模式，无需等待背景线程，防止退出时死锁
    internal_unregister_subscriber(comm, topic);
    // 同时也尝试唤醒背景线程去处理 GC
    if (comm->dummy_notifier) {
        iox2_notifier_notify(&comm->dummy_notifier, NULL);
    }
}

// ---------------- 内部实际执行逻辑 ----------------

static bool internal_register_publisher(shm_communicator_t* comm, const char* topic) {
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            // 如果已标记注销，则清除标记实现重用
            if (comm->publishers[i].marked_for_unregistration) {
                comm->publishers[i].marked_for_unregistration = false;
            }
            return true; // Already registered
        }
    }

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] == '\0') {
            entry = &comm->publishers[i]; break;
        }
    }
    if (!entry) return false;

    memset(entry, 0, sizeof(PublisherEntry));

    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) return false;
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        iox2_service_name_drop(entry->service_name); return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        iox2_service_name_drop(entry->service_name); return false;
    }

    iox2_port_factory_publisher_builder_h pub_builder = iox2_port_factory_pub_sub_publisher_builder(&entry->service, NULL);
    iox2_port_factory_publisher_builder_set_allocation_strategy(&pub_builder, iox2_allocation_strategy_e_POWER_OF_TWO);
    iox2_port_factory_publisher_builder_set_initial_max_slice_len(&pub_builder, 1024);
    if (iox2_port_factory_publisher_builder_create(pub_builder, NULL, &entry->publisher) != IOX2_OK) {
        iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }

    char event_topic[512];
    snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
    if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
        iox2_publisher_drop(entry->publisher); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }
    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
        iox2_service_name_drop(entry->event_service_name); iox2_publisher_drop(entry->publisher); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }
    iox2_port_factory_notifier_builder_h not_builder = iox2_port_factory_event_notifier_builder(&entry->event_service, NULL);
    if (iox2_port_factory_notifier_builder_create(not_builder, NULL, &entry->notifier) != IOX2_OK) {
        iox2_port_factory_event_drop(entry->event_service); iox2_service_name_drop(entry->event_service_name); iox2_publisher_drop(entry->publisher); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    return true;
}

static void real_drop_publisher(PublisherEntry* entry) {
    if (!entry) return;
    memset(entry->topic, 0, sizeof(entry->topic));
    if (entry->notifier) { iox2_notifier_drop(entry->notifier); entry->notifier = NULL; }
    if (entry->event_service) { iox2_port_factory_event_drop(entry->event_service); entry->event_service = NULL; }
    if (entry->event_service_name) { iox2_service_name_drop(entry->event_service_name); entry->event_service_name = NULL; }
    if (entry->publisher) { iox2_publisher_drop(entry->publisher); entry->publisher = NULL; }
    if (entry->service) { iox2_port_factory_pub_sub_drop(entry->service); entry->service = NULL; }
    if (entry->service_name) { iox2_service_name_drop(entry->service_name); entry->service_name = NULL; }
    if (entry->pre_loaned_sample) { iox2_sample_mut_drop(entry->pre_loaned_sample); entry->pre_loaned_sample = NULL; }
}

static void internal_unregister_publisher(shm_communicator_t* comm, const char* topic) {
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            // 仅标记注销，不立即释放资源
            comm->publishers[i].marked_for_unregistration = true;
            break;
        }
    }
}

static bool internal_register_subscriber(shm_communicator_t* comm, const char* topic) {
    SubscriberEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] != '\0' && strcmp(comm->subscribers[i].topic, topic) == 0) {
            // 如果已标记注销，则清除标记实现重用
            if (comm->subscribers[i].marked_for_unregistration) {
                comm->subscribers[i].marked_for_unregistration = false;
            }
            return true;
        }
    }

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] == '\0') {
            entry = &comm->subscribers[i]; break;
        }
    }
    if (!entry) return false;

    memset(entry, 0, sizeof(SubscriberEntry));

    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) return false;
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        iox2_service_name_drop(entry->service_name); return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        iox2_service_name_drop(entry->service_name); return false;
    }

    iox2_port_factory_subscriber_builder_h sub_builder = iox2_port_factory_pub_sub_subscriber_builder(&entry->service, NULL);
    if (iox2_port_factory_subscriber_builder_create(sub_builder, NULL, &entry->subscriber) != IOX2_OK) {
        iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }

    if (comm->enable_waitset) {
        char event_topic[512];
        snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
        if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
            iox2_subscriber_drop(entry->subscriber); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
        }
        iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
        iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
        if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
            iox2_service_name_drop(entry->event_service_name); iox2_subscriber_drop(entry->subscriber); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
        }
        iox2_port_factory_listener_builder_h list_b = iox2_port_factory_event_listener_builder(&entry->event_service, NULL);
        if (iox2_port_factory_listener_builder_create(list_b, NULL, &entry->listener) != IOX2_OK) {
            iox2_port_factory_event_drop(entry->event_service); iox2_service_name_drop(entry->event_service_name); iox2_subscriber_drop(entry->subscriber); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
        }
        if (iox2_waitset_attach_notification(&comm->waitset, iox2_listener_get_file_descriptor(&entry->listener), NULL, &entry->guard) != IOX2_OK) {
            iox2_listener_drop(entry->listener); iox2_port_factory_event_drop(entry->event_service); iox2_service_name_drop(entry->event_service_name); iox2_subscriber_drop(entry->subscriber); iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
        }
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    return true;
}

static void real_drop_subscriber(SubscriberEntry* entry) {
    if (!entry) return;
    memset(entry->topic, 0, sizeof(entry->topic));
    if (entry->guard) { iox2_waitset_guard_drop(entry->guard); entry->guard = NULL; }
    if (entry->listener) { iox2_listener_drop(entry->listener); entry->listener = NULL; }
    if (entry->event_service) { iox2_port_factory_event_drop(entry->event_service); entry->event_service = NULL; }
    if (entry->event_service_name) { iox2_service_name_drop(entry->event_service_name); entry->event_service_name = NULL; }
    if (entry->subscriber) { iox2_subscriber_drop(entry->subscriber); entry->subscriber = NULL; }
    if (entry->service) { iox2_port_factory_pub_sub_drop(entry->service); entry->service = NULL; }
    if (entry->service_name) { iox2_service_name_drop(entry->service_name); entry->service_name = NULL; }
}

static void internal_unregister_subscriber(shm_communicator_t* comm, const char* topic) {
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] != '\0' && strcmp(comm->subscribers[i].topic, topic) == 0) {
            // 仅标记注销，不立即释放资源
            comm->subscribers[i].marked_for_unregistration = true;
            break;
        }
    }
}

// ---------------- 数据收发路径 ----------------

bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size) {
    void* sample_h = NULL;
    void* payload = shm_communicator_loan_uninit(comm, topic, size, &sample_h);
    if (!payload) return false;
    memcpy(payload, data, size);
    bool success = shm_communicator_send(comm, topic, sample_h);
    if (success) shm_communicator_ensure_pre_loan(comm, topic);
    return success;
}

void* shm_communicator_loan_uninit(shm_communicator_t* comm, const char* topic, size_t size, void** sample_handle) {
    if (!comm || !topic || !sample_handle) return NULL;
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            // 如果已标记注销，拒绝借用
            if (comm->publishers[i].marked_for_unregistration) return NULL;
            entry = &comm->publishers[i]; break;
        }
    }
    if (!entry || entry->publisher == NULL) return NULL;
    
    uint8_t* payload = NULL;
    c_size_t num_elements = 0;
    if (entry->pre_loaned_sample != NULL) {
        if (entry->pre_loaned_size < size) {
            iox2_sample_mut_drop(entry->pre_loaned_sample); entry->pre_loaned_sample = NULL; entry->pre_loaned_size = 0;
        } else {
            iox2_sample_mut_payload_mut(&entry->pre_loaned_sample, (void**) &payload, &num_elements);
            *sample_handle = (void*)entry->pre_loaned_sample; entry->pre_loaned_sample = NULL; entry->pre_loaned_size = 0;
            return (void*)payload;
        }
    }

    if (size > entry->max_loaned_size) entry->max_loaned_size = size;
    iox2_sample_mut_h sample = NULL;
    if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &sample, size) != IOX2_OK) return NULL;
    iox2_sample_mut_payload_mut(&sample, (void**) &payload, &num_elements);
    *sample_handle = (void*)sample;
    return (void*)payload;
}

bool shm_communicator_send(shm_communicator_t* comm, const char* topic, void* sample_handle) {
    if (!comm || !topic || !sample_handle) return false;
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            // 如果已标记注销，拒绝发送
            if (comm->publishers[i].marked_for_unregistration) return false;
            entry = &comm->publishers[i]; break;
        }
    }
    if (!entry || entry->publisher == NULL || entry->notifier == NULL) return false;

    iox2_sample_mut_h sample = (iox2_sample_mut_h)sample_handle;
    if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
        iox2_sample_mut_drop(sample);
        return false;
    }
    if (entry->notifier) iox2_notifier_notify(&entry->notifier, NULL);
    return true;
}

static bool shm_communicator_ensure_pre_loan_internal(PublisherEntry* entry) {
    if (entry->pre_loaned_sample == NULL && entry->max_loaned_size > 0 && entry->publisher != NULL) {
        if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &entry->pre_loaned_sample, entry->max_loaned_size) == IOX2_OK) {
            entry->pre_loaned_size = entry->max_loaned_size;
            return true;
        }
    }
    return (entry->pre_loaned_sample != NULL);
}

bool shm_communicator_ensure_pre_loan(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return false;
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            entry = &comm->publishers[i]; break;
        }
    }
    if (!entry) return false;
    return shm_communicator_ensure_pre_loan_internal(entry);
}

static void process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry, bool clear_events) {
    if (!entry || entry->subscriber == NULL) return;
    if (clear_events && entry->listener) {
        iox2_event_id_t event_id;
        bool has_received = false;
        do {
            has_received = false;
            iox2_listener_try_wait_one(&entry->listener, &event_id, &has_received);
        } while (has_received);
    }

    bool has_samples = false;
    do {
        has_samples = false;
        iox2_sample_h sample = NULL;
        if (iox2_subscriber_receive(&entry->subscriber, NULL, &sample) == IOX2_OK && sample != NULL) {
            has_samples = true;
            const uint8_t* payload = NULL;
            c_size_t num_elements = 0;
            iox2_sample_payload(&sample, (const void**) &payload, &num_elements);
            // 只有在未注销的情况下才调用用户回调
            if (comm->user_callback && !entry->marked_for_unregistration) {
                comm->user_callback(entry->topic, payload, num_elements, comm->user_context);
            }
            iox2_sample_drop(sample);
        }
    } while (has_samples);
}

static iox2_callback_progression_e waitset_on_event(iox2_waitset_attachment_id_h attachment_id, void* context) {
    shm_communicator_t* comm = (shm_communicator_t*)context;
    bool handled = false;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] != '\0' && comm->subscribers[i].guard != NULL) {
            if (iox2_waitset_attachment_id_has_event_from(&attachment_id, &comm->subscribers[i].guard)) {
                process_subscriber_entry(comm, &comm->subscribers[i], true); handled = true;
            }
        }
    }

    if (!handled && comm->dummy_guard && iox2_waitset_attachment_id_has_event_from(&attachment_id, &comm->dummy_guard)) {
        iox2_event_id_t dummy_id; bool dummy_rcv = false;
        do { dummy_rcv = false; iox2_listener_try_wait_one(&comm->dummy_listener, &dummy_id, &dummy_rcv); } while (dummy_rcv);
    }
    
    return iox2_callback_progression_e_CONTINUE;
}

// 指令处理逻辑 (背景线程调用)
static void process_lifecycle_commands(shm_communicator_t* comm) {
    if (!comm || comm->cmd.type == LIFECYCLE_CMD_NONE) return;

    lifecycle_cmd_type_e type = comm->cmd.type;
    switch (type) {
        case LIFECYCLE_CMD_REG_PUB: comm->cmd.result = internal_register_publisher(comm, comm->cmd.topic); break;
        case LIFECYCLE_CMD_UNREG_PUB: internal_unregister_publisher(comm, comm->cmd.topic); comm->cmd.result = true; break;
        case LIFECYCLE_CMD_REG_SUB: comm->cmd.result = internal_register_subscriber(comm, comm->cmd.topic); break;
        case LIFECYCLE_CMD_UNREG_SUB: internal_unregister_subscriber(comm, comm->cmd.topic); comm->cmd.result = true; break;
        default: break;
    }

    comm->cmd.type = LIFECYCLE_CMD_NONE;
    comm->cmd.done = true;
}

// 背景线程调用的垃圾回收逻辑 (GC)
static void shm_communicator_gc_internal(shm_communicator_t* comm) {
    if (!comm) return;

    // 处理发布者 GC
    for (int i = 0; i < MAX_TOPICS; ++i) {
        PublisherEntry* entry = &comm->publishers[i];
        if (entry->topic[0] != '\0' && entry->marked_for_unregistration) {
            // 检查订阅者数量是否为 0
            size_t remote_subs = iox2_port_factory_pub_sub_dynamic_config_number_of_subscribers(&entry->service);
            if (remote_subs == 0) {
                // 真正的释放资源
                real_drop_publisher(entry);
            }
        }
    }

    // 处理订阅者接收与 GC
    for (int i = 0; i < MAX_TOPICS; ++i) {
        SubscriberEntry* entry = &comm->subscribers[i];
        if (entry->topic[0] != '\0') {
            process_subscriber_entry(comm, entry, false);
            
            // 检查是否需要注销回收
            if (entry->marked_for_unregistration) {
                // 检查发布者数量是否为 0
                size_t remote_pubs = iox2_port_factory_pub_sub_dynamic_config_number_of_publishers(&entry->service);
                if (remote_pubs == 0) {
                    // 真正的释放资源
                    real_drop_subscriber(entry);
                }
            }
        }
    }
}

static void shm_communicator_poll_internal(shm_communicator_t* comm) {
    process_lifecycle_commands(comm);
    shm_communicator_gc_internal(comm);
}

#ifdef __linux__
static void* receiver_thread_func(void* arg) {
    shm_communicator_t* comm = (shm_communicator_t*)arg;
    while (comm->thread_running) {
        if (comm->enable_waitset) {
            iox2_waitset_run_result_e res;
            // 10ms 超时，确保即使没有事件也能周期性执行 GC
            iox2_waitset_wait_and_process_once_with_timeout(&comm->waitset, waitset_on_event, comm, 0, 10000000, &res);
            // 处理指令
            process_lifecycle_commands(comm);
            // 即使在 WaitSet 模式下也要执行 GC 检查
            shm_communicator_gc_internal(comm);
        } else {
            shm_communicator_poll_internal(comm);
        }
    }
    return NULL;
}

bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id) {
    if (!comm || comm->thread_running) return true;
    comm->thread_running = true;
    pthread_attr_t attr; pthread_attr_init(&attr);
    if (cpu_core_id >= 0) {
        cpu_set_t cpuset; CPU_ZERO(&cpuset); CPU_SET(cpu_core_id, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    }
    if (pthread_create(&comm->waitset_thread, &attr, receiver_thread_func, comm) != 0) {
        comm->thread_running = false; pthread_attr_destroy(&attr); return false;
    }
    pthread_attr_destroy(&attr); return true;
}
#endif

bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms) {
    if (!comm) return false;
    SLEEP_MS(timeout_ms); return true;
}

void shm_communicator_poll(shm_communicator_t* comm) {
    if (!comm) return;
    shm_communicator_poll_internal(comm);
}

void shm_communicator_destroy(shm_communicator_t* comm) {
    if (!comm) return;
    if (comm->thread_running) {
        comm->thread_running = false;
        // 反复通知以确保背景线程从 WaitSet 唤醒并看到退出标志
        for (int i = 0; i < 5; ++i) {
            if (comm->dummy_notifier) iox2_notifier_notify(&comm->dummy_notifier, NULL);
            busy_wait_us(1000); // 等待 1ms
        }
        pthread_join(comm->waitset_thread, NULL);
    }

    for (int i = 0; i < MAX_TOPICS; ++i) {
        real_drop_publisher(&comm->publishers[i]);
        real_drop_subscriber(&comm->subscribers[i]);
    }

    if (comm->enable_waitset) {
        if (comm->dummy_notifier) iox2_notifier_drop(comm->dummy_notifier);
        if (comm->dummy_guard) iox2_waitset_guard_drop(comm->dummy_guard);
        if (comm->dummy_listener) iox2_listener_drop(comm->dummy_listener);
        if (comm->dummy_service) iox2_port_factory_event_drop(comm->dummy_service);
        if (comm->dummy_service_name) iox2_service_name_drop(comm->dummy_service_name);
        iox2_waitset_drop(comm->waitset);
    }
    iox2_node_drop(comm->node_handle);
    if (comm->config) iox2_config_drop(comm->config);
    free(comm);
}
