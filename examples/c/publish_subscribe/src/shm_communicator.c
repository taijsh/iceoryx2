// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

// 文件描述: shm_communicator.c
// 该文件实现了共享内存通信器的 C API 封装。

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdatomic.h>
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
#else
#include <stdalign.h>
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

// 发布者封装
typedef struct PublisherEntry {
    char topic[256];
    atomic_bool active;
    atomic_int usage_count;
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

    // 调试统计
    uint64_t debug_total_send_count;
    uint64_t debug_sum_send_ns;
    uint64_t debug_sum_notify_ns;
    uint64_t debug_sum_preloan_ns;
} PublisherEntry;

// 订阅者封装
typedef struct SubscriberEntry {
    char topic[256];
    atomic_bool active;
    atomic_int usage_count;
    iox2_service_name_h service_name;
    iox2_port_factory_pub_sub_h service;
    iox2_subscriber_h subscriber;

    // 事件服务监听器
    iox2_service_name_h event_service_name;
    iox2_port_factory_event_h event_service;
    iox2_listener_h listener;
    
    // WaitSet 守卫句柄
    iox2_waitset_guard_h guard;
} SubscriberEntry;

// 通信器结构体定义
struct shm_communicator_t {
    // 调试统计
    uint64_t debug_total_poll_count;
    uint64_t debug_sum_poll_ns;
    iox2_config_h config;
    iox2_node_h node_handle;
    iox2_waitset_h waitset;
    
    // 保护性虚拟事件，用于防止 WaitSet 为空时报错
    iox2_service_name_h dummy_service_name;
    iox2_port_factory_event_h dummy_service;
    iox2_listener_h dummy_listener;
    iox2_waitset_guard_h dummy_guard;
    
    PublisherEntry publishers[MAX_TOPICS];
    SubscriberEntry subscribers[MAX_TOPICS];
    on_receive_cb user_callback;
    void* user_context;
    bool enable_waitset;
    pthread_mutex_t list_lock; // 仅保护槽位注册/注销时的列表一致性

#ifdef __linux__
    pthread_t waitset_thread;
    volatile bool thread_running;
#endif
};

shm_communicator_t* shm_communicator_create(void) {
    shm_communicator_t* comm = (shm_communicator_t*)calloc(1, sizeof(shm_communicator_t));
    if (comm) {
        pthread_mutex_init(&comm->list_lock, NULL);
    }
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
        printf("成功加载配置文件: %s\n", config_path);
    }

    iox2_node_builder_h node_builder = iox2_node_builder_new(NULL);
    if (comm->config) {
        iox2_node_builder_set_config(&node_builder, &comm->config);
    }
    // 防止 iceoryx2 在 C 层面拦截系统的 SIGINT / SIGTERM 导致我们自己的信号处理失效
    iox2_node_builder_set_signal_handling_mode(&node_builder, iox2_signal_handling_mode_e_DISABLED);

    if (iox2_node_builder_create(node_builder, NULL, iox2_service_type_e_IPC, &comm->node_handle) != IOX2_OK) {
        printf("无法创建节点。\n");
        return false;
    }

    if (comm->enable_waitset) {
        // 创建 WaitSet
        iox2_waitset_builder_h waitset_builder = NULL;
        iox2_waitset_builder_new(NULL, &waitset_builder);
        iox2_waitset_builder_set_signal_handling_mode(&waitset_builder, iox2_signal_handling_mode_e_DISABLED);
        if (iox2_waitset_builder_create(waitset_builder, iox2_service_type_e_IPC, NULL, &comm->waitset) != IOX2_OK) {
            printf("无法创建 WaitSet。\n");
            return false;
        }

        // 绑定一个 Dummy Listener 到 WaitSet，以防止 WaitSet 报出无附件错误而退出
        if (iox2_service_name_new(NULL, "ShmCommunicator_Dummy_Event", strlen("ShmCommunicator_Dummy_Event"), &comm->dummy_service_name) == IOX2_OK) {
            iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(comm->dummy_service_name));
            iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
            if (iox2_service_builder_event_open_or_create(ev_b, NULL, &comm->dummy_service) == IOX2_OK) {
                iox2_port_factory_listener_builder_h list_b = iox2_port_factory_event_listener_builder(&comm->dummy_service, NULL);
                if (iox2_port_factory_listener_builder_create(list_b, NULL, &comm->dummy_listener) == IOX2_OK) {
                    iox2_waitset_attach_notification(&comm->waitset, iox2_listener_get_file_descriptor(&comm->dummy_listener), NULL, &comm->dummy_guard);
                }
            }
        }
    }

    return true;
}

bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic) {
    if (!comm) return false;

    pthread_mutex_lock(&comm->list_lock);
    // 查找空闲槽位
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (!atomic_load(&comm->publishers[i].active)) {
            entry = &comm->publishers[i];
            break;
        }
    }
    
    if (!entry) {
        printf("无法注册发布者 %s: 达到最大主题限制。\n", topic);
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    
    // 1. 创建发布订阅服务
    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    // 2. 创建发布者 (动态内存分配)
    iox2_port_factory_publisher_builder_h pub_builder = iox2_port_factory_pub_sub_publisher_builder(&entry->service, NULL);
    iox2_port_factory_publisher_builder_set_allocation_strategy(&pub_builder, iox2_allocation_strategy_e_POWER_OF_TWO);
    iox2_port_factory_publisher_builder_set_initial_max_slice_len(&pub_builder, 1024);
    if (iox2_port_factory_publisher_builder_create(pub_builder, NULL, &entry->publisher) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    // 3. 创建事件服务和通知器
    char event_topic[512];
    snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
    if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    iox2_port_factory_notifier_builder_h not_builder = iox2_port_factory_event_notifier_builder(&entry->event_service, NULL);
    if (iox2_port_factory_notifier_builder_create(not_builder, NULL, &entry->notifier) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    atomic_store(&entry->usage_count, 0);
    atomic_store(&entry->active, true);
    pthread_mutex_unlock(&comm->list_lock);
    return true;
}

void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    
    pthread_mutex_lock(&comm->list_lock);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->publishers[i].active) && strcmp(comm->publishers[i].topic, topic) == 0) {
            // 关键步骤：先原子标记为失效，不再接受新请求
            atomic_store(&comm->publishers[i].active, false);
            
            // 等待当前正在进行的发送/借用操作完成
            while (atomic_load(&comm->publishers[i].usage_count) > 0) {
                usleep(100); 
            }

            iox2_notifier_drop(comm->publishers[i].notifier);
            iox2_port_factory_event_drop(comm->publishers[i].event_service);
            iox2_service_name_drop(comm->publishers[i].event_service_name);

            iox2_publisher_drop(comm->publishers[i].publisher);
            iox2_port_factory_pub_sub_drop(comm->publishers[i].service);
            iox2_service_name_drop(comm->publishers[i].service_name);
            
            if (comm->publishers[i].pre_loaned_sample) {
                iox2_sample_mut_drop(comm->publishers[i].pre_loaned_sample);
                comm->publishers[i].pre_loaned_sample = NULL;
            }
            
            memset(comm->publishers[i].topic, 0, sizeof(comm->publishers[i].topic));
            break;
        }
    }
    pthread_mutex_unlock(&comm->list_lock);
}

bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic) {
    if (!comm) return false;

    pthread_mutex_lock(&comm->list_lock);
    // 查找空闲槽位
    SubscriberEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (!atomic_load(&comm->subscribers[i].active)) {
            entry = &comm->subscribers[i];
            break;
        }
    }
    if (!entry) {
        printf("无法注册订阅者 %s: 达到最大主题限制。\n", topic);
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);

    // 1. 创建发布订阅服务
    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    // 2. 创建订阅者
    iox2_port_factory_subscriber_builder_h sub_builder = iox2_port_factory_pub_sub_subscriber_builder(&entry->service, NULL);
    if (iox2_port_factory_subscriber_builder_create(sub_builder, NULL, &entry->subscriber) != IOX2_OK) {
        pthread_mutex_unlock(&comm->list_lock);
        return false;
    }

    if (comm->enable_waitset) {
        // 3. 创建事件服务和监听器
        char event_topic[512];
        snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
        if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
            pthread_mutex_unlock(&comm->list_lock);
            return false;
        }
        iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
        iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
        if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
            pthread_mutex_unlock(&comm->list_lock);
            return false;
        }

        iox2_port_factory_listener_builder_h list_b = iox2_port_factory_event_listener_builder(&entry->event_service, NULL);
        if (iox2_port_factory_listener_builder_create(list_b, NULL, &entry->listener) != IOX2_OK) {
            pthread_mutex_unlock(&comm->list_lock);
            return false;
        }

        // 4. 将监听器附加到通信器的 WaitSet
        if (iox2_waitset_attach_notification(&comm->waitset, iox2_listener_get_file_descriptor(&entry->listener), NULL, &entry->guard) != IOX2_OK) {
            pthread_mutex_unlock(&comm->list_lock);
            return false;
        }
    }

    atomic_store(&entry->usage_count, 0);
    atomic_store(&entry->active, true);
    pthread_mutex_unlock(&comm->list_lock);
    return true;
}

void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    
    pthread_mutex_lock(&comm->list_lock);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->subscribers[i].active) && strcmp(comm->subscribers[i].topic, topic) == 0) {
            atomic_store(&comm->subscribers[i].active, false);

            while (atomic_load(&comm->subscribers[i].usage_count) > 0) {
                usleep(100);
            }
            
            if (comm->enable_waitset) {
                iox2_waitset_guard_drop(comm->subscribers[i].guard);
                iox2_listener_drop(comm->subscribers[i].listener);
                iox2_port_factory_event_drop(comm->subscribers[i].event_service);
                iox2_service_name_drop(comm->subscribers[i].event_service_name);
            }

            iox2_subscriber_drop(comm->subscribers[i].subscriber);
            iox2_port_factory_pub_sub_drop(comm->subscribers[i].service);
            iox2_service_name_drop(comm->subscribers[i].service_name);
            
            memset(comm->subscribers[i].topic, 0, sizeof(comm->subscribers[i].topic));
            break;
        }
    }
    pthread_mutex_unlock(&comm->list_lock);
}

bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size) {
    void* sample_h = NULL;
    void* payload = shm_communicator_loan_uninit(comm, topic, size, &sample_h);
    if (!payload) return false;

    // 将外部数据拷贝到借用的共享内存中
    memcpy(payload, data, size);

    // 发送数据 (send 内部目前仍包含自动化预借用以保证兼容性)
    bool success = shm_communicator_send(comm, topic, sample_h);
    
    // 显式触发预借用 (确保表现一致)
    if (success) {
        shm_communicator_ensure_pre_loan(comm, topic);
    }

    return success;
}

void* shm_communicator_loan_uninit(shm_communicator_t* comm, const char* topic, size_t size, void** sample_handle) {
    if (!comm || !sample_handle) return NULL;

    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        // 先检查状态，如果是活跃的且主题匹配，再尝试增加引用计数 (乐观查找)
        if (atomic_load(&comm->publishers[i].active) && strcmp(comm->publishers[i].topic, topic) == 0) {
            atomic_fetch_add(&comm->publishers[i].usage_count, 1);
            
            // 双重检查：确保增加计数后状态依然有效且主题未变
            if (!atomic_load(&comm->publishers[i].active) || strcmp(comm->publishers[i].topic, topic) != 0) {
                atomic_fetch_sub(&comm->publishers[i].usage_count, 1);
                continue;
            }
            entry = &comm->publishers[i];
            break;
        }
    }
    
    if (!entry) return NULL;

    uint8_t* payload = NULL;
    c_size_t num_elements = 0;

    // 检查是否有预借用的样本
    if (entry->pre_loaned_sample != NULL) {
        if (entry->pre_loaned_size < size) {
            iox2_sample_mut_drop(entry->pre_loaned_sample);
            entry->pre_loaned_sample = NULL;
            entry->pre_loaned_size = 0;
        } else {
            iox2_sample_mut_payload_mut(&entry->pre_loaned_sample, (void**) &payload, &num_elements);
            *sample_handle = (void*)entry->pre_loaned_sample;
            entry->pre_loaned_sample = NULL; 
            entry->pre_loaned_size = 0;
            
            // 操作完成，释放引用计数 (注意：由于 sample_handle 已返回，
            // 稍后的 send 调用会再次持有引用，这里可以安全释放)
            atomic_fetch_sub(&entry->usage_count, 1);
            return (void*)payload;
        }
    }

    if (size > entry->max_loaned_size) {
        entry->max_loaned_size = size;
    }

    iox2_sample_mut_h sample = NULL;
    if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &sample, size) != IOX2_OK) {
        atomic_fetch_sub(&entry->usage_count, 1);
        return NULL;
    }

    iox2_sample_mut_payload_mut(&sample, (void**) &payload, &num_elements);
    *sample_handle = (void*)sample;

    atomic_fetch_sub(&entry->usage_count, 1);
    return (void*)payload;
}

bool shm_communicator_send(shm_communicator_t* comm, const char* topic, void* sample_handle) {
    if (!comm || !sample_handle) return false;

    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->publishers[i].active) && strcmp(comm->publishers[i].topic, topic) == 0) {
            atomic_fetch_add(&comm->publishers[i].usage_count, 1);
            if (!atomic_load(&comm->publishers[i].active) || strcmp(comm->publishers[i].topic, topic) != 0) {
                atomic_fetch_sub(&comm->publishers[i].usage_count, 1);
                continue;
            }
            entry = &comm->publishers[i];
            break;
        }
    }
    if (!entry) return false;

    iox2_sample_mut_h sample = (iox2_sample_mut_h)sample_handle;
    
    struct timespec ts1, ts2, ts3, ts4;
    clock_gettime(CLOCK_MONOTONIC, &ts1);
    
    if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
        atomic_fetch_sub(&entry->usage_count, 1);
        return false;
    }
    clock_gettime(CLOCK_MONOTONIC, &ts2);

    // 发送事件通知
    iox2_notifier_notify(&entry->notifier, NULL);
    clock_gettime(CLOCK_MONOTONIC, &ts3);

    // 备注：已移除这里的自动预借用逻辑，改由调用方通过显式接口触发，
    // 以保证 send 函数的时延统计不受分配逻辑干扰。
    ts4 = ts3; // 保持 ts4 有效以防后续统计代码使用

    entry->debug_total_send_count++;
    entry->debug_sum_send_ns += (ts2.tv_sec - ts1.tv_sec) * 1000000000ULL + (ts2.tv_nsec - ts1.tv_nsec);
    entry->debug_sum_notify_ns += (ts3.tv_sec - ts2.tv_sec) * 1000000000ULL + (ts3.tv_nsec - ts2.tv_nsec);
    entry->debug_sum_preloan_ns += (ts4.tv_sec - ts3.tv_sec) * 1000000000ULL + (ts4.tv_nsec - ts3.tv_nsec);

    if (entry->debug_total_send_count % 1000 == 0) {
        printf("[DEBUG] Topic:%s, SendAvg:%.2f us, NotifyAvg:%.2f us, PreloanAvg:%.2f us\n",
               entry->topic,
               (double)entry->debug_sum_send_ns / entry->debug_total_send_count / 1000.0,
               (double)entry->debug_sum_notify_ns / entry->debug_total_send_count / 1000.0,
               (double)entry->debug_sum_preloan_ns / entry->debug_total_send_count / 1000.0);
        entry->debug_sum_send_ns = 0;
        entry->debug_sum_notify_ns = 0;
        entry->debug_sum_preloan_ns = 0;
        entry->debug_total_send_count = 0;
    }
    
    atomic_fetch_sub(&entry->usage_count, 1);
    return true;
}

static bool shm_communicator_ensure_pre_loan_internal(PublisherEntry* entry) {
    if (entry->pre_loaned_sample == NULL && entry->max_loaned_size > 0) {
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
        if (atomic_load(&comm->publishers[i].active) && strcmp(comm->publishers[i].topic, topic) == 0) {
            atomic_fetch_add(&comm->publishers[i].usage_count, 1);
            if (!atomic_load(&comm->publishers[i].active) || strcmp(comm->publishers[i].topic, topic) != 0) {
                atomic_fetch_sub(&comm->publishers[i].usage_count, 1);
                continue;
            }
            entry = &comm->publishers[i];
            break;
        }
    }
    if (!entry) return false;

    bool result = shm_communicator_ensure_pre_loan_internal(entry);
    atomic_fetch_sub(&entry->usage_count, 1);
    return result;
}

// 内部处理某个特定连接的接收逻辑
static void process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry, bool clear_events) {
    if (clear_events && entry->listener) {
        iox2_event_id_t event_id;
        bool has_received_event = false;

        // printf("[DEBUG] SHM: process_subscriber_entry start\n");
        // 提取所有 Listener 中触发的事件，防止死锁累积
        do {
            has_received_event = false;
            if (iox2_listener_try_wait_one(&entry->listener, &event_id, &has_received_event) != IOX2_OK) {
                printf("[DEBUG] SHM: try_wait_one loop broke via ERROR\n");
                break;
            }
        } while (has_received_event);
    }

    // 收取该主题的样本流并在回调中上抛
    bool has_samples = false;
    do {
        has_samples = false;
        iox2_sample_h sample = NULL;
        if (iox2_subscriber_receive(&entry->subscriber, NULL, &sample) == IOX2_OK && sample != NULL) {
            has_samples = true;
            const uint8_t* payload = NULL;
            c_size_t num_elements = 0;
            iox2_sample_payload(&sample, (const void**) &payload, &num_elements);

            if (comm->user_callback) {
                comm->user_callback(entry->topic, payload, num_elements, comm->user_context);
            }

            iox2_sample_drop(sample);
        }
    } while (has_samples);
    // atomic_fetch_sub(&entry->usage_count, 1); // 已移除，由调用者负责释放
}

// 事件的回调函数指针
static iox2_callback_progression_e waitset_on_event(iox2_waitset_attachment_id_h attachment_id, void* context) {
    shm_communicator_t* comm = (shm_communicator_t*)context;
    bool has_handled = false;
    
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->subscribers[i].active)) {
            // 只有在 ID 匹配时才标记为正在使用
            if (iox2_waitset_attachment_id_has_event_from(&attachment_id, &comm->subscribers[i].guard)) {
                atomic_fetch_add(&comm->subscribers[i].usage_count, 1);
                // 再次检查状态是否依然有效
                if (atomic_load(&comm->subscribers[i].active)) {
                    process_subscriber_entry(comm, &comm->subscribers[i], true);
                    has_handled = true;
                }
                atomic_fetch_sub(&comm->subscribers[i].usage_count, 1);
            }
        }
    }

    // 清理 Dummy Event
    if (!has_handled && comm->dummy_guard && iox2_waitset_attachment_id_has_event_from(&attachment_id, &comm->dummy_guard)) {
        iox2_event_id_t dummy_event_id;
        bool dummy_has_received = false;
        do {
            dummy_has_received = false;
            if (iox2_listener_try_wait_one(&comm->dummy_listener, &dummy_event_id, &dummy_has_received) != IOX2_OK) {
                break;
            }
        } while (dummy_has_received);
    }
    
    iox2_waitset_attachment_id_drop(attachment_id);
    return iox2_callback_progression_e_CONTINUE;
}

// 同步轮询所有活跃的订阅者并触发回调 (Busy Polling)
static void shm_communicator_poll_internal(shm_communicator_t* comm) {
    struct timespec ts_start, ts_end;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->subscribers[i].active)) {
            atomic_fetch_add(&comm->subscribers[i].usage_count, 1);
            if (atomic_load(&comm->subscribers[i].active)) {
                process_subscriber_entry(comm, &comm->subscribers[i], false);
            }
            atomic_fetch_sub(&comm->subscribers[i].usage_count, 1);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    comm->debug_total_poll_count++;
    comm->debug_sum_poll_ns += (ts_end.tv_sec - ts_start.tv_sec) * 1000000000ULL + (ts_end.tv_nsec - ts_start.tv_nsec);

    if (comm->debug_total_poll_count % 10000 == 0) {
        // printf("[DEBUG] Poll Cycle Avg: %.2f us\n", (double)comm->debug_sum_poll_ns / comm->debug_total_poll_count / 1000.0);
        comm->debug_sum_poll_ns = 0;
        comm->debug_total_poll_count = 0;
    }
}

#ifdef __linux__
static void* receiver_thread_func(void* arg) {
    shm_communicator_t* comm = (shm_communicator_t*)arg;
    printf("[DEBUG] SHM: 后台接收线程已启动, 模式: %s\n", 
           comm->enable_waitset ? "WaitSet (事件驱动)" : "Polling (忙轮询)");

    while (comm->thread_running) {
        if (comm->enable_waitset) {
            iox2_waitset_run_result_e result = iox2_waitset_run_result_e_STOP_REQUEST;
            // 等待并处理事件，超时时间 10ms 以便检查 thread_running 标志
            iox2_waitset_wait_and_process_once_with_timeout(&comm->waitset, waitset_on_event, comm, 0, 10000000, &result);
        } else {
            // 忙轮询模式：尽可能快地检查所有主题
            shm_communicator_poll_internal(comm);
            // 为了防止 CPU 占用过高导致无法退出机，可以极短地放弃时间片
            // sched_yield(); 
        }
    }
    printf("[DEBUG] SHM: 后台接收线程已退出。\n");
    return NULL;
}

bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id) {
    if (!comm) return false;
    
    if (comm->thread_running) {
        return true; 
    }
    comm->thread_running = true;

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    if (cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_core_id, &cpuset);
        
        if (pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset) != 0) {
            printf("警告: 无法为通信器接收线程绑定 CPU 核心 %d\n", cpu_core_id);
        } else {
            printf("成功为通信器接收线程绑定 CPU 核心 %d\n", cpu_core_id);
        }
    }

    if (pthread_create(&comm->waitset_thread, &attr, receiver_thread_func, comm) != 0) {
        printf("错误: 无法创建后台接收线程。\n");
        comm->thread_running = false;
        pthread_attr_destroy(&attr);
        return false;
    }

    pthread_attr_destroy(&attr);
    return true;
}
#endif

bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms) {
    if (!comm) return false;
    // In background thread mode, this serves as a controlled sleep for the app loop.
#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
    Sleep((DWORD)timeout_ms);
#else
    usleep((useconds_t)timeout_ms * 1000);
#endif
    return true;
}

void shm_communicator_poll(shm_communicator_t* comm) {
    if (!comm) return;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (atomic_load(&comm->subscribers[i].active)) {
            atomic_fetch_add(&comm->subscribers[i].usage_count, 1);
            if (atomic_load(&comm->subscribers[i].active)) {
                process_subscriber_entry(comm, &comm->subscribers[i], false);
            }
            atomic_fetch_sub(&comm->subscribers[i].usage_count, 1);
        }
    }
}

void shm_communicator_destroy(shm_communicator_t* comm) {
    if (!comm) return;

    printf("[DEBUG] SHM: shm_communicator_destroy Called.\n");
#ifdef __linux__
    if (comm->thread_running) {
        comm->thread_running = false;
        
        // 我们已经切换到 wait_and_process_once_with_timeout，因此线程最多阻塞 10ms 即可自己退出循环。
        // 无需发送强行唤醒通知以及 pthread_cancel。
        printf("[DEBUG] SHM: Awaiting pthread_join...\n");
        pthread_join(comm->waitset_thread, NULL);
        printf("[DEBUG] SHM: pthread_join finished.\n");
    }
#endif

    printf("[DEBUG] SHM: Cleaning up topic resources...\n");

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].active) {
            iox2_notifier_drop(comm->publishers[i].notifier);
            iox2_port_factory_event_drop(comm->publishers[i].event_service);
            iox2_service_name_drop(comm->publishers[i].event_service_name);

            iox2_publisher_drop(comm->publishers[i].publisher);
            iox2_port_factory_pub_sub_drop(comm->publishers[i].service);
            iox2_service_name_drop(comm->publishers[i].service_name);
            
            // 释放预借用的样本
            if (comm->publishers[i].pre_loaned_sample) {
                iox2_sample_mut_drop(comm->publishers[i].pre_loaned_sample);
            }
        }

        if (comm->subscribers[i].active) {
            if (comm->enable_waitset) {
                iox2_waitset_guard_drop(comm->subscribers[i].guard);
                iox2_listener_drop(comm->subscribers[i].listener);
                iox2_port_factory_event_drop(comm->subscribers[i].event_service);
                iox2_service_name_drop(comm->subscribers[i].event_service_name);
            }

            iox2_subscriber_drop(comm->subscribers[i].subscriber);
            iox2_port_factory_pub_sub_drop(comm->subscribers[i].service);
            iox2_service_name_drop(comm->subscribers[i].service_name);
        }
    }

    if (comm->enable_waitset) {
        if (comm->dummy_guard) {
            iox2_waitset_guard_drop(comm->dummy_guard);
            iox2_listener_drop(comm->dummy_listener);
            iox2_port_factory_event_drop(comm->dummy_service);
            iox2_service_name_drop(comm->dummy_service_name);
        }

        iox2_waitset_drop(comm->waitset);
    }
    iox2_node_drop(comm->node_handle);
    if (comm->config) {
        iox2_config_drop(comm->config);
    }

    pthread_mutex_unlock(&comm->list_lock);
    pthread_mutex_destroy(&comm->list_lock);
    free(comm);
}
