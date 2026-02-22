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

#define MAX_TOPICS 32

// 发布者封装
typedef struct {
    char topic[256];
    bool active;
    iox2_service_name_h service_name;
    iox2_port_factory_pub_sub_h service;
    iox2_publisher_h publisher;

    // 事件服务通知器 (用于通知WaitSet)
    iox2_service_name_h event_service_name;
    iox2_port_factory_event_h event_service;
    iox2_notifier_h notifier;
} PublisherEntry;

// 订阅者封装
typedef struct {
    char topic[256];
    bool active;
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

#ifdef __linux__
    pthread_t waitset_thread;
    volatile bool thread_running;
#endif
};

shm_communicator_t* shm_communicator_create(void) {
    shm_communicator_t* comm = (shm_communicator_t*)calloc(1, sizeof(shm_communicator_t));
    return comm;
}

bool shm_communicator_init(shm_communicator_t* comm,
                           const char* config_path,
                           on_receive_cb callback,
                           void* user_context) {
    if (!comm) return false;

    comm->user_callback = callback;
    comm->user_context = user_context;

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

    return true;
}

bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic) {
    if (!comm) return false;

    // 查找空闲槽位
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (!comm->publishers[i].active) {
            entry = &comm->publishers[i];
            break;
        }
    }
    
    if (!entry) {
        printf("无法注册发布者 %s: 达到最大主题限制。\n", topic);
        return false;
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    
    // 1. 创建发布订阅服务
    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) {
        return false;
    }
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        return false;
    }

    // 2. 创建发布者 (动态内存分配)
    iox2_port_factory_publisher_builder_h pub_builder = iox2_port_factory_pub_sub_publisher_builder(&entry->service, NULL);
    iox2_port_factory_publisher_builder_set_allocation_strategy(&pub_builder, iox2_allocation_strategy_e_POWER_OF_TWO);
    iox2_port_factory_publisher_builder_set_initial_max_slice_len(&pub_builder, 1024);
    if (iox2_port_factory_publisher_builder_create(pub_builder, NULL, &entry->publisher) != IOX2_OK) {
        return false;
    }

    // 3. 创建事件服务和通知器
    char event_topic[512];
    snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
    if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
        return false;
    }

    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
        return false;
    }

    iox2_port_factory_notifier_builder_h not_builder = iox2_port_factory_event_notifier_builder(&entry->event_service, NULL);
    if (iox2_port_factory_notifier_builder_create(not_builder, NULL, &entry->notifier) != IOX2_OK) {
        return false;
    }

    entry->active = true;
    return true;
}

bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic) {
    if (!comm) return false;

    // 查找空闲槽位
    SubscriberEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (!comm->subscribers[i].active) {
            entry = &comm->subscribers[i];
            break;
        }
    }
    if (!entry) {
        printf("无法注册订阅者 %s: 达到最大主题限制。\n", topic);
        return false;
    }

    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);

    // 1. 创建发布订阅服务
    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) {
        return false;
    }
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t)) != IOX2_OK) {
        return false;
    }
    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        return false;
    }

    // 2. 创建订阅者
    iox2_port_factory_subscriber_builder_h sub_builder = iox2_port_factory_pub_sub_subscriber_builder(&entry->service, NULL);
    if (iox2_port_factory_subscriber_builder_create(sub_builder, NULL, &entry->subscriber) != IOX2_OK) {
        return false;
    }

    // 3. 创建事件服务和监听器
    char event_topic[512];
    snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
    if (iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name) != IOX2_OK) {
        return false;
    }
    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    if (iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service) != IOX2_OK) {
        return false;
    }

    iox2_port_factory_listener_builder_h list_b = iox2_port_factory_event_listener_builder(&entry->event_service, NULL);
    if (iox2_port_factory_listener_builder_create(list_b, NULL, &entry->listener) != IOX2_OK) {
        return false;
    }

    // 4. 将监听器附加到通信器的 WaitSet
    if (iox2_waitset_attach_notification(&comm->waitset, iox2_listener_get_file_descriptor(&entry->listener), NULL, &entry->guard) != IOX2_OK) {
        return false;
    }

    entry->active = true;
    return true;
}

bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size) {
    if (!comm) return false;

    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].active && strcmp(comm->publishers[i].topic, topic) == 0) {
            entry = &comm->publishers[i];
            break;
        }
    }
    
    if (!entry) return false;

    // 借用样本
    iox2_sample_mut_h sample = NULL;
    // printf("[DEBUG] SHM: Loaning payload... size %zu\n", size);
    if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &sample, size) != IOX2_OK) {
        printf("[DEBUG] SHM: publisher_loan_slice_uninit failed\n");
        return false; // 通信不畅
    }

    uint8_t* payload = NULL;
    c_size_t num_elements = 0;
    iox2_sample_mut_payload_mut(&sample, (void**) &payload, &num_elements);
    
    memcpy(payload, data, size);

    // printf("[DEBUG] SHM: Sending sample...\n");
    if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
        printf("[DEBUG] SHM: sample_mut_send failed\n");
        return false;
    }

    // 发送 WaitSet 事件通知订阅者
    // printf("[DEBUG] SHM: Calling notifier_notify...\n");
    iox2_notifier_notify(&entry->notifier, NULL);

    return true;
}

// 内部处理某个特定连接的接收逻辑
static void process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry) {
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
    // printf("[DEBUG] SHM: process_subscriber_entry exit\n");
}

// 事件的回调函数指针
static iox2_callback_progression_e waitset_on_event(iox2_waitset_attachment_id_h attachment_id, void* context) {
    shm_communicator_t* comm = (shm_communicator_t*)context;
    bool has_handled = false;
    
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].active) {
            // 通过守卫来判断此次事件的来源归属
            if (iox2_waitset_attachment_id_has_event_from(&attachment_id, &comm->subscribers[i].guard)) {
                process_subscriber_entry(comm, &comm->subscribers[i]);
                has_handled = true;
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

#ifdef __linux__
static void* waitset_thread_func(void* arg) {
    shm_communicator_t* comm = (shm_communicator_t*)arg;
    
    while (comm->thread_running) {
        iox2_waitset_run_result_e result = iox2_waitset_run_result_e_STOP_REQUEST;
        // 使用 once_with_timeout，配合 10ms 的超时时间，既能实现微秒级唤醒，
        // 又能让线程及时醒来检查 comm->thread_running 标志位真正退出。
        int stat = iox2_waitset_wait_and_process_once_with_timeout(&comm->waitset, waitset_on_event, comm, 0, 10000000, &result);
        if (stat != IOX2_OK) {
            // ALL_EVENTS_HANDLED 等不一定是错误，仅当出现严重不匹配才报错
        }
    }
    return NULL;
}

bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id) {
    if (!comm) return false;
    
    if (comm->thread_running) {
        return true; // 已经运行
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

    if (pthread_create(&comm->waitset_thread, &attr, waitset_thread_func, comm) != 0) {
        printf("错误: 无法创建后台 WaitSet 接收线程。\n");
        comm->thread_running = false;
        pthread_attr_destroy(&attr);
        return false;
    }

    pthread_attr_destroy(&attr);
    return true;
}
#else
bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id) {
    printf("平台不支持，后台接收线程目前仅支持 Linux 平台。\n");
    return false;
}
#endif

bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms) {
    if (!comm) return false;

    // 因为后台线程已经接管了真实的 WaitSet / Listener 轮询，
    // 这里的主线程相当于只起到“等频发包”挂起的作用。
    // 如果没有 C 定时器模块，最简单的实现是直接调用系统级 Sleep 挂起主线程。
    
#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
    Sleep(timeout_ms);
#else
    usleep(timeout_ms * 1000);
#endif

    return true;
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
        }

        if (comm->subscribers[i].active) {
            iox2_waitset_guard_drop(comm->subscribers[i].guard);
            iox2_listener_drop(comm->subscribers[i].listener);
            iox2_port_factory_event_drop(comm->subscribers[i].event_service);
            iox2_service_name_drop(comm->subscribers[i].event_service_name);

            iox2_subscriber_drop(comm->subscribers[i].subscriber);
            iox2_port_factory_pub_sub_drop(comm->subscribers[i].service);
            iox2_service_name_drop(comm->subscribers[i].service_name);
        }
    }

    if (comm->dummy_guard) {
        iox2_waitset_guard_drop(comm->dummy_guard);
        iox2_listener_drop(comm->dummy_listener);
        iox2_port_factory_event_drop(comm->dummy_service);
        iox2_service_name_drop(comm->dummy_service_name);
    }

    iox2_waitset_drop(comm->waitset);
    iox2_node_drop(comm->node_handle);
    if (comm->config) {
        iox2_config_drop(comm->config);
    }

    free(comm);
}
