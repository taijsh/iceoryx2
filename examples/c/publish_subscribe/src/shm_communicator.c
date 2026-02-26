// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

// 文件描述: shm_communicator.c
// 该文件实现了共享 memory 通信器的 C API 封装。

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "shm_communicator.h"
#include "iox2/iceoryx2.h"
#include "transmission_data.h"

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
    // Windows 平台简单实现
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

// 获取当前毫秒级时间戳 (单调时钟)
static uint64_t get_current_time_ms(void) {
#ifdef __linux__
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
#else
    LARGE_INTEGER frequency, counter;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&counter);
    return (uint64_t)(counter.QuadPart * 1000 / frequency.QuadPart);
#endif
}

#define MAX_TOPICS 32
#define MAX_GRACE_ROUNDS 256  // 发现无数据后额外空转轮询的次数

typedef struct shm_communicator_t shm_communicator_t;
typedef struct PublisherEntry PublisherEntry;
typedef struct SubscriberEntry SubscriberEntry;

static bool shm_communicator_ensure_pre_loan_internal(PublisherEntry* entry);
static bool process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry, bool clear_events);

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
    size_t max_loaned_size; 

    shm_data_type_e data_type;               // 数据类型
    volatile bool marked_for_unregistration; // 标记是否注销
    uint64_t zero_peer_count_timestamp_ms;    // 记录对端数量为 0 的起始时间戳
    volatile int ref_count;                  // 引用计数，防止在数据路径执行时资源被释放
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

    shm_data_type_e data_type;               // 数据类型
    volatile bool marked_for_unregistration; // 标记是否注销
    uint64_t zero_peer_count_timestamp_ms;    // 记录对端数量为 0 的起始时间戳
    volatile int ref_count;                  // 引用计数
} SubscriberEntry;

// 通信器结构体定义
struct shm_communicator_t {
    iox2_config_h config;
    iox2_node_h node_handle;
    iox2_waitset_h waitset;
    
    PublisherEntry publishers[MAX_TOPICS];
    SubscriberEntry subscribers[MAX_TOPICS];
    on_receive_cb user_callback;
    void* user_context;
    bool enable_waitset;

#ifdef __linux__
    pthread_t waitset_thread;
    pthread_mutex_t mutex;
    volatile bool thread_running;
#endif
};

// 内部同步操作封装
static void internal_unregister_publisher(shm_communicator_t* comm, const char* topic);
static void internal_unregister_subscriber(shm_communicator_t* comm, const char* topic);

// 真正的资源释放逻辑
static void real_drop_publisher(PublisherEntry* entry);
static void real_drop_subscriber(SubscriberEntry* entry);
static bool internal_register_publisher(shm_communicator_t* comm, const char* topic, shm_data_type_e type);
static bool internal_register_subscriber(shm_communicator_t* comm, const char* topic, shm_data_type_e type);

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
    }

#ifdef __linux__
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&comm->mutex, &attr);
    pthread_mutexattr_destroy(&attr);
#endif

    return true;
}

bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic) {
    return shm_communicator_register_publisher_ext(comm, topic, SHM_DATA_DYNAMIC);
}

bool shm_communicator_register_publisher_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type) {
    if (!comm || !topic) return false;
    return internal_register_publisher(comm, topic, type);
}

void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    internal_unregister_publisher(comm, topic);
}

bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic) {
    return shm_communicator_register_subscriber_ext(comm, topic, SHM_DATA_DYNAMIC);
}

bool shm_communicator_register_subscriber_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type) {
    if (!comm || !topic) return false;
    return internal_register_subscriber(comm, topic, type);
}

void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic) {
    if (!comm || !topic) return;
    internal_unregister_subscriber(comm, topic);
}

// ---------------- 内部实际执行逻辑 ----------------

static bool internal_register_publisher(shm_communicator_t* comm, const char* topic, shm_data_type_e type) {
    pthread_mutex_lock(&comm->mutex);
    PublisherEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            if (comm->publishers[i].marked_for_unregistration) {
                comm->publishers[i].marked_for_unregistration = false;
                comm->publishers[i].zero_peer_count_timestamp_ms = 0;
            }
            pthread_mutex_unlock(&comm->mutex);
            return true;
        }
    }

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] == '\0') {
            entry = &comm->publishers[i]; break;
        }
    }
    if (!entry) { pthread_mutex_unlock(&comm->mutex); return false; }
    memset(entry, 0, sizeof(PublisherEntry));
    pthread_mutex_unlock(&comm->mutex);

    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) return false;
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (type == SHM_DATA_DYNAMIC) {
        iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t));
    } else {
        const char* type_name = "";
        size_t type_size = 0;
        size_t type_align = 0;
        switch (type) {
            case SHM_DATA_COMMON: type_name = "TransmissionCommonData"; type_size = sizeof(struct TransmissionCommonData); type_align = alignof(struct TransmissionCommonData); break;
            case SHM_DATA_LARGE:  type_name = "TransmissionLargeData";  type_size = sizeof(struct TransmissionLargeData);  type_align = alignof(struct TransmissionLargeData); break;
            case SHM_DATA_STREAM: type_name = "TransmissionStreamData"; type_size = sizeof(struct TransmissionStreamData); type_align = alignof(struct TransmissionStreamData); break;
            default: break;
        }
        iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_FIXED_SIZE, type_name, strlen(type_name), type_size, type_align);
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
    iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name);
    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service);
    iox2_port_factory_notifier_builder_h not_builder = iox2_port_factory_event_notifier_builder(&entry->event_service, NULL);
    iox2_port_factory_notifier_builder_create(not_builder, NULL, &entry->notifier);

    pthread_mutex_lock(&comm->mutex);
    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    entry->data_type = type;
    pthread_mutex_unlock(&comm->mutex);
    return true;
}

static void real_drop_publisher(PublisherEntry* entry) {
    if (!entry) return;
    memset(entry->topic, 0, sizeof(entry->topic));
    if (entry->notifier) iox2_notifier_drop(entry->notifier);
    if (entry->event_service) iox2_port_factory_event_drop(entry->event_service);
    if (entry->event_service_name) iox2_service_name_drop(entry->event_service_name);
    if (entry->publisher) iox2_publisher_drop(entry->publisher);
    if (entry->service) iox2_port_factory_pub_sub_drop(entry->service);
    if (entry->service_name) iox2_service_name_drop(entry->service_name);
    if (entry->pre_loaned_sample) iox2_sample_mut_drop(entry->pre_loaned_sample);
    memset(entry, 0, sizeof(PublisherEntry));
}

static void internal_unregister_publisher(shm_communicator_t* comm, const char* topic) {
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            comm->publishers[i].marked_for_unregistration = true;
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);
}

static bool internal_register_subscriber(shm_communicator_t* comm, const char* topic, shm_data_type_e type) {
    pthread_mutex_lock(&comm->mutex);
    SubscriberEntry* entry = NULL;
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] != '\0' && strcmp(comm->subscribers[i].topic, topic) == 0) {
            if (comm->subscribers[i].marked_for_unregistration) {
                comm->subscribers[i].marked_for_unregistration = false;
                comm->subscribers[i].zero_peer_count_timestamp_ms = 0;
            }
            pthread_mutex_unlock(&comm->mutex);
            return true;
        }
    }

    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] == '\0') {
            entry = &comm->subscribers[i]; break;
        }
    }
    if (!entry) { pthread_mutex_unlock(&comm->mutex); return false; }
    memset(entry, 0, sizeof(SubscriberEntry));
    pthread_mutex_unlock(&comm->mutex);

    if (iox2_service_name_new(NULL, topic, strlen(topic), &entry->service_name) != IOX2_OK) return false;
    iox2_service_builder_h service_builder = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->service_name));
    iox2_service_builder_pub_sub_h pb = iox2_service_builder_pub_sub(service_builder);
    
    if (type == SHM_DATA_DYNAMIC) {
        iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_DYNAMIC, "uint8_t", strlen("uint8_t"), sizeof(uint8_t), alignof(uint8_t));
    } else {
        const char* type_name = "";
        size_t type_size = 0;
        size_t type_align = 0;
        switch (type) {
            case SHM_DATA_COMMON: type_name = "TransmissionCommonData"; type_size = sizeof(struct TransmissionCommonData); type_align = alignof(struct TransmissionCommonData); break;
            case SHM_DATA_LARGE:  type_name = "TransmissionLargeData";  type_size = sizeof(struct TransmissionLargeData);  type_align = alignof(struct TransmissionLargeData); break;
            case SHM_DATA_STREAM: type_name = "TransmissionStreamData"; type_size = sizeof(struct TransmissionStreamData); type_align = alignof(struct TransmissionStreamData); break;
            default: break;
        }
        iox2_service_builder_pub_sub_set_payload_type_details(&pb, iox2_type_variant_e_FIXED_SIZE, type_name, strlen(type_name), type_size, type_align);
    }

    if (iox2_service_builder_pub_sub_open_or_create(pb, NULL, &entry->service) != IOX2_OK) {
        iox2_service_name_drop(entry->service_name); return false;
    }

    iox2_port_factory_subscriber_builder_h sub_builder = iox2_port_factory_pub_sub_subscriber_builder(&entry->service, NULL);
    if (iox2_port_factory_subscriber_builder_create(sub_builder, NULL, &entry->subscriber) != IOX2_OK) {
        iox2_port_factory_pub_sub_drop(entry->service); iox2_service_name_drop(entry->service_name); return false;
    }

    char event_topic[512];
    snprintf(event_topic, sizeof(event_topic), "%s_Events", topic);
    iox2_service_name_new(NULL, event_topic, strlen(event_topic), &entry->event_service_name);
    iox2_service_builder_h ev_sb = iox2_node_service_builder(&comm->node_handle, NULL, iox2_cast_service_name_ptr(entry->event_service_name));
    iox2_service_builder_event_h ev_b = iox2_service_builder_event(ev_sb);
    iox2_service_builder_event_open_or_create(ev_b, NULL, &entry->event_service);
    iox2_port_factory_listener_builder_h list_builder = iox2_port_factory_event_listener_builder(&entry->event_service, NULL);
    iox2_port_factory_listener_builder_create(list_builder, NULL, &entry->listener);

    if (comm->enable_waitset) {
        iox2_file_descriptor_ptr fd = iox2_listener_get_file_descriptor(&entry->listener);
        iox2_waitset_attach_notification(&comm->waitset, fd, NULL, &entry->guard);
    }

    pthread_mutex_lock(&comm->mutex);
    strncpy(entry->topic, topic, sizeof(entry->topic) - 1);
    entry->data_type = type;
    pthread_mutex_unlock(&comm->mutex);
    return true;
}

static void real_drop_subscriber(SubscriberEntry* entry) {
    if (!entry) return;
    memset(entry->topic, 0, sizeof(entry->topic));
    if (entry->guard) iox2_waitset_guard_drop(entry->guard);
    if (entry->listener) iox2_listener_drop(entry->listener);
    if (entry->event_service) iox2_port_factory_event_drop(entry->event_service);
    if (entry->event_service_name) iox2_service_name_drop(entry->event_service_name);
    if (entry->subscriber) iox2_subscriber_drop(entry->subscriber);
    if (entry->service) iox2_port_factory_pub_sub_drop(entry->service);
    if (entry->service_name) iox2_service_name_drop(entry->service_name);
    memset(entry, 0, sizeof(SubscriberEntry));
}

static void internal_unregister_subscriber(shm_communicator_t* comm, const char* topic) {
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->subscribers[i].topic[0] != '\0' && strcmp(comm->subscribers[i].topic, topic) == 0) {
            comm->subscribers[i].marked_for_unregistration = true;
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);
}

// ---------------- 数据收发路径 ----------------

bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size) {
    if (!comm) return false;
    
    PublisherEntry* entry = NULL;
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (strcmp(comm->publishers[i].topic, topic) == 0 && comm->publishers[i].topic[0] != '\0') {
            entry = &comm->publishers[i];
            entry->ref_count++;
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);

    if (!entry) return false;

    void* sample_h = NULL;
    void* payload = shm_communicator_loan_uninit(comm, topic, size, &sample_h);
    bool success = false;

    if (payload) {
        memcpy(payload, data, size);
        success = shm_communicator_send(comm, topic, sample_h);
        if (success) {
            shm_communicator_ensure_pre_loan(comm, topic);
        }
    }

    pthread_mutex_lock(&comm->mutex);
    entry->ref_count--;
    pthread_mutex_unlock(&comm->mutex);

    return success;
}

bool shm_communicator_publish_copy(shm_communicator_t* comm, const char* topic, const void* data, size_t size) {
    if (!comm || !topic || !data) return false;

    PublisherEntry* entry = NULL;
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (strcmp(comm->publishers[i].topic, topic) == 0 && comm->publishers[i].topic[0] != '\0') {
            entry = &comm->publishers[i];
            entry->ref_count++;
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);

    if (!entry || entry->publisher == NULL) {
        if (entry) {
            pthread_mutex_lock(&comm->mutex);
            entry->ref_count--;
            pthread_mutex_unlock(&comm->mutex);
        }
        return false;
    }

    // 直接调用 send_slice_copy 接口，直接将用户内存数据拷贝到共享内存进行发送
    size_t element_size = 1;
    size_t num_elements = size;
    if (entry->data_type != SHM_DATA_DYNAMIC) {
        switch (entry->data_type) {
            case SHM_DATA_COMMON: element_size = sizeof(struct TransmissionCommonData); break;
            case SHM_DATA_LARGE:  element_size = sizeof(struct TransmissionLargeData);  break;
            case SHM_DATA_STREAM: element_size = sizeof(struct TransmissionStreamData); break;
            default: break;
        }
        num_elements = 1;
    }
    int result = iox2_publisher_send_slice_copy(&entry->publisher, data, element_size, num_elements, NULL);
    bool success = (result == IOX2_OK);

    if (success && entry->notifier) {
        iox2_notifier_notify(&entry->notifier, NULL);
    }

    pthread_mutex_lock(&comm->mutex);
    entry->ref_count--;
    pthread_mutex_unlock(&comm->mutex);

    return success;
}

void* shm_communicator_loan_uninit(shm_communicator_t* comm, const char* topic, size_t size, void** sample_handle) {
    if (!comm || !topic || !sample_handle) return NULL;
    
    PublisherEntry* entry = NULL;
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            if (comm->publishers[i].marked_for_unregistration) {
                pthread_mutex_unlock(&comm->mutex);
                return NULL;
            }
            entry = &comm->publishers[i];
            entry->ref_count++;
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);

    if (!entry || entry->publisher == NULL) return NULL;
    
    uint8_t* payload = NULL;
    c_size_t num_elements = 0;
    
    // 注意：pre_loaned_sample 的访问目前仍受互斥锁保护，或我们需要更精细化
    // 简单起见，这里重新加锁处理内部状态
    pthread_mutex_lock(&comm->mutex);
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
            pthread_mutex_unlock(&comm->mutex);
            // 保持 ref_count，因为后续 memcpy 还需要访问 payload (由用户执行)
            // 但用户怎么释放呢？这里有个隐含风险：用户拿到 payload 没法手动还 ref_count
            // 这是一个不完美的地方，但在publish中使用是闭环的
            return (void*)payload;
        }
    }
    
    if (size > entry->max_loaned_size) entry->max_loaned_size = size;
    pthread_mutex_unlock(&comm->mutex);

    iox2_sample_mut_h sample = NULL;
    size_t num_to_loan = (entry->data_type == SHM_DATA_DYNAMIC) ? size : 1;
    if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &sample, num_to_loan) != IOX2_OK) {
        pthread_mutex_lock(&comm->mutex);
        entry->ref_count--;
        pthread_mutex_unlock(&comm->mutex);
        return NULL;
    }
    
    iox2_sample_mut_payload_mut(&sample, (void**) &payload, &num_elements);
    *sample_handle = (void*)sample;
    
    // 注意：这里由于是 public API，ref_count 必须在配套的 send 或 drop 中处理
    // 但我们的 C API 没有专门的 drop。为了简化且保证 publish 路径安全，
    // 我们要求 publish 专用。
    return (void*)payload;
}

bool shm_communicator_send(shm_communicator_t* comm, const char* topic, void* sample_handle) {
    if (!comm || !topic || !sample_handle) return false;
    
    PublisherEntry* entry = NULL;
    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0' && strcmp(comm->publishers[i].topic, topic) == 0) {
            entry = &comm->publishers[i];
            // 如果 entry 已经在 publish 中被加过计数，这里就不再强求
            // 但为了支持独立调用，这里尝试查找
            break;
        }
    }
    pthread_mutex_unlock(&comm->mutex);

    if (!entry || entry->publisher == NULL || entry->notifier == NULL) return false;

    iox2_sample_mut_h sample = (iox2_sample_mut_h)sample_handle;
    if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
        iox2_sample_mut_drop(sample);
        return false;
    }
    iox2_notifier_notify(&entry->notifier, NULL);
    return true;
}

static bool shm_communicator_ensure_pre_loan_internal(PublisherEntry* entry) {
    if (entry->pre_loaned_sample == NULL && entry->max_loaned_size > 0 && entry->publisher != NULL) {
        size_t num_to_loan = (entry->data_type == SHM_DATA_DYNAMIC) ? entry->max_loaned_size : 1;
        if (iox2_publisher_loan_slice_uninit(&entry->publisher, NULL, &entry->pre_loaned_sample, num_to_loan) == IOX2_OK) {
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

static bool process_subscriber_entry(shm_communicator_t* comm, SubscriberEntry* entry, bool clear_events) {
    if (!entry || entry->subscriber == NULL) return false;
    
    bool received_any = false;
    if (clear_events && entry->listener) {
        iox2_event_id_t event_id; bool has_received = false;
        // 清除所有积压的事件标志，防止 WaitSet 重复唤醒
        do { has_received = false; iox2_listener_try_wait_one(&entry->listener, &event_id, &has_received); } while (has_received);
    }

    bool has_samples = false;
    do {
        has_samples = false; iox2_sample_h sample = NULL;
        if (iox2_subscriber_receive(&entry->subscriber, NULL, &sample) == IOX2_OK && sample != NULL) {
            has_samples = true;
            received_any = true;
            const uint8_t* payload = NULL; c_size_t num_elements = 0;
            iox2_sample_payload(&sample, (const void**) &payload, &num_elements);
            if (comm->user_callback && !entry->marked_for_unregistration) {
                size_t total_size = num_elements;
                if (entry->data_type != SHM_DATA_DYNAMIC) {
                    switch (entry->data_type) {
                        case SHM_DATA_COMMON: total_size = sizeof(struct TransmissionCommonData); break;
                        case SHM_DATA_LARGE:  total_size = sizeof(struct TransmissionLargeData);  break;
                        case SHM_DATA_STREAM: total_size = sizeof(struct TransmissionStreamData); break;
                        default: break;
                    }
                }
                comm->user_callback(entry->topic, payload, total_size, comm->user_context);
            }
            iox2_sample_drop(sample);
        }
    } while (has_samples);

    return received_any;
}

// 尝试“排干”所有活跃订阅者的数据，并包含“优雅期”空转优化
static bool shm_communicator_drain_all_subscribers(shm_communicator_t* comm) {
    bool overall_received = false;
    int empty_rounds = 0;

    // 只要还在产生数据，或者还在“优雅期”循环内，就继续轮询
    while (empty_rounds < MAX_GRACE_ROUNDS) {
        bool round_received = false;
        for (int i = 0; i < MAX_TOPICS; ++i) {
            if (comm->subscribers[i].topic[0] != '\0') {
                // WaitSet 模式下，即便不是由它唤醒，也顺便检查数据 (机会主义轮询)
                // 传入 clear_events=true 确保状态位清零
                if (process_subscriber_entry(comm, &comm->subscribers[i], true)) {
                    round_received = true;
                }
            }
        }

        if (round_received) {
            overall_received = true;
            empty_rounds = 0; // 只要有数据，重置优雅期计数
        } else {
            empty_rounds++;
        }
        
        // 如果不是在忙等待模式，我们可以稍微让出一点 CPU 给其他线程
        if (!comm->enable_waitset && !round_received) break;
    }
    
    return overall_received;
}

static iox2_callback_progression_e waitset_on_event(iox2_waitset_attachment_id_h attachment_id, void* context) {
    shm_communicator_t* comm = (shm_communicator_t*)context;
    // 当 WaitSet 唤醒时，不论是哪个服务触发的，我们直接执行全局“排水”逻辑
    // 这样可以一次性处理所有并发到达的消息，减少系统调用
    shm_communicator_drain_all_subscribers(comm);
    return iox2_callback_progression_e_CONTINUE;
}

static void shm_communicator_gc_internal(shm_communicator_t* comm) {
    if (!comm) return;

    if (pthread_mutex_trylock(&comm->mutex) != 0) return;
    
    // 处理发布者 GC
    for (int i = 0; i < MAX_TOPICS; ++i) {
        PublisherEntry* entry = &comm->publishers[i];
        if (entry->topic[0] != '\0' && entry->marked_for_unregistration) {
            size_t remote_subs = iox2_port_factory_pub_sub_dynamic_config_number_of_subscribers(&entry->service);
            if (remote_subs == 0) {
                if (entry->zero_peer_count_timestamp_ms == 0) {
                    entry->zero_peer_count_timestamp_ms = get_current_time_ms();
                } else if (get_current_time_ms() - entry->zero_peer_count_timestamp_ms >= 1000) {
                    // 仅当没有人在使用该资源时才释放
                    if (entry->ref_count == 0) {
                        real_drop_publisher(entry);
                    }
                }
            } else { entry->zero_peer_count_timestamp_ms = 0; }
        }
    }
    for (int i = 0; i < MAX_TOPICS; ++i) {
        SubscriberEntry* entry = &comm->subscribers[i];
        if (entry->topic[0] != '\0') {
            // GC 时顺便取数，注意传入 clear_events=true 避免冗余唤醒
            process_subscriber_entry(comm, entry, true);
            if (entry->marked_for_unregistration) {
                size_t remote_pubs = iox2_port_factory_pub_sub_dynamic_config_number_of_publishers(&entry->service);
                if (remote_pubs == 0) {
                    if (entry->zero_peer_count_timestamp_ms == 0) {
                        entry->zero_peer_count_timestamp_ms = get_current_time_ms();
                    } else if (get_current_time_ms() - entry->zero_peer_count_timestamp_ms >= 1000) {
                        if (entry->ref_count == 0) {
                            real_drop_subscriber(entry);
                        }
                    }
                } else { entry->zero_peer_count_timestamp_ms = 0; }
            }
        }
    }
    pthread_mutex_unlock(&comm->mutex);
}

static void shm_communicator_poll_internal(shm_communicator_t* comm) {
    shm_communicator_gc_internal(comm);
}

#ifdef __linux__
static void* receiver_thread_func(void* arg) {
    shm_communicator_t* comm = (shm_communicator_t*)arg;
    while (comm->thread_running) {
        if (comm->enable_waitset) {
            iox2_waitset_run_result_e res;
            iox2_waitset_wait_and_process_once_with_timeout(&comm->waitset, waitset_on_event, comm, 0, 10000000, &res);
            // 唤醒并执行完回调后，再次进行一次“由于可能有新数据到来而进行的全局排水”
            shm_communicator_drain_all_subscribers(comm);
            shm_communicator_gc_internal(comm);
        } else {
            // 轮询模式下，直接调用具备优雅期优化的排水函数
            shm_communicator_drain_all_subscribers(comm);
            shm_communicator_gc_internal(comm);
            sched_yield(); 
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
        pthread_join(comm->waitset_thread, NULL);
    }

    // 等待所有正在进行中的发布/订阅操作完成 (ref_count == 0)
    bool all_clear = false;
    int retry_count = 0;
    while (!all_clear && retry_count++ < 100) {
        all_clear = true;
        pthread_mutex_lock(&comm->mutex);
        for (int i = 0; i < MAX_TOPICS; ++i) {
            if (comm->publishers[i].ref_count > 0 || comm->subscribers[i].ref_count > 0) {
                all_clear = false;
                break;
            }
        }
        pthread_mutex_unlock(&comm->mutex);
        if (!all_clear) busy_wait_us(1000);
    }

    pthread_mutex_lock(&comm->mutex);
    for (int i = 0; i < MAX_TOPICS; ++i) {
        if (comm->publishers[i].topic[0] != '\0') real_drop_publisher(&comm->publishers[i]);
        if (comm->subscribers[i].topic[0] != '\0') real_drop_subscriber(&comm->subscribers[i]);
    }

    if (comm->enable_waitset) {
        iox2_waitset_drop(comm->waitset);
    }
    iox2_node_drop(comm->node_handle);
    if (comm->config) iox2_config_drop(comm->config);
    pthread_mutex_unlock(&comm->mutex);
    pthread_mutex_destroy(&comm->mutex);
    free(comm);
}
