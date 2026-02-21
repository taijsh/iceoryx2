// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache Software License 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license
// which is available at https://opensource.org/licenses/MIT.
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

#include "iox2/iceoryx2.h"

#if defined(_WIN32) || defined(WIN32) || defined(__WIN32__) || defined(_WIN64)
#define alignof __alignof
#else
#include <stdalign.h>
#endif
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

// 获取当前时间的纳秒表示
uint64_t get_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

// 包含守卫和订阅者、监听器的上下文
struct CallbackContext {
    iox2_waitset_guard_h_ref guard;
    iox2_listener_h_ref listener;
    iox2_subscriber_h_ref subscriber;
    
    // 延迟统计
    uint64_t min_latency;
    uint64_t max_latency;
    uint64_t total_latency;
    uint64_t sample_count;
};

// 当监听器接收到事件时调用的回调函数
iox2_callback_progression_e on_event(iox2_waitset_attachment_id_h attachment_id, void* context) {
    struct CallbackContext* ctx = (struct CallbackContext*) context;

    iox2_event_id_t event_id;
    bool has_received_event = false;

    // 检查事件是否来自我们的监听器
    if (iox2_waitset_attachment_id_has_event_from(&attachment_id, ctx->guard)) {
        // 重要提示：
        // 我们需要收集所有通知，因为只要有内容可读，WaitSet 就会唤醒我们。如果完全跳过此步骤，我们最终会陷入繁忙的循环中。
        do {
            if (iox2_listener_try_wait_one(ctx->listener, &event_id, &has_received_event) != IOX2_OK) {
                printf("无法在监听器上接收事件\n");
            }
        } while (has_received_event);

        // 既然我们知道发布者已经发送了数据，那么就可以从订阅者处接收并处理所有排队的样本
        bool has_samples = false;
        do {
            has_samples = false;
            iox2_sample_h sample = NULL;
            if (iox2_subscriber_receive(ctx->subscriber, NULL, &sample) != IOX2_OK) {
                printf("接收样本失败\n");
                break;
            }

            if (sample != NULL) {
                has_samples = true;
                const uint8_t* payload = NULL;
                c_size_t num_elements = 0;
                iox2_sample_payload(&sample, (const void**) &payload, &num_elements);

                if (num_elements >= sizeof(uint64_t)) {
                    uint64_t recv_time = get_time_ns();
                    uint64_t send_time;
                    memcpy(&send_time, payload, sizeof(uint64_t));
                    
                    uint64_t latency = recv_time - send_time;
                    ctx->sample_count++;
                    ctx->total_latency += latency;
                    if (latency < ctx->min_latency) ctx->min_latency = latency;
                    if (latency > ctx->max_latency) ctx->max_latency = latency;
                    
                    uint64_t avg_latency = ctx->total_latency / ctx->sample_count;

                    printf("接收 %llu 字节 | 延迟: %llu ns | 最小: %llu ns | 最大: %llu ns | 平均: %llu ns\n",
                           (unsigned long long)num_elements, (unsigned long long)latency,
                           (unsigned long long)ctx->min_latency, (unsigned long long)ctx->max_latency,
                           (unsigned long long)avg_latency);
                } else {
                    printf("成功接收了 %llu 字节的样本数据 (无时间戳)\n", (unsigned long long)num_elements);
                }

                iox2_sample_drop(sample);
            }
        } while (has_samples);
    }

    iox2_waitset_attachment_id_drop(attachment_id);
    return iox2_callback_progression_e_CONTINUE;
}

int main(int argc, char** argv) {
    // 设置日志
    iox2_set_log_level_from_env_or(iox2_log_level_e_TRACE);

    // 确定配置文件路径iox2_log_level_e_TRACE
    const char* config_path = "/home/taijsh/ljtx/ljtx_component.toml";
    if (argc > 1) {
        config_path = argv[1];
    }
    printf("使用配置文件: %s\n", config_path);

    // 加载自定义配置
    iox2_config_h config = NULL;
    if (iox2_config_from_file(NULL, &config, config_path) != IOX2_OK) {
        printf("无法从文件加载配置: %s\n", config_path);
        goto end;
    }

    // 创建新节点并注入配置
    iox2_node_builder_h node_builder_handle = iox2_node_builder_new(NULL);
    iox2_node_builder_set_config(&node_builder_handle, &config);
    
    iox2_node_h node_handle = NULL;
    if (iox2_node_builder_create(node_builder_handle, NULL, iox2_service_type_e_IPC, &node_handle) != IOX2_OK) {
        printf("无法创建节点！\n");
        goto end;
    }

    // 创建发布订阅服务名称
    const char* service_name_value = "My/Funk/PubSubDynamic_C";
    iox2_service_name_h service_name = NULL;
    if (iox2_service_name_new(NULL, service_name_value, strlen(service_name_value), &service_name) != IOX2_OK) {
        printf("无法创建服务名称！\n");
        goto drop_node;
    }

    // 创建事件服务名称
    const char* event_service_name_value = "My/Funk/PubSubDynamicEvents_C";
    iox2_service_name_h event_service_name = NULL;
    if (iox2_service_name_new(NULL, event_service_name_value, strlen(event_service_name_value), &event_service_name) != IOX2_OK) {
        printf("无法创建事件服务名称！\n");
        goto drop_service_name;
    }

    // 创建发布订阅服务构建器
    iox2_service_name_ptr service_name_ptr = iox2_cast_service_name_ptr(service_name);
    iox2_service_builder_h service_builder = iox2_node_service_builder(&node_handle, NULL, service_name_ptr);
    iox2_service_builder_pub_sub_h service_builder_pub_sub = iox2_service_builder_pub_sub(service_builder);

    // 设置发布订阅负载类型为动态大小的主题
    const char* payload_type_name = "uint8_t";
    if (iox2_service_builder_pub_sub_set_payload_type_details(&service_builder_pub_sub,
                                                              iox2_type_variant_e_DYNAMIC,
                                                              payload_type_name,
                                                              strlen(payload_type_name),
                                                              sizeof(uint8_t),
                                                              alignof(uint8_t)) != IOX2_OK) {
        printf("无法设置负载类型详情\n");
        goto drop_event_service_name;
    }

    // 创建发布订阅服务
    iox2_port_factory_pub_sub_h service = NULL;
    if (iox2_service_builder_pub_sub_open_or_create(service_builder_pub_sub, NULL, &service) != IOX2_OK) {
        printf("无法创建发布订阅服务！\n");
        goto drop_event_service_name;
    }

    // 创建事件服务构建器
    iox2_service_name_ptr event_service_name_ptr = iox2_cast_service_name_ptr(event_service_name);
    iox2_service_builder_h event_service_builder = iox2_node_service_builder(&node_handle, NULL, event_service_name_ptr);
    iox2_service_builder_event_h event_service_builder_event = iox2_service_builder_event(event_service_builder);

    // 创建事件服务
    iox2_port_factory_event_h event_service = NULL;
    if (iox2_service_builder_event_open_or_create(event_service_builder_event, NULL, &event_service) != IOX2_OK) {
        printf("无法创建事件服务！\n");
        goto drop_service;
    }

    // 创建订阅者
    iox2_port_factory_subscriber_builder_h subscriber_builder =
        iox2_port_factory_pub_sub_subscriber_builder(&service, NULL);
    iox2_subscriber_h subscriber = NULL;
    if (iox2_port_factory_subscriber_builder_create(subscriber_builder, NULL, &subscriber) != IOX2_OK) {
        printf("无法创建订阅者！\n");
        goto drop_event_service;
    }

    // 创建事件监听器
    iox2_port_factory_listener_builder_h listener_builder =
        iox2_port_factory_event_listener_builder(&event_service, NULL);
    iox2_listener_h listener = NULL;
    if (iox2_port_factory_listener_builder_create(listener_builder, NULL, &listener) != IOX2_OK) {
        printf("无法创建接收器！\n");
        goto drop_subscriber;
    }

    // 创建 WaitSet
    iox2_waitset_builder_h waitset_builder = NULL;
    iox2_waitset_builder_new(NULL, &waitset_builder);
    iox2_waitset_h waitset = NULL;
    if (iox2_waitset_builder_create(waitset_builder, iox2_service_type_e_IPC, NULL, &waitset) != IOX2_OK) {
        printf("无法创建 WaitSet\n");
        goto drop_listener;
    }

    // 将监听器连接到 WaitSet
    iox2_waitset_guard_h guard = NULL;
    if (iox2_waitset_attach_notification(&waitset, iox2_listener_get_file_descriptor(&listener), NULL, &guard) != IOX2_OK) {
        printf("无法连接监听器\n");
        goto drop_waitset;
    }

    struct CallbackContext context;
    context.guard = &guard;
    context.listener = &listener;
    context.subscriber = &subscriber;
    context.min_latency = UINT64_MAX;
    context.max_latency = 0;
    context.total_latency = 0;
    context.sample_count = 0;

    printf("订阅者准备好使用 WaitSet 接收动态长度数据并统计延迟！\n");

    iox2_waitset_run_result_e result = iox2_waitset_run_result_e_STOP_REQUEST;
    if (iox2_waitset_wait_and_process(&waitset, on_event, (void*) &context, &result) != IOX2_OK) {
        printf("WaitSet 处理循环出错\n");
    }

    iox2_waitset_guard_drop(guard);

drop_waitset:
    iox2_waitset_drop(waitset);

drop_listener:
    iox2_listener_drop(listener);

drop_subscriber:
    iox2_subscriber_drop(subscriber);

drop_event_service:
    iox2_port_factory_event_drop(event_service);

drop_service:
    iox2_port_factory_pub_sub_drop(service);

drop_event_service_name:
    iox2_service_name_drop(event_service_name);

drop_service_name:
    iox2_service_name_drop(service_name);

drop_node:
    iox2_node_drop(node_handle);

    // 释放自定义配置
    if (config != NULL) {
        iox2_config_drop(config);
    }

end:
    return 0;
}
