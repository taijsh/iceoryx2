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

int main(int argc, char** argv) {
    // 设置日志
    iox2_set_log_level_from_env_or(iox2_log_level_e_TRACE);

    // 确定配置文件路径
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

    // 创建事件服务名称 (用于通知 WaitSet)
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

    // 设置发布订阅负载类型为动态大小的字节数组
    const char* payload_type_name = "uint8_t";
    if (iox2_service_builder_pub_sub_set_payload_type_details(&service_builder_pub_sub,
                                                              iox2_type_variant_e_DYNAMIC,
                                                              payload_type_name,
                                                              strlen(payload_type_name),
                                                              sizeof(uint8_t),
                                                              alignof(uint8_t)) != IOX2_OK) {
        printf("无法设置负载类型详情！\n");
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

    // 创建发布者
    iox2_port_factory_publisher_builder_h publisher_builder =
        iox2_port_factory_pub_sub_publisher_builder(&service, NULL);
    iox2_port_factory_publisher_builder_set_allocation_strategy(&publisher_builder, iox2_allocation_strategy_e_POWER_OF_TWO);
    iox2_port_factory_publisher_builder_set_initial_max_slice_len(&publisher_builder, 1024);
    
    iox2_publisher_h publisher = NULL;
    if (iox2_port_factory_publisher_builder_create(publisher_builder, NULL, &publisher) != IOX2_OK) {
        printf("无法创建发布者！\n");
        goto drop_event_service;
    }

    // 创建通知器
    iox2_port_factory_notifier_builder_h notifier_builder = iox2_port_factory_event_notifier_builder(&event_service, NULL);
    iox2_notifier_h notifier = NULL;
    if (iox2_port_factory_notifier_builder_create(notifier_builder, NULL, &notifier) != IOX2_OK) {
        printf("无法创建通知器！\n");
        goto drop_publisher;
    }

    int32_t counter = 0;
    while (iox2_node_wait(&node_handle, 1, 0) == IOX2_OK) {
        counter += 1;
        // 保证数据大小至少能容纳一个 uint64_t 的时间戳
        uint64_t data_size = sizeof(uint64_t) + counter * 10;
        
        // 借用动态大小的样本
        iox2_sample_mut_h sample = NULL;
        if (iox2_publisher_loan_slice_uninit(&publisher, NULL, &sample, data_size) != IOX2_OK) {
            printf("无法借用样本！\n");
            goto drop_notifier;
        }

        // 写入负载数据
        uint8_t* payload = NULL;
        c_size_t num_elements = 0;
        iox2_sample_mut_payload_mut(&sample, (void**) &payload, &num_elements);
        
        // 在负载开头写入时间戳
        uint64_t current_time = get_time_ns();
        memcpy(payload, &current_time, sizeof(uint64_t));

        for (c_size_t i = sizeof(uint64_t); i < num_elements; ++i) {
            payload[i] = (uint8_t)(counter % 256);
        }

        // 发送样本
        if (iox2_sample_mut_send(sample, NULL) != IOX2_OK) {
            printf("无法发送样本！\n");
            goto drop_notifier;
        }
        
        // 发送通知
        iox2_notifier_notify(&notifier, NULL);

        printf("发送了 %llu 字节的样本，序号 %d ...\n", (unsigned long long)data_size, counter);
    }

drop_notifier:
    iox2_notifier_drop(notifier);

drop_publisher:
    iox2_publisher_drop(publisher);

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
