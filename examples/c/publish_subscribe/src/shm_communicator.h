// Copyright (c) 2024 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0 OR MIT

// 文件描述: shm_communicator.h
// 该文件定义了 C 语言版本的共享内存通信器，提供了统一的接口管理发布者和订阅者。
// 类似于 C++ 版本的 SharedMemoryCommunicator，它封装了冰羚 (iceoryx2) 的
// 发布-订阅机制，包含服务注册、数据发布以及 WaitSet 机制的事件驱动数据接收。

#ifndef IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
#define IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// 定义通信器不透明句柄
typedef struct shm_communicator_t shm_communicator_t;

// 定义接收到数据时的回调函数类型
// topic: 接收到数据的主题名称
// data: 数据负载内容
// size: 数据负载大小(字节)
// user_context: 用户注册时传入的上下文指针
typedef void (*on_receive_cb)(const char* topic, const void* data, size_t size, void* user_context);

// 创建一个新的通信器实例
// 返回非NULL的句柄表示成功
shm_communicator_t* shm_communicator_create(void);

// 初始化通信器
// comm: 通信器句柄
// config_path: iceoryx2 配置文件路径 (可以为 NULL，使用系统默认)
// enable_waitset: 是否启用事件驱动的 WaitSet 模式 (true: 事件驱动微秒延迟, false: 纯忙轮询纳秒延迟)
// callback: 订阅者接收数据时触发的数据回调
// user_context: 需要透传给回调函数的用户上下文
// 返回 true 表示成功 
bool shm_communicator_init(shm_communicator_t* comm, 
                           const char* config_path,
                           bool enable_waitset,
                           on_receive_cb callback, 
                           void* user_context);

// 注册发布者
bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic);

// 注销发布者
void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic);

/**
 * @brief 注册订阅者（消费者）到指定主题
 * 
 * 内部将创建订阅者并把其 Listener 附加到进程的 WaitSet.
 *
 * @param comm      通信器实例
 * @param topic     主题名称，如 "SensorData"
 * @return bool     成功发挥 true，否则 false
 */
bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic);

// 注销订阅者
void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic);

/**
 * @brief 启动底层接收线程 (仅支持 Linux)
 * 
 * 在注册完所有的主题之后调用。此函数将生成一个后台线程，该线程专职负责等待
 * WaitSet 并在收到数据时触发回调函数。
 * 
 * @param comm      通信器实例
 * @param cpu_core_id 绑定的 CPU 核心 ID。传入 -1 表示不绑定（由 OS 自动调度）。
 * @return bool     成功启动返回 true，否则 false
 */
bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id);

// 向指定的主题发布数据
bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size);

// 处理内置的 WaitSet 事件
// timeout_ms: 最大的阻塞等待时间（毫秒）
// 返回 false 表示接收到终止信号或遇到不可逆错误，返回 true 表示未遇到致命错误可以继续运行
bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms);

// 同步轮询所有活跃的订阅者并触发回调 (Busy Polling)
// 只有在 enable_waitset = false 时使用，否则由后台 WaitSet 线程自动处理
// 专为追求极致纳秒级延迟的场景设计
void shm_communicator_poll(shm_communicator_t* comm);

// 销毁通信器实例并释放资源
void shm_communicator_destroy(shm_communicator_t* comm);

#ifdef __cplusplus
}
#endif

#endif // IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
