#ifndef SHARED_MEMORY_COMMUNICATOR_H
#define SHARED_MEMORY_COMMUNICATOR_H

#include <string>
#include <vector>
#include <memory>
#include <atomic>

// 引入 C 语言通信器头文件 (由 .h 内部或此处声明)
#ifdef __cplusplus
extern "C" {
#endif
#include "shm_communicator.h"
#ifdef __cplusplus
}
#endif

/**
 * @brief SharedMemoryCommunicator C++11 封装类
 * 
 * 基于 shm_communicator C API 实现，提供现代 C++ 接口，支持 WaitSet 事件驱动及轮询接收模式。
 */
class SharedMemoryCommunicator {
public:
    /**
     * @brief 数据头部信息总结 (用于兼容性封装)
     */
    struct DataHeader {
        uint32_t len;           // payload 长度
        uint32_t pid;           // 生产者编号
        char topic_id[64];      // 主题名
        uint64_t timestamp;     // 消息发送时间戳 (us)
        uint64_t seq;           // 消息序列号
        uint32_t block;         // 阻塞标记
        char component_id[150]; // 组件 ID
    };

    /**
     * @brief 构造函数 (根据用户要求修改初始化接口)
     * 
     * @param config_path 配置文件路径 (iceoryx2 配置文件)
     * @param enable_waitset 是否启用 WaitSet 事件通知机制
     */
    SharedMemoryCommunicator(const std::string& config_path = "", bool enable_waitset = true);

    /**
     * @brief 析构函数，释放资源
     */
    virtual ~SharedMemoryCommunicator();

    // 禁止拷贝和赋值
    SharedMemoryCommunicator(const SharedMemoryCommunicator&) = delete;
    SharedMemoryCommunicator& operator=(const SharedMemoryCommunicator&) = delete;

    /**
     * @brief 初始化并启动后台服务线程
     * 
     * @param cpu_core_id 绑定 CPU 核心 (-1 表示不绑定)
     * @return true 启动成功
     */
    bool start(int cpu_core_id = -1);

    /**
     * @brief 停止后台服务线程
     */
    void stop();

    /**
     * @brief 注册发布者
     * 
     * @param topic 主题名称
     * @param type 数据类型
     * @return true 注册成功
     */
    bool registerPublisher(const std::string& topic, shm_data_type_e type = SHM_DATA_DYNAMIC);

    /**
     * @brief 注册订阅者
     * 
     * @param topic 主题名称
     * @param type 数据类型
     * @return true 注册成功
     */
    bool registerSubscriber(const std::string& topic, shm_data_type_e type = SHM_DATA_DYNAMIC);

    /**
     * @brief 发布消息（带完整头部信息，保持原有接口功能）
     * 
     * @param data 原始负载数据
     * @param len 数据长度
     * @param topic_id 发布到的主题名称
     * @param timestamp 时间戳
     * @param seq 序列号
     * @param component_id 发送者组件 ID
     * @param block 阻塞标志
     * @return true 发送成功
     */
    bool publish(const void* data, size_t len, const char* topic_id, uint64_t timestamp, uint64_t seq, const char* component_id, uint32_t block = 0);

    /**
     * @brief 简单发布接口 (不带 DataHeader，可选使用)
     */
    bool publishRaw(const std::string& topic, const void* data, size_t len);

    /**
     * @brief 手动轮询处理 (非异步模式下使用)
     */
    void poll();

protected:
    /**
     * @brief 接收回调虚函数，子类通过重写此函数处理接收到的数据
     * 
     * @param data 数据内容 (剥离 DataHeader 后的纯负载)
     * @param header 解析出的头部信息 (包含时间戳、序列号等)
     */
    virtual void on_message_received(const void* data, const DataHeader* header) = 0;

private:
    // C 接口的回调跳板
    static void c_callback_wrapper(const char* topic, const void* data, size_t size, void* user_context);

    shm_communicator_t* m_comm = nullptr; // 内部 C 通信器指针
    std::atomic<bool> m_running;          // 运行状态
};

#endif // SHARED_MEMORY_COMMUNICATOR_H
