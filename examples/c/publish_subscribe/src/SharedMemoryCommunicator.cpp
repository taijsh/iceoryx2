#include "SharedMemoryCommunicator.h"
#include <iostream>
#include <cstring>
#include <vector>
#include <stdexcept>

/**
 * @brief 构造函数实现
 * 
 * 初始化底层的 C 通信器，并将 C++ 本身作为上下文传入，用于分发回调。
 */
SharedMemoryCommunicator::SharedMemoryCommunicator(const std::string& config_path, bool enable_waitset)
    : m_comm(nullptr), m_running(false)
{
    // 创建 C 通信器实例
    m_comm = shm_communicator_create();
    if (!m_comm) {
        throw std::runtime_error("无法创建底层的 shm_communicator 实例");
    }

    const char* config_ptr = config_path.empty() ? nullptr : config_path.c_str();

    // 初始化 C 通信器，设置跳板回调
    if (!shm_communicator_init(m_comm, config_ptr, enable_waitset, c_callback_wrapper, this)) {
        shm_communicator_destroy(m_comm);
        m_comm = nullptr;
        throw std::runtime_error("底层的 shm_communicator 初始化失败");
    }
}

/**
 * @brief 析构函数实现
 * 
 * 停止服务并销毁 C 通信器。
 */
SharedMemoryCommunicator::~SharedMemoryCommunicator()
{
    stop();
    if (m_comm) {
        shm_communicator_destroy(m_comm);
        m_comm = nullptr;
    }
}

/**
 * @brief 启动后台接收
 */
bool SharedMemoryCommunicator::start(int cpu_core_id)
{
    if (!m_comm) return false;
    if (m_running) return true;

    if (shm_communicator_start(m_comm, cpu_core_id)) {
        m_running = true;
        return true;
    }
    return false;
}

/**
 * @brief 停止接收
 */
void SharedMemoryCommunicator::stop()
{
    m_running = false;
}

/**
 * @brief 注册发布者
 */
bool SharedMemoryCommunicator::registerPublisher(const std::string& topic, shm_data_type_e type)
{
    if (!m_comm) return false;
    return shm_communicator_register_publisher_ext(m_comm, topic.c_str(), type);
}

/**
 * @brief 注册订阅者
 */
bool SharedMemoryCommunicator::registerSubscriber(const std::string& topic, shm_data_type_e type)
{
    if (!m_comm) return false;
    return shm_communicator_register_subscriber_ext(m_comm, topic.c_str(), type);
}

/**
 * @brief 发布附带 DataHeader 的消息
 * 
 * 将头部信息和负载打包在一起发送。去除了优先级逻辑，直接按主题发送。
 */
bool SharedMemoryCommunicator::publish(const void* data, size_t len, const char* topic_id, uint64_t timestamp, uint64_t seq, const char* component_id, uint32_t block)
{
    if (!m_comm) return false;

    // 填充头部信息
    DataHeader header;
    header.len = (uint32_t)len;
    header.pid = 0; 
    std::strncpy(header.topic_id, topic_id, sizeof(header.topic_id) - 1);
    header.topic_id[sizeof(header.topic_id) - 1] = '\0';
    header.timestamp = timestamp;
    header.seq = seq;
    header.block = block;
    std::strncpy(header.component_id, component_id, sizeof(header.component_id) - 1);
    header.component_id[sizeof(header.component_id) - 1] = '\0';

    // 使用多块发布接口，避免手动拼接 Header 和 Payload
    const void* blocks[2] = { &header, data };
    size_t sizes[2] = { sizeof(DataHeader), len };
    
    return shm_communicator_publish_multi(m_comm, topic_id, blocks, sizes, (len > 0 && data) ? 2 : 1);
}

/**
 * @brief 原生发布接口 (不带头部)
 */
bool SharedMemoryCommunicator::publishRaw(const std::string& topic, const void* data, size_t len)
{
    if (!m_comm) return false;
    return shm_communicator_publish(m_comm, topic.c_str(), data, len);
}

/**
 * @brief 轮询处理
 */
void SharedMemoryCommunicator::poll()
{
    if (m_comm) {
        shm_communicator_poll(m_comm);
    }
}

/**
 * @brief C 回调跳板函数
 * 
 * 将 C 回调转换回 C++ 虚函数调用。
 */
void SharedMemoryCommunicator::c_callback_wrapper(const char* topic, const void* data, size_t size, void* user_context)
{
    if (!user_context || !data || size < sizeof(DataHeader)) return;

    SharedMemoryCommunicator* self = static_cast<SharedMemoryCommunicator*>(user_context);
    
    // 解析头部
    const DataHeader* header = static_cast<const DataHeader*>(data);
    const void* payload = static_cast<const char*>(data) + sizeof(DataHeader);
    
    // 触发虚函数（剥离头部后的数据）
    self->on_message_received(payload, header);
}
