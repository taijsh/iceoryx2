#ifndef IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
#define IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief 创建共享内存通信器实例
 * 
 * @return shm_communicator_t* 指向新创建的通信器实例的指针；失败时返回 NULL。
 * 
 * @note 该函数仅分配内存，不初始化 iceoryx2 节点。使用后需调用 shm_communicator_init 进行初始化。
 */
shm_communicator_t* shm_communicator_create(void);

/**
 * @brief 初始化共享内存通信器
 * 
 * @param comm 通信器指针
 * @param config_path 配置文件路径，传入 NULL 则使用默认配置
 * @param enable_waitset 是否启用 WaitSet 模式（事件驱动接收）。true 为异步事件模式，false 为轮询模式。
 * @param callback 接收数据时的回调函数
 * @param user_context 用户上下文指针，将传递给回调函数
 * @return true 初始化成功
 * @return false 初始化失败
 * 
 * @note 若启用 WaitSet，接收将由后台线程处理。
 */
bool shm_communicator_init(shm_communicator_t* comm, const char* config_path, bool enable_waitset, on_receive_cb callback, void* user_context);

/**
 * @brief 共享内存数据类型定义
 */
typedef enum {
    SHM_DATA_DYNAMIC, /**< 动态长度数据类型（uint8_t 序列） */
    SHM_DATA_COMMON,  /**< 通用固定长度数据结构 (TransmissionCommonData) */
    SHM_DATA_LARGE,   /**< 大容量固定长度数据结构 (TransmissionLargeData) */
    SHM_DATA_STREAM   /**< 流式数据传输结构 (TransmissionStreamData) */
} shm_data_type_e;

/**
 * @brief 注册一个发布者（默认使用动态长度数据类型）
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @return true 注册成功
 * @return false 注册失败
 */
bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic);

/**
 * @brief 注册一个发布者（可指定数据类型）
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param type 指定的数据类型 (shm_data_type_e)
 * @return true 注册成功
 * @return false 注册失败
 */
bool shm_communicator_register_publisher_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type);

/**
 * @brief 注销指定主题的发布者
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 */
void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic);

/**
 * @brief 注册一个订阅者（默认使用动态长度数据类型）
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @return true 注册成功
 * @return false 注册失败
 */
bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic);

/**
 * @brief 注册一个订阅者（可指定数据类型）
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param type 指定的数据类型 (shm_data_type_e)
 * @return true 注册成功
 * @return false 注册失败
 */
bool shm_communicator_register_subscriber_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type);

/**
 * @brief 注销指定主题的订阅者
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 */
void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic);

/**
 * @brief 启动通信器工作线程
 * 
 * @param comm 通信器指针
 * @param cpu_core_id 绑定的 CPU 核心 ID，传入 -1 则不进行核心绑定
 * @return true 启动成功
 * @return false 启动失败
 * 
 * @note 该函数会启动一个后台线程用于处理数据接收（WaitSet 模式下）或定期轮询。
 */
bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id);

/**
 * @brief 发布数据到指定主题
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param data 待发送数据指针
 * @param size 数据大小（字节）
 * @return true 发送成功
 * @return false 发送失败
 * 
 * @note 该接口内部使用了 loan-copy-send 模式，适用于小型或动态长度数据。
 */
bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size);

/**
 * @brief 直接拷贝用户数据到共享内存并发送（非零拷贝接口）
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param data 待发送数据指针
 * @param size 数据大小（字节）
 * @return true 发送成功
 * @return false 发送失败
 * 
 * @note 相比于 publish，此函数利用底层直接拷贝接口，在某些场景下效率更高。
 */
bool shm_communicator_publish_copy(shm_communicator_t* comm, const char* topic, const void* data, size_t size);

// 优化后的零拷贝接口 (Optimized Zero-Copy API)

/**
 * @brief 借用（Loan）共享内存样本而不初始化
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param size 需要借用的容量大小
 * @param sample_handle [out] 输出样本句柄，用于后续的 send 或清理
 * @return void* 指向借得的共享内存有效负载区域的指针，失败返回 NULL
 * 
 * @note 零拷贝发送的第一步。获取指针后应填充数据，再调用 shm_communicator_send。
 */
void* shm_communicator_loan_uninit(shm_communicator_t* comm, const char* topic, size_t size, void** sample_handle);

/**
 * @brief 发送之前通过 loan 借用的样本
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param sample_handle 样本句柄
 * @return true 发送成功
 * @return false 发送失败
 */
bool shm_communicator_send(shm_communicator_t* comm, const char* topic, void* sample_handle);

/**
 * @brief 确保预借用（Pre-loan）机制生效
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @return true 预借用成功或已存在
 * @return false 机制无法建立
 * 
 * @note 内部优化机制：预先从共享内存池中借用一块缓冲区，减少发送时的分配耗时。
 */
bool shm_communicator_ensure_pre_loan(shm_communicator_t* comm, const char* topic);

/**
 * @brief （外部驱动）处理事件并休眠
 * 
 * @param comm 通信器指针
 * @param timeout_ms 休眠超时时间（毫秒）
 * @return true 执行成功
 * @return false 失败
 */
bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms);

/**
 * @brief 主动触发一次垃圾回收和数据轮询
 * 
 * @param comm 通信器指针
 * 
 * @note 在非异步接收模式下，手动拉取数据时调用。
 */
void shm_communicator_poll(shm_communicator_t* comm);

/**
 * @brief 销毁通信器并释放所有资源
 * 
 * @param comm 通信器指针
 */
void shm_communicator_destroy(shm_communicator_t* comm);

#ifdef __cplusplus
}
#endif

#endif // IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
