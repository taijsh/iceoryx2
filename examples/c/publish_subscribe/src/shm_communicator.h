#ifndef IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
#define IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct shm_communicator_t shm_communicator_t;

typedef void (*on_receive_cb)(const char* topic, const void* data, size_t size, void* user_context);

shm_communicator_t* shm_communicator_create(void);
bool shm_communicator_init(shm_communicator_t* comm, const char* config_path, bool enable_waitset, on_receive_cb callback, void* user_context);

typedef enum {
    SHM_DATA_DYNAMIC,
    SHM_DATA_COMMON,
    SHM_DATA_LARGE,
    SHM_DATA_STREAM
} shm_data_type_e;

bool shm_communicator_register_publisher(shm_communicator_t* comm, const char* topic);
bool shm_communicator_register_publisher_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type);
void shm_communicator_unregister_publisher(shm_communicator_t* comm, const char* topic);

bool shm_communicator_register_subscriber(shm_communicator_t* comm, const char* topic);
bool shm_communicator_register_subscriber_ext(shm_communicator_t* comm, const char* topic, shm_data_type_e type);
void shm_communicator_unregister_subscriber(shm_communicator_t* comm, const char* topic);

bool shm_communicator_start(shm_communicator_t* comm, int cpu_core_id);
bool shm_communicator_publish(shm_communicator_t* comm, const char* topic, const void* data, size_t size);
/**
 * @brief 将数据从用户内存直接拷贝到共享内存并发送 (非零拷贝接口)
 * 
 * @param comm 通信器指针
 * @param topic 主题名称
 * @param data 用户数据指针
 * @param size 数据字节数
 * @return true 发送成功
 * @return false 发送失败
 */
bool shm_communicator_publish_copy(shm_communicator_t* comm, const char* topic, const void* data, size_t size);

// Optimized Zero-Copy API
void* shm_communicator_loan_uninit(shm_communicator_t* comm, const char* topic, size_t size, void** sample_handle);
bool shm_communicator_send(shm_communicator_t* comm, const char* topic, void* sample_handle);
bool shm_communicator_ensure_pre_loan(shm_communicator_t* comm, const char* topic);

bool shm_communicator_process_events(shm_communicator_t* comm, uint64_t timeout_ms);
void shm_communicator_poll(shm_communicator_t* comm);

void shm_communicator_destroy(shm_communicator_t* comm);

#ifdef __cplusplus
}
#endif

#endif // IOX2_EXAMPLES_C_SHM_COMMUNICATOR_H
