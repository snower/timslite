/* Timslite C Header */

#ifndef TIMSLITE_H
#define TIMSLITE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stddef.h>

/* Data types */
#define TIMSLITE_TYPE_INDEX 0
#define TIMSLITE_TYPE_WAVE 1
#define TIMSLITE_TYPE_MEASURE 2
#define TIMSLITE_TYPE_EVENT 3
#define TIMSLITE_TYPE_WAL 4
#define TIMSLITE_TYPE_MANUAL_MEASURE 5

/* Opaque handles */
typedef struct TimeStoreHandle TimeStoreHandle;
typedef struct DatasetHandle DatasetHandle;

/* 
 * Open a time-series store
 * 
 * @param data_dir Path to data directory
 * @return Handle to store, or NULL on error
 */
TimeStoreHandle* timslite_open(const char* data_dir);

/*
 * Close a time-series store
 * 
 * @param handle Store handle
 */
void timslite_close(TimeStoreHandle* handle);

/*
 * Open a dataset
 * 
 * @param handle Store handle
 * @param name Dataset name
 * @param data_type Data type (0-5)
 * @return Handle to dataset, or NULL on error
 */
DatasetHandle* timslite_open_dataset(TimeStoreHandle* handle, const char* name, int32_t data_type);

/*
 * Close a dataset
 * 
 * @param handle Dataset handle
 */
void timslite_close_dataset(DatasetHandle* handle);

/*
 * Write data to dataset
 * 
 * @param handle Dataset handle
 * @param timestamp Timestamp (seconds)
 * @param data Data buffer
 * @param data_len Data length
 * @return Offset of written data, or -1 on error
 */
int64_t timslite_write(DatasetHandle* handle, int64_t timestamp, const uint8_t* data, size_t data_len);

/*
 * Read data from dataset
 * 
 * @param handle Dataset handle
 * @param start_timestamp Start timestamp (inclusive)
 * @param end_timestamp End timestamp (inclusive)
 * @param out_data Output buffer (can be NULL to get count)
 * @param out_data_len Output: number of records
 * @return 0 on success, -1 on error
 */
int32_t timslite_read(
    DatasetHandle* handle,
    int64_t start_timestamp,
    int64_t end_timestamp,
    uint8_t* out_data,
    size_t* out_data_len
);

/*
 * Flush dataset to disk
 * 
 * @param handle Dataset handle
 * @return 0 on success, -1 on error
 */
int32_t timslite_flush(DatasetHandle* handle);

/*
 * Get error message for error code
 * 
 * @param error_code Error code
 * @return Error message string (caller must free with timslite_free_string)
 */
char* timslite_error_message(int32_t error_code);

/*
 * Free a string returned by timslite
 * 
 * @param s String to free
 */
void timslite_free_string(char* s);

#ifdef __cplusplus
}
#endif

#endif /* TIMSLITE_H */