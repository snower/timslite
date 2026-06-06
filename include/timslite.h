/**
 * timslite - Time-Series Data Storage Library (C Header)
 *
 * A high-performance, mmap-backed time-series data store with:
 * - Block-level aggregation (max 64KB per block)
 * - Delayed compression (seal on overflow)
 * - Lazy segment lifecycle (on-demand open, idle-close after 30min)
 * - Time-indexed queries with binary search
 */

#ifndef TIMSLITE_H
#define TIMSLITE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TMSL_STORE_CONFIG_FFI_VERSION 4u
#define TMSL_DATASET_CONFIG_FFI_VERSION 2u

typedef struct TmslStoreConfigFFI {
    uint32_t version;
    uint64_t flush_interval_ms;
    uint64_t idle_timeout_ms;
    uint64_t data_segment_size;
    uint64_t index_segment_size;
    uint64_t initial_data_segment_size;
    uint64_t initial_index_segment_size;
    uint64_t cache_max_memory;
    uint64_t cache_idle_timeout_ms;
    uint8_t compress_level;
    uint8_t compress_type; /* 0=zstd (default), 1=deflate */
    uint8_t retention_check_hour; /* UTC hour, 0-23 */
    uint8_t enable_background_thread;
    uint8_t enable_journal; /* 0=false, non-zero=true; default true */
} TmslStoreConfigFFI;

typedef struct TmslDatasetConfigFFI {
    uint32_t version;
    uint64_t data_segment_size;
    uint64_t index_segment_size;
    uint64_t initial_data_segment_size;
    uint64_t initial_index_segment_size;
    uint64_t retention_window;
    uint8_t compress_level;
    uint8_t compress_type; /* 0=zstd (default), 1=deflate */
    uint8_t index_continuous;
} TmslDatasetConfigFFI;

/* 鈹€鈹€鈹€ Store Management 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ */

/**
 * Fill a store config struct with default values.
 * @param out_config   Output config pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_store_config_default(TmslStoreConfigFFI* out_config,
                              char* err_buf, size_t err_buf_len);

/**
 * Open a store at the given directory.
 * @param data_dir     Path to data directory.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque store pointer, or NULL on error.
 */
void* tmsl_store_open(const char* data_dir, char* err_buf, size_t err_buf_len);

/**
 * Open a store at the given directory with explicit config.
 * Passing config = NULL is equivalent to tmsl_store_open.
 * @param data_dir     Path to data directory.
 * @param config       Versioned config pointer, or NULL for defaults.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque store pointer, or NULL on error.
 */
void* tmsl_store_open_with_config(const char* data_dir,
                                  const TmslStoreConfigFFI* config,
                                  char* err_buf, size_t err_buf_len);

/**
 * Close a store and release all resources.
 * Fails if any dataset, iterator, queue, or consumer handle created from this
 * store is still open.
 * @param store        Opaque store pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_store_close(void* store, char* err_buf, size_t err_buf_len);

/**
 * Execute one tick of background tasks synchronously (flush, idle-close,
 * cache eviction, retention reclaim).  Safe to call even when the
 * background thread is enabled.
 * @param store            Opaque store pointer.
 * @param out_executed     Written with the number of tasks executed (0-4).
 * @param out_next_delay_ms Written with the delay in ms until the next task is due.
 * @param err_buf          Buffer for error message.
 * @param err_buf_len      Length of error buffer.
 * @return 0 on success (even if no task was due, executed=0), -1 on error.
 */
int tmsl_store_tick_background_tasks(void* store,
                                     unsigned int* out_executed,
                                     uint64_t* out_next_delay_ms,
                                     char* err_buf, size_t err_buf_len);

/**
 * Query the delay until the next background task is due, without
 * executing any tasks.
 * @param store            Opaque store pointer.
 * @param out_next_delay_ms Written with the delay in milliseconds.
 * @param err_buf          Buffer for error message.
 * @param err_buf_len      Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_store_next_background_delay(void* store,
                                     uint64_t* out_next_delay_ms,
                                     char* err_buf, size_t err_buf_len);

/* 鈹€鈹€鈹€ Dataset Management 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ */

/**
 * Create a new dataset (errors if already exists).
 * @param store                Opaque store pointer.
 * @param name                 Dataset name, must match ^[0-9A-Za-z_-]+$.
 * @param dataset_type         Dataset type, must match ^[0-9A-Za-z_-]+$.
 * @param data_segment_size    Data segment size in bytes.
 * @param index_segment_size   Index segment size in bytes.
 * @param compress_level       Compression level (1-9).
 * @param index_continuous     0 = non-continuous (strict order), 1 = continuous (filler entries).
 * @param retention_window         Data validity period (same unit as timestamp, 0 = no limit).
 *                             Store-level `retention_check_hour` controls daily UTC reclaim schedule.
 * @param err_buf              Buffer for error message.
 * @param err_buf_len          Length of error buffer.
 * @return Opaque dataset pointer, or NULL on error.
 */
void* tmsl_dataset_create(void* store, const char* name, const char* dataset_type,
                          uint64_t data_segment_size, uint64_t index_segment_size,
                          unsigned char compress_level, unsigned char index_continuous,
                          uint64_t retention_window,
                          char* err_buf, size_t err_buf_len);

/**
 * Create a new dataset with explicit config, including initial segment sizes.
 * @param store        Opaque store pointer.
 * @param name         Dataset name, must match ^[0-9A-Za-z_-]+$.
 * @param dataset_type Dataset type, must match ^[0-9A-Za-z_-]+$.
 * @param config       Versioned dataset config pointer (must not be NULL).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque dataset pointer, or NULL on error.
 */
void* tmsl_dataset_create_with_config(void* store,
                                      const char* name,
                                      const char* dataset_type,
                                      const TmslDatasetConfigFFI* config,
                                      char* err_buf, size_t err_buf_len);

/**
 * Drop (delete) an entire dataset.
 * @param store        Opaque store pointer.
 * @param name         Dataset name, must match ^[0-9A-Za-z_-]+$.
 * @param dataset_type Dataset type, must match ^[0-9A-Za-z_-]+$.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_drop(void* store, const char* name, const char* dataset_type,
                      char* err_buf, size_t err_buf_len);

/**
 * Open an existing dataset (errors if not exists).
 * @param store        Opaque store pointer.
 * @param name         Dataset name, must match ^[0-9A-Za-z_-]+$.
 * @param dataset_type Dataset type, must match ^[0-9A-Za-z_-]+$.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque dataset pointer, or NULL on error.
 */
void* tmsl_dataset_open(void* store, const char* name, const char* dataset_type,
                        char* err_buf, size_t err_buf_len);

/**
 * Close a dataset.
 * @param dataset      Opaque dataset pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_close(void* dataset, char* err_buf, size_t err_buf_len);

/**
 * Flush a dataset (msync only, does not seal/ compress).
 * @param dataset      Opaque dataset pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_flush(void* dataset, char* err_buf, size_t err_buf_len);

/**
 * Get the maximum timestamp that has been successfully written to a dataset.
 *
 * This is not necessarily the latest valid/undeleted record. Deleting the
 * current maximum timestamp keeps this value unchanged.
 *
 * @param dataset      Opaque dataset pointer.
 * @param out_ts       Output: maximum written timestamp (0 if the dataset is empty).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_latest_timestamp(void* dataset, int64_t* out_ts,
                                  char* err_buf, size_t err_buf_len);

/* 鈹€鈹€鈹€ Data Write 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ */

/**
 * Write a record to a dataset.
 *
 * Supports three timestamp modes:
 *   - correction: timestamp == latest 鈫?overwrite data in place (index unchanged)
 *   - out-of-order: timestamp < latest 鈫?append to latest segment + update index
 *     entry in place; the old data segment's invalid_record_count is incremented
 *     if the previous entry referenced real data; its global cache entry is invalidated
 *   - in-order: timestamp > latest 鈫?append; continuous mode fills gaps with filler
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp (unit must match the dataset's timestamp scheme).
 * @param data         Raw data bytes.
 * @param data_len     Length of data.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_write(void* dataset, int64_t timestamp,
                       const unsigned char* data, size_t data_len,
                       char* err_buf, size_t err_buf_len);

/**
 * Append bytes to a dataset record.
 *
 * If timestamp is greater than the dataset latest timestamp, this creates a new
 * record. If timestamp equals latest, the latest record must still be the
 * uncompressed tail record; otherwise this returns an error. A single logical
 * record may not exceed 4MiB.
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp to append/create.
 * @param data         Raw data bytes to append.
 * @param data_len     Length of data.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_append(void* dataset, int64_t timestamp,
                        const unsigned char* data, size_t data_len,
                        char* err_buf, size_t err_buf_len);

/**
 * Delete the record at the given timestamp.
 *
 * Marks the index entry as sentinel (block_offset = 0xFFFFFFFFFFFFFFFF,
 * in_block_offset = 0xFFFF) and increments the old data segment's
 * invalid_record_count and invalidates the old global cache entry. The physical data is preserved on disk until
 * retention-based reclamation; current versions do not support compaction.
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp of the record to delete.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error (e.g. not found or already deleted).
 */
int tmsl_dataset_delete(void* dataset, int64_t timestamp,
                        char* err_buf, size_t err_buf_len);

/* 鈹€鈹€鈹€ Single Record Read 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ */

/**
 * Read a single record by exact timestamp.
 *
 * On success (record found): allocates `out_data` via malloc, sets `out_ts`
 * (the actual timestamp of the record) and `out_data_len`. Caller must free
 * `out_data` via `tmsl_data_free`.
 *
 * Shortcut: passing `timestamp = -1` resolves to the maximum written timestamp.
 * It does not search backward for the latest valid record. If the dataset is
 * empty or the maximum timestamp entry has been deleted, returns 1 (not found).
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp of the record to read, or -1 for the maximum written timestamp.
 * @param out_ts       Output: actual timestamp of the record returned.
 * @param out_data     Output: pointer to data (malloc'd, must be freed via tmsl_data_free).
 * @param out_data_len Output: data length in bytes.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 = success, 1 = not found (or filler/deleted/empty dataset with -1), -1 = error.
 */
int tmsl_dataset_read(void* dataset, int64_t timestamp,
                      int64_t* out_ts, unsigned char** out_data, size_t* out_data_len,
                      char* err_buf, size_t err_buf_len);

/* 鈹€鈹€鈹€ Query Iterator 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€ */

/**
 * Query records in a time range.
 *
 * The iterator owns a snapshot of matching index entries captured at query
 * creation time. Data payload is still read lazily by tmsl_iter_next.
 * @param dataset      Opaque dataset pointer.
 * @param start_ts     Start timestamp (inclusive).
 * @param end_ts       End timestamp (inclusive).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque iterator pointer, or NULL on error.
 */
void* tmsl_dataset_query(void* dataset, int64_t start_ts, int64_t end_ts,
                         char* err_buf, size_t err_buf_len);

/**
 * Get the next record from the iterator.
 * @param iter         Opaque iterator pointer.
 * @param out_ts       Output: timestamp.
 * @param out_data     Output: pointer to data (malloc'd, must be freed).
 * @param out_data_len Output: data length.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 = success (data available), 1 = exhausted, -1 = error.
 */
int tmsl_iter_next(void* iter, int64_t* out_ts, unsigned char** out_data,
                   size_t* out_data_len, char* err_buf, size_t err_buf_len);

/**
 * Free data returned by tmsl_dataset_read, tmsl_iter_next, or tmsl_queue_poll.
 * @param data         Pointer returned by a timslite FFI read/query/queue API.
 */
void tmsl_data_free(void* data);

/**
 * Free data returned by tmsl_iter_next.
 * Compatibility alias for tmsl_data_free.
 * @param data         Pointer from tmsl_iter_next.
 */
void tmsl_iter_free_data(unsigned char* data);

/**
 * Close and free an iterator.
 * @param iter         Opaque iterator pointer.
 */
void tmsl_iter_close(void* iter);

/* Queue API */

/**
 * Open the queue subsystem for a dataset.
 *
 * The dataset argument is the opaque pointer returned by tmsl_dataset_create or
 * tmsl_dataset_open. This function also works for the read-only .journal/logs
 * dataset when journal is enabled.
 *
 * @param dataset      Opaque dataset pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque queue handle (>0), or 0 on error.
 */
size_t tmsl_queue_open(void* dataset, char* err_buf, size_t err_buf_len);

/**
 * Close a queue handle.
 *
 * For normal dataset queues, this closes the underlying dataset queue. For the
 * journal queue, this only releases the FFI handle because the queue is owned by
 * the internal JournalManager. All FFI consumer handles opened from this queue
 * are invalidated.
 *
 * @param queue_handle Queue handle returned by tmsl_queue_open.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_queue_close(size_t queue_handle, char* err_buf, size_t err_buf_len);

/**
 * Open or create a queue consumer group.
 *
 * group_name must match ^[0-9A-Za-z_-]+$ and be at most 255 bytes.
 *
 * @param queue_handle Queue handle returned by tmsl_queue_open.
 * @param group_name   Consumer group name.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque consumer handle (>0), or 0 on error.
 */
size_t tmsl_queue_consumer_open(size_t queue_handle, const char* group_name,
                                char* err_buf, size_t err_buf_len);

/**
 * Drop a consumer group and invalidate matching FFI consumer handles.
 *
 * @param queue_handle    Queue handle returned by tmsl_queue_open.
 * @param consumer_handle Consumer handle returned by tmsl_queue_consumer_open.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_queue_consumer_drop(size_t queue_handle, size_t consumer_handle,
                             char* err_buf, size_t err_buf_len);

/**
 * Push data into a normal dataset queue.
 *
 * This assigns timestamp = latest_written_timestamp + 1. Journal queues reject
 * external push and return -1.
 *
 * @param queue_handle Queue handle returned by tmsl_queue_open.
 * @param data         Data bytes.
 * @param data_len     Data length.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Assigned timestamp on success, -1 on error.
 */
int64_t tmsl_queue_push(size_t queue_handle, const unsigned char* data,
                        size_t data_len, char* err_buf, size_t err_buf_len);

/**
 * Poll data from a queue consumer.
 *
 * On success, allocates out_data via malloc. Caller must free it with
 * tmsl_data_free.
 *
 * @param consumer_handle Consumer handle returned by tmsl_queue_consumer_open.
 * @param timeout_ms      Timeout in milliseconds. <=0 performs a nonblocking poll.
 * @param out_timestamp   Output timestamp.
 * @param out_data        Output data pointer.
 * @param out_data_len    Output data length.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 = success, -2 = timeout/no data, -1 = error.
 */
int tmsl_queue_poll(size_t consumer_handle, int64_t timeout_ms,
                    int64_t* out_timestamp, unsigned char** out_data,
                    size_t* out_data_len, char* err_buf, size_t err_buf_len);

/**
 * Ack a previously polled queue record.
 *
 * @param consumer_handle Consumer handle returned by tmsl_queue_consumer_open.
 * @param timestamp       Timestamp to ack.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_queue_ack(size_t consumer_handle, int64_t timestamp,
                   char* err_buf, size_t err_buf_len);

#ifdef __cplusplus
}
#endif

#endif /* TIMSLITE_H */
