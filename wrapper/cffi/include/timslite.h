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

#define TMSL_STORE_CONFIG_FFI_VERSION 5u
#define TMSL_DATASET_CONFIG_FFI_VERSION 3u
#define TMSL_QUEUE_CONSUMER_CONFIG_FFI_VERSION 1u

#define TMSL_STORE_READ_ONLY_AUTO 0u  /**< Auto: writable if .lock can be locked, otherwise read-only */
#define TMSL_STORE_READ_ONLY_FALSE 1u /**< Require writable .lock, fail if already locked */
#define TMSL_STORE_READ_ONLY_TRUE 2u  /**< Force read-only, do not check or take .lock */

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
    uint8_t read_only_mode; /* TMSL_STORE_READ_ONLY_*; default AUTO */
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
    uint8_t enable_journal; /* 0=false, non-zero=true; default true */
} TmslDatasetConfigFFI;

typedef struct TmslQueueConsumerConfigFFI {
    uint32_t version;
    uint32_t running_expired_seconds; /* 0=never expire while running, default 900, max 65535 */
    uint32_t max_retry_count;         /* 0=unlimited, default 3, max 255 */
} TmslQueueConsumerConfigFFI;

typedef void (*TmslQueuePollCallback)(void* userdata);

typedef struct TmslLengthEntry {
    int64_t timestamp;
    uint32_t data_len;
} TmslLengthEntry;

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

/* ─── Store Dataset Enumeration ──────────────────────────────────────── */

/**
 * Free a malloc'd array of C strings returned by tmsl_store_get_dataset_names
 * or tmsl_store_get_dataset_types.
 * @param arr    Pointer to the array returned by the enumeration function.
 * @param count  Number of elements in the array.
 */
void tmsl_free_string_array(char** arr, uint32_t count);

/**
 * Get all unique dataset names in the store.
 * @param store      Opaque store pointer.
 * @param out_names  Written with a malloc'd array of malloc'd C strings.
 * @param out_count  Written with the number of names.
 * @param err_buf    Buffer for error message.
 * @param err_buf_len Length of error buffer.
 * @return 0 on success, -1 on error. Caller must free with tmsl_free_string_array.
 */
int tmsl_store_get_dataset_names(void* store,
                                  char*** out_names,
                                  uint32_t* out_count,
                                  char* err_buf, size_t err_buf_len);

/**
 * Get all dataset types for a given name.
 * @param store      Opaque store pointer.
 * @param name       Dataset name to query.
 * @param out_types  Written with a malloc'd array of malloc'd C strings.
 * @param out_count  Written with the number of types.
 * @param err_buf    Buffer for error message.
 * @param err_buf_len Length of error buffer.
 * @return 0 on success, -1 on error. Caller must free with tmsl_free_string_array.
 */
int tmsl_store_get_dataset_types(void* store,
                                  const char* name,
                                  char*** out_types,
                                  uint32_t* out_count,
                                  char* err_buf, size_t err_buf_len);

/**
 * Create a new dataset (errors if already exists).
 * @param store                Opaque store pointer.
 * @param name                 Dataset name, must match ^[0-9A-Za-z_-]+$.
 * @param dataset_type         Dataset type, must match ^[0-9A-Za-z_-]+$.
 * @param data_segment_size    Data segment size in bytes.
 * @param index_segment_size   Index segment size in bytes.
 * @param compress_level       Compression level (0-9), interpreted by the selected algorithm.
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
 * Open an existing dataset by its Store-assigned numeric identifier.
 * @param store       Opaque store pointer.
 * @param identifier  Dataset identifier (> 0).
 * @param err_buf     Buffer for error message.
 * @param err_buf_len Length of error buffer.
 * @return Opaque dataset pointer, or NULL on error.
 */
void* tmsl_dataset_open_by_identifier(void* store, uint64_t identifier,
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
 * @param out_ts       Output: maximum written timestamp (valid when return=0).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0=success, 1=empty dataset, -1=error.
 */
int tmsl_dataset_latest_timestamp(void* dataset, int64_t* out_ts,
                                  char* err_buf, size_t err_buf_len);

/**
 * Get the Store-assigned numeric identifier of a dataset.
 * @param dataset        Opaque dataset pointer.
 * @param out_identifier Output: dataset identifier.
 * @param err_buf        Buffer for error message.
 * @param err_buf_len    Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_identifier(void* dataset, uint64_t* out_identifier,
                            char* err_buf, size_t err_buf_len);

/**
 * Write a record to a dataset.
 *
 * Supports three timestamp modes:
 *   - correction: timestamp == latest -> overwrite data in place (index unchanged)
 *   - out-of-order: timestamp < latest -> append to latest segment + update index
 *     entry in place; the old data segment's invalid_record_count is incremented
 *     if the previous entry referenced real data; its global cache entry is invalidated
 *   - in-order: timestamp > latest -> append; continuous mode fills gaps with filler
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

/**
 * Read a single record by exact timestamp.
 *
 * On success (record found): allocates `out_data` via malloc, sets `out_ts`
 * (the actual timestamp of the record) and `out_data_len`. Caller must free
 * `out_data` via `tmsl_data_free`.
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Exact signed business timestamp of the record to read.
 * @param out_ts       Output: actual timestamp of the record returned.
 * @param out_data     Output: pointer to data (malloc'd, must be freed via tmsl_data_free).
 * @param out_data_len Output: data length in bytes.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 = success, 1 = not found (or filler/deleted/expired), -1 = error.
 */
int tmsl_dataset_read(void* dataset, int64_t timestamp,
                      int64_t* out_ts, unsigned char** out_data, size_t* out_data_len,
                      char* err_buf, size_t err_buf_len);

/**
 * Read the record at the maximum written timestamp.
 *
 * Does not search backward for the latest valid record. If the dataset is empty
 * or the maximum timestamp entry has been deleted, returns 1 (not found).
 *
 * @param dataset      Opaque dataset pointer.
 * @param out_ts       Output: actual timestamp of the record returned.
 * @param out_data     Output: pointer to data (malloc'd, must be freed via tmsl_data_free).
 * @param out_data_len Output: data length in bytes.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 = success, 1 = not found/empty/deleted latest, -1 = error.
 */
int tmsl_dataset_read_latest(void* dataset,
                             int64_t* out_ts, unsigned char** out_data, size_t* out_data_len,
                             char* err_buf, size_t err_buf_len);

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
 * Free data returned by tmsl_dataset_read, tmsl_dataset_read_latest,
 * tmsl_iter_next, or tmsl_queue_poll.
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

/* Lightweight Read Operations */

/**
 * Check if visible data exists for a timestamp.
 * timestamp is an exact signed business timestamp.
 * Expired timestamps and filler/deleted entries return false.
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp to check.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0=false, 1=true, -1=error.
 */
int tmsl_dataset_read_exist(void* dataset, int64_t timestamp,
                            char* err_buf, size_t err_buf_len);

/**
 * Check visible data existence in [start_ts, end_ts].
 * Returns bitmap via out_bitmap (allocated with malloc, caller frees with tmsl_data_free).
 * Bit i represents (start_ts + i): 1=visible data exists, 0=not found/expired/filler.
 * The returned bitmap is capped at 4 MiB; larger ranges return an error.
 * @param dataset      Opaque dataset pointer.
 * @param start_ts     Start timestamp (inclusive).
 * @param end_ts       End timestamp (inclusive).
 * @param out_bitmap   Output: pointer to bitmap data.
 * @param out_bitmap_len Output: bitmap byte count.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_query_exist(void* dataset, int64_t start_ts, int64_t end_ts,
                             unsigned char** out_bitmap, size_t* out_bitmap_len,
                             char* err_buf, size_t err_buf_len);

/**
 * Read the logical data length for a timestamp.
 * timestamp is an exact signed business timestamp.
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp to read.
 * @param out_len      Output: data length (valid when return=0).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0=success (out_len valid), 1=not found, -1=error.
 */
int tmsl_dataset_read_length(void* dataset, int64_t timestamp,
                             uint32_t* out_len,
                             char* err_buf, size_t err_buf_len);

/**
 * Query data lengths for timestamps in [start_ts, end_ts].
 * Returns array of TmslLengthEntry values via out_array (allocated with malloc).
 * TmslLengthEntry uses normal C struct layout, not packed layout:
 * sizeof(TmslLengthEntry) == 16 and alignment == 8 on supported ABIs.
 * out_array_len is the number of TmslLengthEntry elements, not a byte count.
 * The trailing padding bytes after data_len are not data.
 * Caller frees with tmsl_data_free.
 * @param dataset      Opaque dataset pointer.
 * @param start_ts     Start timestamp (inclusive).
 * @param end_ts       End timestamp (inclusive).
 * @param out_array    Output: pointer to TmslLengthEntry array.
 * @param out_array_len Output: element count.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_query_length(void* dataset, int64_t start_ts, int64_t end_ts,
                              TmslLengthEntry** out_array, size_t* out_array_len,
                              char* err_buf, size_t err_buf_len);

/**
 * Create a query length iterator for lazy data length iteration.
 * @param dataset      Opaque dataset pointer.
 * @param start_ts     Start timestamp (inclusive).
 * @param end_ts       End timestamp (inclusive).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque iterator pointer, or NULL on error.
 */
void* tmsl_dataset_query_length_iter(void* dataset, int64_t start_ts, int64_t end_ts,
                                     char* err_buf, size_t err_buf_len);

/**
 * Get next data length from query length iterator.
 * @param iter         Opaque iterator pointer.
 * @param out_ts       Output: timestamp (valid when return=0).
 * @param out_len      Output: data length (valid when return=0).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0=success, 1=done, -1=error.
 */
int tmsl_length_iter_next(void* iter, int64_t* out_ts, uint32_t* out_len,
                          char* err_buf, size_t err_buf_len);

/**
 * Close and free a length iterator returned by tmsl_dataset_query_length_iter.
 * @param iter         Opaque length iterator pointer.
 */
void tmsl_length_iter_close(void* iter);

/* Queue API */

/**
 * Open the queue subsystem for a dataset.
 *
 * The dataset argument is the opaque pointer returned by tmsl_dataset_create or
 * tmsl_dataset_open. Journal queues use the dedicated tmsl_journal_queue_* API.
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
 * Closes the underlying dataset queue and invalidates all FFI consumer handles
 * opened from this queue.
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
 * Open or create a queue consumer group with explicit retry configuration.
 *
 * Passing config = NULL is equivalent to tmsl_queue_consumer_open.
 *
 * @param queue_handle Queue handle returned by tmsl_queue_open.
 * @param group_name   Consumer group name.
 * @param config       Versioned config pointer, or NULL for defaults.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque consumer handle (>0), or 0 on error.
 */
size_t tmsl_queue_consumer_open_with_config(size_t queue_handle,
                                            const char* group_name,
                                            const TmslQueueConsumerConfigFFI* config,
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
 * This assigns timestamp = latest_written_timestamp + 1.
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

/**
 * Register or clear a lightweight wake callback for a queue consumer.
 *
 * The callback is invoked synchronously after data waiters are notified. It is
 * best-effort, may be skipped or repeated, and must only wake external
 * processing. Pass callback = NULL to clear the callback.
 *
 * @param consumer_handle Consumer handle returned by tmsl_queue_consumer_open.
 * @param callback        Wake callback, or NULL to clear. Passing a non-NULL
 *                        callback while one is already registered for this
 *                        consumer returns -1 and keeps the old callback.
 * @param userdata        Opaque pointer passed to callback.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_queue_consumer_poll_callback(size_t consumer_handle,
                                      TmslQueuePollCallback callback,
                                      void* userdata,
                                      char* err_buf, size_t err_buf_len);

/* Journal API */

/**
 * Return the latest journal sequence.
 *
 * The journal sequence is one-based. On success, out_sequence is set to 0 when
 * the journal is empty.
 *
 * @param store        Opaque store pointer.
 * @param out_sequence Output latest sequence, or 0 when empty.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_journal_latest_sequence(void* store, int64_t* out_sequence,
                                 char* err_buf, size_t err_buf_len);

/**
 * Read one encoded journal record by sequence.
 *
 * On success, allocates out_data via malloc. Caller must free it with
 * tmsl_data_free.
 *
 * @param store          Opaque store pointer.
 * @param sequence       Journal sequence to read.
 * @param out_sequence   Output actual sequence.
 * @param out_data       Output encoded journal record.
 * @param out_data_len   Output record length.
 * @param err_buf        Buffer for error message.
 * @param err_buf_len    Length of error buffer.
 * @return 0 = found, 1 = not found, -1 = error.
 */
int tmsl_journal_read(void* store, int64_t sequence, int64_t* out_sequence,
                      unsigned char** out_data, size_t* out_data_len,
                      char* err_buf, size_t err_buf_len);

/**
 * Query encoded journal records by inclusive sequence range.
 *
 * @param store          Opaque store pointer.
 * @param start_sequence Start sequence, inclusive.
 * @param end_sequence   End sequence, inclusive.
 * @param err_buf        Buffer for error message.
 * @param err_buf_len    Length of error buffer.
 * @return Opaque journal iterator pointer, or NULL on error.
 */
void* tmsl_journal_query(void* store, int64_t start_sequence, int64_t end_sequence,
                         char* err_buf, size_t err_buf_len);

/**
 * Get the next encoded journal record from a journal iterator.
 *
 * On success, allocates out_data via malloc. Caller must free it with
 * tmsl_data_free.
 *
 * @param iter           Opaque journal iterator pointer.
 * @param out_sequence   Output journal sequence.
 * @param out_data       Output encoded journal record.
 * @param out_data_len   Output record length.
 * @param err_buf        Buffer for error message.
 * @param err_buf_len    Length of error buffer.
 * @return 0 = success, 1 = done, -1 = error.
 */
int tmsl_journal_iter_next(void* iter, int64_t* out_sequence,
                           unsigned char** out_data, size_t* out_data_len,
                           char* err_buf, size_t err_buf_len);

/**
 * Close and free a journal iterator.
 */
void tmsl_journal_iter_close(void* iter);

/**
 * Open the built-in journal queue.
 *
 * @param store        Opaque store pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque journal queue handle (>0), or 0 on error.
 */
size_t tmsl_journal_queue_open(void* store, char* err_buf, size_t err_buf_len);

/**
 * Close a journal queue handle and invalidate its consumers.
 *
 * @param queue_handle Journal queue handle.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_journal_queue_close(size_t queue_handle, char* err_buf, size_t err_buf_len);

/**
 * Open or create a journal queue consumer group.
 *
 * group_name must match ^[0-9A-Za-z_-]+$ and be at most 255 bytes.
 *
 * @param queue_handle Journal queue handle.
 * @param group_name   Consumer group name.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque consumer handle (>0), or 0 on error.
 */
size_t tmsl_journal_queue_consumer_open(size_t queue_handle, const char* group_name,
                                        char* err_buf, size_t err_buf_len);

/**
 * Open or create a journal queue consumer group with explicit retry configuration.
 *
 * Passing config = NULL is equivalent to tmsl_journal_queue_consumer_open.
 *
 * @param queue_handle Journal queue handle.
 * @param group_name   Consumer group name.
 * @param config       Versioned config pointer, or NULL for defaults.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque consumer handle (>0), or 0 on error.
 */
size_t tmsl_journal_queue_consumer_open_with_config(
    size_t queue_handle,
    const char* group_name,
    const TmslQueueConsumerConfigFFI* config,
    char* err_buf,
    size_t err_buf_len);

/**
 * Poll data from a journal queue consumer.
 *
 * On success, allocates out_data via malloc. Caller must free it with
 * tmsl_data_free.
 *
 * @param consumer_handle Journal consumer handle.
 * @param timeout_ms      Timeout in milliseconds. <=0 performs a nonblocking poll.
 * @param out_sequence    Output journal sequence.
 * @param out_data        Output encoded journal record.
 * @param out_data_len    Output record length.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 = success, -2 = timeout/no data, -1 = error.
 */
int tmsl_journal_queue_poll(size_t consumer_handle, int64_t timeout_ms,
                            int64_t* out_sequence, unsigned char** out_data,
                            size_t* out_data_len, char* err_buf, size_t err_buf_len);

/**
 * Ack a previously polled journal queue record.
 *
 * @param consumer_handle Journal consumer handle.
 * @param sequence        Journal sequence to ack.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_journal_queue_ack(size_t consumer_handle, int64_t sequence,
                           char* err_buf, size_t err_buf_len);

/**
 * Register or clear a lightweight wake callback for a journal queue consumer.
 *
 * The callback is invoked synchronously after journal data waiters are
 * notified. It is best-effort, may be skipped or repeated, and must only wake
 * external processing. Pass callback = NULL to clear the callback.
 *
 * @param consumer_handle Journal consumer handle.
 * @param callback        Wake callback, or NULL to clear. Passing a non-NULL
 *                        callback while one is already registered for this
 *                        consumer returns -1 and keeps the old callback.
 * @param userdata        Opaque pointer passed to callback.
 * @param err_buf         Buffer for error message.
 * @param err_buf_len     Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_journal_queue_consumer_poll_callback(size_t consumer_handle,
                                              TmslQueuePollCallback callback,
                                              void* userdata,
                                              char* err_buf, size_t err_buf_len);

/* ─── Dataset Inspect ─────────────────────────────────────────────────── */

/**
 * Dataset immutable configuration info.
 */
typedef struct TmslDataSetInfo {
    char* name;                       /**< Dataset name (caller must free) */
    char* dataset_type;               /**< Dataset type (caller must free) */
    char* base_dir;                   /**< Dataset directory path (caller must free) */
    uint64_t identifier;              /**< Store-assigned numeric dataset identifier */
    uint64_t data_segment_size;       /**< Data segment file size limit (bytes) */
    uint64_t index_segment_size;      /**< Index segment file size limit (bytes) */
    uint64_t initial_data_segment_size; /**< Initial data segment file size (bytes) */
    uint64_t initial_index_segment_size; /**< Initial index segment file size (bytes) */
    uint8_t compress_type;            /**< Compression algorithm type (0=zstd, 1=deflate) */
    uint8_t compress_level;           /**< Compression level (0-9) */
    uint8_t index_continuous;         /**< Index mode: 0=sparse, 1=continuous */
    uint64_t retention_window;        /**< Data retention window (0=no limit) */
    uint8_t enable_journal;           /**< Whether this dataset records journal entries */
    int64_t create_time;              /**< Dataset creation time (Unix milliseconds) */
} TmslDataSetInfo;

/**
 * Dataset mutable state info.
 */
typedef struct TmslDataSetState {
    uint8_t has_latest_written_timestamp; /**< Whether latest_written_timestamp is valid */
    int64_t latest_written_timestamp; /**< Highest written timestamp */
    uint32_t open_data_segments;      /**< Number of currently open data segments */
    uint32_t data_segments;           /**< Total number of data segments */
    uint64_t total_record_count;      /**< Total record count across all data segments */
    uint64_t total_data_size;         /**< Total used space across all data segments (bytes) */
    uint64_t total_uncompressed_size; /**< Total uncompressed size across all data segments (bytes) */
    uint64_t total_invalid_record_count; /**< Total invalid record count across all data segments */
    uint8_t has_min_timestamp;        /**< Whether min_timestamp is valid */
    int64_t min_timestamp;            /**< Global minimum timestamp from the index-visible range */
    uint8_t has_max_timestamp;        /**< Whether max_timestamp is valid */
    int64_t max_timestamp;            /**< Global maximum timestamp from the index-visible range */
    uint32_t open_index_segments;     /**< Number of currently open index segments */
    uint32_t index_segments;          /**< Total number of index segments */
    uint32_t pending_index_entries;   /**< Number of in-memory buffered index entries */
    uint8_t has_base_timestamp;       /**< Whether base_timestamp is valid */
    int64_t base_timestamp;           /**< Index base timestamp */
    uint8_t read_only;                /**< Whether the dataset is in read-only mode */
    uint8_t has_block_cache;          /**< Whether BlockCache is enabled */
    uint8_t has_journal;              /**< Whether Journal is enabled */
    uint8_t has_queue;                /**< Whether the dataset has an associated Queue */
    uint32_t queue_consumer_groups;   /**< Number of queue consumer groups */
} TmslDataSetState;

/**
 * Dataset inspect result.
 */
typedef struct TmslInspectResult {
    TmslDataSetInfo info;   /**< Immutable configuration info */
    TmslDataSetState state; /**< Mutable current state */
} TmslInspectResult;

/**
 * Get detailed info and state of a dataset.
 *
 * On success writes the inspect result to `out_result`. Caller must free with
 * `tmsl_free_inspect_result`.
 *
 * @param store         Opaque store pointer.
 * @param name          Dataset name.
 * @param dataset_type  Dataset type.
 * @param out_result    Written with the inspect result.
 * @param err_buf       Buffer for error message.
 * @param err_buf_len   Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_store_inspect_dataset(void* store,
                                const char* name,
                                const char* dataset_type,
                                TmslInspectResult* out_result,
                                char* err_buf, size_t err_buf_len);

/**
 * Free the memory allocated by `tmsl_store_inspect_dataset`.
 *
 * This frees the strings in `info` (name, dataset_type, base_dir).
 *
 * @param result  Pointer to the inspect result to free.
 */
void tmsl_free_inspect_result(TmslInspectResult* result);

#ifdef __cplusplus
}
#endif

#endif /* TIMSLITE_H */
