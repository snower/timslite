/**
 * timslite - Time-Series Data Storage Library (C Header)
 *
 * A high-performance, mmap-backed time-series data store with:
 * - Block-level aggregation (max 64KB per block)
 * - Delayed compression (seal on overflow or idle-close)
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

/* ─── Store Management ──────────────────────────────────────────────────── */

/**
 * Open a store at the given directory.
 * @param data_dir     Path to data directory.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return Opaque store pointer, or NULL on error.
 */
void* tmsl_store_open(const char* data_dir, char* err_buf, size_t err_buf_len);

/**
 * Close a store and release all resources.
 * @param store        Opaque store pointer.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_store_close(void* store, char* err_buf, size_t err_buf_len);

/* ─── Dataset Management ────────────────────────────────────────────────── */

/**
 * Create a new dataset (errors if already exists).
 * @param store                Opaque store pointer.
 * @param name                 Dataset name.
 * @param dataset_type         Dataset type.
 * @param data_segment_size    Data segment size in bytes.
 * @param index_segment_size   Index segment size in bytes.
 * @param compress_level       Compression level (1-9).
 * @param index_continuous     0 = non-continuous (strict order), 1 = continuous (filler entries).
 * @param retention_ms         Data validity period (same unit as timestamp, 0 = no limit).
 *                             Store-level `retention_check_hour` controls daily reclaim schedule.
 * @param err_buf              Buffer for error message.
 * @param err_buf_len          Length of error buffer.
 * @return Opaque dataset pointer, or NULL on error.
 */
void* tmsl_dataset_create(void* store, const char* name, const char* dataset_type,
                          uint64_t data_segment_size, uint64_t index_segment_size,
                          unsigned char compress_level, unsigned char index_continuous,
                          uint64_t retention_ms,
                          char* err_buf, size_t err_buf_len);

/**
 * Drop (delete) an entire dataset.
 * @param store        Opaque store pointer.
 * @param name         Dataset name.
 * @param dataset_type Dataset type.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_drop(void* store, const char* name, const char* dataset_type,
                      char* err_buf, size_t err_buf_len);

/**
 * Open an existing dataset (errors if not exists).
 * @param store        Opaque store pointer.
 * @param name         Dataset name.
 * @param dataset_type Dataset type.
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
 * Get the latest successfully written timestamp of a dataset.
 *
 * @param dataset      Opaque dataset pointer.
 * @param out_ts       Output: latest written timestamp (0 if the dataset is empty).
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_latest_timestamp(void* dataset, int64_t* out_ts,
                                  char* err_buf, size_t err_buf_len);

/* ─── Data Write ─────────────────────────────────────────────────────────── */

/**
 * Write a record to a dataset.
 *
 * Supports three timestamp modes:
 *   - correction: timestamp == latest → overwrite data in place (index unchanged)
 *   - out-of-order: timestamp < latest → append to latest segment + update index
 *     entry in place; the old data segment's invalid_record_count is incremented
 *     if the previous entry referenced real data
 *   - in-order: timestamp > latest → append; continuous mode fills gaps with filler
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
 * Delete the record at the given timestamp.
 *
 * Marks the index entry as sentinel (block_offset = 0xFFFFFFFFFFFFFFFF,
 * in_block_offset = 0xFFFF) and increments the old data segment's
 * invalid_record_count. The physical data is preserved on disk until
 * retention-based reclamation or future compaction.
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp of the record to delete.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error (e.g. not found or already deleted).
 */
int tmsl_dataset_delete(void* dataset, int64_t timestamp,
                        char* err_buf, size_t err_buf_len);

/* ─── Single Record Read ────────────────────────────────────────────────── */

/**
 * Read a single record by exact timestamp.
 *
 * On success (record found): allocates `out_data` via malloc, sets `out_ts`
 * (the actual timestamp of the record) and `out_data_len`. Caller must free
 * `out_data` via `tmsl_iter_free_data`.
 *
 * Shortcut: passing `timestamp = -1` resolves to the latest written timestamp
 * and returns the newest record. If the dataset is empty or the latest entry
 * has been deleted, returns 1 (not found).
 *
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp of the record to read, or -1 for the latest record.
 * @param out_ts       Output: actual timestamp of the record returned.
 * @param out_data     Output: pointer to data (malloc'd, must be freed via tmsl_iter_free_data).
 * @param out_data_len Output: data length in bytes.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 = success, 1 = not found (or filler/deleted/empty dataset with -1), -1 = error.
 */
int tmsl_dataset_read(void* dataset, int64_t timestamp,
                      int64_t* out_ts, unsigned char** out_data, size_t* out_data_len,
                      char* err_buf, size_t err_buf_len);

/* ─── Query Iterator ────────────────────────────────────────────────────── */

/**
 * Query records in a time range.
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
 * Free data returned by tmsl_iter_next.
 * @param data         Pointer from tmsl_iter_next.
 */
void tmsl_iter_free_data(unsigned char* data);

/**
 * Close and free an iterator.
 * @param iter         Opaque iterator pointer.
 */
void tmsl_iter_close(void* iter);

#ifdef __cplusplus
}
#endif

#endif /* TIMSLITE_H */
