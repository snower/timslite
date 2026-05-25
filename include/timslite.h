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
 * Open or create a dataset within a store.
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

/* ─── Data Write ─────────────────────────────────────────────────────────── */

/**
 * Write a record to a dataset.
 * @param dataset      Opaque dataset pointer.
 * @param timestamp    Timestamp (seconds since epoch).
 * @param data         Raw data bytes.
 * @param data_len     Length of data.
 * @param err_buf      Buffer for error message.
 * @param err_buf_len  Length of error buffer.
 * @return 0 on success, -1 on error.
 */
int tmsl_dataset_write(void* dataset, int64_t timestamp,
                       const unsigned char* data, size_t data_len,
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
