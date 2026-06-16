"""Write and query pattern tests."""

import pytest
import timslite


class TestWriteQuery:
    def test_single_write_query(self, tmpdir):
        """Write 1 record, query returns 1 record."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("single", "data")
            ds = store.open_dataset("single", "data")
            ds.write(1, b"only_one")
            results = ds.query_all(1, 1)
            assert len(results) == 1
            assert results[0] == (1, b"only_one")

    def test_append_latest_record(self, tmpdir):
        """append() creates a new record, then appends to the latest record."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("append", "data")
            ds = store.open_dataset("append", "data")
            ds.append(1, b"abc")
            ds.append(1, b"de")
            assert ds.read(1) == (1, b"abcde")

    def test_multiple_write_query_range(self, tmpdir):
        """Write 100 records, query(3, 7) returns 5."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("range", "data")
            ds = store.open_dataset("range", "data")
            for i in range(1, 101):
                ds.write(i, f"record_{i}".encode())
            results = ds.query_all(3, 7)
            assert len(results) == 5
            assert results[0] == (3, b"record_3")
            assert results[-1] == (7, b"record_7")

    def test_query_empty_range(self, tmpdir):
        """Query a range with no data returns empty iterator."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("empty", "data")
            ds = store.open_dataset("empty", "data")
            ds.write(100, b"data")
            results = ds.query_all(1, 50)
            assert len(results) == 0

    def test_query_all_convenience(self, tmpdir):
        """query_all() produces identical results to list(query())."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("cmp", "data")
            ds = store.open_dataset("cmp", "data")
            for i in range(1, 11):
                ds.write(i, f"d_{i}".encode())
            all_results = ds.query_all(1, 10)
            iter_results = list(ds.query(1, 10))
            assert all_results == iter_results

    def test_iterator_protocol(self, tmpdir):
        """for ts, data in ds.query(...) correctly enumerates."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("iter", "data")
            ds = store.open_dataset("iter", "data")
            ds.write(1, b"a")
            ds.write(2, b"b")
            ds.write(3, b"c")
            collected = []
            for ts, data in ds.query(1, 10):
                collected.append((ts, data))
            assert collected == [(1, b"a"), (2, b"b"), (3, b"c")]

    def test_iterator_partial_consumption(self, tmpdir):
        """Consume half the iterator, then discard — no crash."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("partial", "data")
            ds = store.open_dataset("partial", "data")
            for i in range(1, 11):
                ds.write(i, f"d_{i}".encode())
            it = ds.query(1, 10)
            assert it.__next__() is not None
            assert it.__next__() is not None
            # Iterator goes out of scope — no error

    def test_write_zero_and_negative_timestamps(self, tmpdir):
        """write() accepts zero and negative signed timestamps."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("neg_ts", "data")
            ds = store.open_dataset("neg_ts", "data")
            ds.write(-1, b"negative")
            ds.write(0, b"zero")
            assert ds.read(-1) == (-1, b"negative")
            assert ds.read(0) == (0, b"zero")

    def test_write_out_of_order_succeeds(self, tmpdir):
        """Non-continuous mode: out-of-order write overwrites existing entry (Phase 18)."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("order", "data")
            ds = store.open_dataset("order", "data")
            ds.write(10, b"v1")
            ds.write(15, b"v2")
            ds.write(20, b"v3")
            ds.write(15, b"updated")  # out-of-order overwrite of existing ts=15
            results = ds.query_all(1, 30)
            assert len(results) == 3
            for ts, data in results:
                if ts == 15:
                    assert data == b"updated"
                if ts == 10:
                    assert data == b"v1"
                if ts == 20:
                    assert data == b"v3"

    def test_flush_manual(self, tmpdir):
        """Manual flush after write persists data."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("flush", "data")
            ds = store.open_dataset("flush", "data")
            ds.write(1, b"needs_flush")
            ds.flush()
            results = ds.query_all(1, 1)
            assert len(results) == 1


class TestExtendedAPI:
    def test_delete_removes_record(self, tmpdir):
        """ds.delete(timestamp) marks the record as deleted, query skips it."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("del_ds", "data")
            ds = store.open_dataset("del_ds", "data")
            ds.write(10, b"v1")
            ds.write(20, b"v2")
            ds.write(30, b"v3")
            ds.delete(20)
            results = ds.query_all(1, 100)
            assert len(results) == 2
            timestamps = [ts for ts, _ in results]
            assert 20 not in timestamps

    def test_delete_nonexistent_raises(self, tmpdir):
        """Deleting a non-existent timestamp raises TmslNotFoundError."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("del_ne", "data")
            ds = store.open_dataset("del_ne", "data")
            ds.write(1, b"x")
            with pytest.raises(timslite.TmslNotFoundError):
                ds.delete(999)

    def test_read_latest_record(self, tmpdir):
        """ds.read_latest() returns the latest written record."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("latest", "data")
            ds = store.open_dataset("latest", "data")
            ds.write(10, b"first")
            ds.write(20, b"second")
            ds.write(30, b"third")
            result = ds.read_latest()
            assert result is not None
            ts, data = result
            assert ts == 30
            assert data == b"third"
            assert ds.read(-1) is None

    def test_read_latest_empty_dataset(self, tmpdir):
        """ds.read_latest() on empty dataset returns None."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("latest_empty", "data")
            ds = store.open_dataset("latest_empty", "data")
            assert ds.latest_timestamp is None
            assert ds.read_latest() is None
            assert ds.read(-1) is None

    def test_signed_timestamp_reads_are_exact(self, tmpdir):
        """Negative timestamps and zero are normal public timestamps."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("signed_ts", "data")
            ds = store.open_dataset("signed_ts", "data")
            ds.write(-1, b"minus-one")
            ds.write(0, b"zero")
            ds.write(1, b"one")
            assert ds.read(-1) == (-1, b"minus-one")
            assert ds.read(0) == (0, b"zero")
            assert ds.read_latest() == (1, b"one")
            assert ds.latest_timestamp == 1

    def test_correction_write_non_continuous(self, tmpdir):
        """Non-continuous mode: out-of-order write overwrites existing entry."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("correction", "data")
            ds = store.open_dataset("correction", "data")
            ds.write(10, b"original")
            ds.write(20, b"other")
            # Correction write: overwrite ts=10 with new data
            ds.write(10, b"corrected")
            result = ds.read(10)
            assert result is not None
            assert result[1] == b"corrected"

    def test_append_to_old_timestamp_fails(self, tmpdir):
        """Appending to an older timestamp in non-continuous mode should fail."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("old_append", "data")
            ds = store.open_dataset("old_append", "data")
            ds.write(10, b"first")
            ds.write(20, b"second")
            # append to ts=10 which is not the latest — should fail
            with pytest.raises(Exception):
                ds.append(10, b"should_fail")
