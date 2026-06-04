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

    def test_write_timestamp_zero_rejected(self, tmpdir):
        """write(0, ...) raises TmslInvalidDataError."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("bad_ts", "data")
            ds = store.open_dataset("bad_ts", "data")
            with pytest.raises(timslite.TmslInvalidDataError):
                ds.write(0, b"bad")

    def test_write_negative_timestamp_rejected(self, tmpdir):
        """write(-1, ...) raises TmslInvalidDataError."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("neg_ts", "data")
            ds = store.open_dataset("neg_ts", "data")
            with pytest.raises(timslite.TmslInvalidDataError):
                ds.write(-1, b"bad")

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
