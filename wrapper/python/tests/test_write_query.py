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

    def test_write_now_and_append_now(self, tmpdir):
        """write_now and append_now use current Unix timestamp."""
        import time

        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("nowapi", "data")
            ds = store.open_dataset("nowapi", "data")

            before = int(time.time())
            ds.write_now(b"hello_now")
            after = int(time.time())

            result = ds.read_latest()
            assert result is not None
            ts, data = result
            assert data == b"hello_now"
            assert before <= ts <= after, f"write_now timestamp {ts} should be in [{before}, {after}]"

            # Test append_now
            ds.append_now(b"-appended")
            after_append = int(time.time())

            result = ds.read_latest()
            assert result is not None
            append_ts, append_data = result
            assert ts <= append_ts <= after_append
            if append_ts == ts:
                assert append_data == b"hello_now-appended"
            else:
                assert append_data == b"-appended"


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


class TestQueryIteratorMethods:
    """Tests for reverse(), skip(), collect_all(), collect_take() on QueryIterator and QueryLengthIterator."""

    def _write_records(self, ds, count=5):
        """Helper: write `count` records with timestamps 1..count."""
        for i in range(1, count + 1):
            ds.write(i, f"rec_{i}".encode())

    def test_query_reverse(self, tmpdir):
        """reverse() yields entries in descending timestamp order."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_rev", "data")
            ds = store.open_dataset("q_rev", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            it.reverse()
            collected = list(it)
            assert len(collected) == 5
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [5, 4, 3, 2, 1]
            for ts, data in collected:
                assert data == f"rec_{ts}".encode()

    def test_query_skip(self, tmpdir):
        """skip(2) skips the first 2 entries, returning the rest."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_skip", "data")
            ds = store.open_dataset("q_skip", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            it.skip(2)
            collected = list(it)
            assert len(collected) == 3
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [3, 4, 5]

    def test_query_collect_all(self, tmpdir):
        """collect_all() returns all entries as a list of (timestamp, data) tuples."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_ca", "data")
            ds = store.open_dataset("q_ca", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            collected = it.collect_all()
            assert len(collected) == 5
            for i, (ts, data) in enumerate(collected, 1):
                assert ts == i
                assert data == f"rec_{i}".encode()

    def test_query_collect_take(self, tmpdir):
        """collect_take(3) returns at most 3 entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_ct", "data")
            ds = store.open_dataset("q_ct", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            collected = it.collect_take(3)
            assert len(collected) == 3
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [1, 2, 3]

    def test_query_skip_and_reverse(self, tmpdir):
        """skip(1) then reverse() skips 1 from the front, then reverses remaining."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_sr", "data")
            ds = store.open_dataset("q_sr", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            it.skip(1)
            it.reverse()
            collected = list(it)
            assert len(collected) == 4
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [5, 4, 3, 2]

    def test_query_skip_more_than_available(self, tmpdir):
        """skip() more entries than exist yields an empty iterator."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_sma", "data")
            ds = store.open_dataset("q_sma", "data")
            self._write_records(ds, 3)
            it = ds.query(1, 3)
            it.skip(10)
            collected = list(it)
            assert collected == []

    def test_query_collect_all_empty(self, tmpdir):
        """collect_all() on an empty range returns an empty list."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_cae", "data")
            ds = store.open_dataset("q_cae", "data")
            ds.write(100, b"data")
            it = ds.query(1, 50)
            collected = it.collect_all()
            assert collected == []

    def test_query_collect_take_more_than_available(self, tmpdir):
        """collect_take(N) where N > total entries returns all entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_ctm", "data")
            ds = store.open_dataset("q_ctm", "data")
            self._write_records(ds, 3)
            it = ds.query(1, 3)
            collected = it.collect_take(100)
            assert len(collected) == 3
            for i, (ts, data) in enumerate(collected, 1):
                assert ts == i
                assert data == f"rec_{i}".encode()

    def test_query_length_reverse(self, tmpdir):
        """reverse() on QueryLengthIterator yields entries in descending order."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_rev", "data")
            ds = store.open_dataset("ql_rev", "data")
            self._write_records(ds, 5)
            it = ds.query_length(1, 5)
            it.reverse()
            collected = list(it)
            assert len(collected) == 5
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [5, 4, 3, 2, 1]
            for ts, length in collected:
                assert length > 0

    def test_query_length_skip(self, tmpdir):
        """skip(2) on QueryLengthIterator skips first 2 entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_skip", "data")
            ds = store.open_dataset("ql_skip", "data")
            self._write_records(ds, 5)
            it = ds.query_length(1, 5)
            it.skip(2)
            collected = list(it)
            assert len(collected) == 3
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [3, 4, 5]

    def test_query_length_collect_all(self, tmpdir):
        """collect_all() on QueryLengthIterator returns all (timestamp, length) entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_ca", "data")
            ds = store.open_dataset("ql_ca", "data")
            self._write_records(ds, 5)
            it = ds.query_length(1, 5)
            collected = it.collect_all()
            assert len(collected) == 5
            for i, (ts, length) in enumerate(collected, 1):
                assert ts == i
                assert length > 0

    def test_query_length_collect_take(self, tmpdir):
        """collect_take(3) on QueryLengthIterator returns at most 3 entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_ct", "data")
            ds = store.open_dataset("ql_ct", "data")
            self._write_records(ds, 5)
            it = ds.query_length(1, 5)
            collected = it.collect_take(3)
            assert len(collected) == 3
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [1, 2, 3]

    def test_query_length_skip_more_than_available(self, tmpdir):
        """skip() more than available on QueryLengthIterator yields empty."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_sma", "data")
            ds = store.open_dataset("ql_sma", "data")
            self._write_records(ds, 3)
            it = ds.query_length(1, 3)
            it.skip(10)
            collected = list(it)
            assert collected == []

    def test_query_length_collect_all_empty(self, tmpdir):
        """collect_all() on QueryLengthIterator for empty range returns []."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("ql_cae", "data")
            ds = store.open_dataset("ql_cae", "data")
            ds.write(100, b"data")
            it = ds.query_length(1, 50)
            collected = it.collect_all()
            assert collected == []

    def test_query_reverse_then_collect_take(self, tmpdir):
        """reverse() then collect_take(2) returns last 2 entries in descending order."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_rct", "data")
            ds = store.open_dataset("q_rct", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            it.reverse()
            collected = it.collect_take(2)
            assert len(collected) == 2
            assert collected[0] == (5, b"rec_5")
            assert collected[1] == (4, b"rec_4")

    def test_query_collect_all_twice_returns_empty(self, tmpdir):
        """Second collect_all() after exhaustion returns empty list (iterator consumed)."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_cat", "data")
            ds = store.open_dataset("q_cat", "data")
            self._write_records(ds, 3)
            it = ds.query(1, 3)
            first = it.collect_all()
            assert len(first) == 3
            second = it.collect_all()
            assert second == []

    def test_query_skip_then_collect_all(self, tmpdir):
        """skip(2) then collect_all() returns remaining entries."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("q_sca", "data")
            ds = store.open_dataset("q_sca", "data")
            self._write_records(ds, 5)
            it = ds.query(1, 5)
            it.skip(2)
            collected = it.collect_all()
            assert len(collected) == 3
            timestamps = [ts for ts, _ in collected]
            assert timestamps == [3, 4, 5]
