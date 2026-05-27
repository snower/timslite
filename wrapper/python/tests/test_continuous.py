"""Continuous mode tests: out-of-order writes, backfill, gaps."""

import pytest
import timslite


class TestContinuous:
    def test_continuous_out_of_order_write(self, tmpdir):
        """Continuous mode allows out-of-order writes."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("cont", "data", index_continuous=True)
            ds = store.open_dataset("cont", "data")
            ds.write(100, b"first")
            ds.write(200, b"last")
            ds.write(50, b"earlier")
            ds.write(150, b"middle")
            results = ds.query_all(1, 300)
            assert len(results) == 4

    def test_continuous_gap_filling(self, tmpdir):
        """Continuous mode: query returns only real entries, fillers auto-filtered."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("gap", "data", index_continuous=True)
            ds = store.open_dataset("gap", "data")
            ds.write(100, b"start")
            ds.write(150, b"end")
            results = ds.query_all(1, 200)
            assert len(results) == 2
            assert results[0] == (100, b"start")
            assert results[1] == (150, b"end")

    def test_continuous_backfill_replaces_filler(self, tmpdir):
        """Back-fill an existing gap, verify the entry replaces the filler."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("bf", "data", index_continuous=True)
            ds = store.open_dataset("bf", "data")
            ds.write(100, b"a")
            ds.write(200, b"c")
            ds.write(150, b"b")  # fills gap between 100 and 200
            results = ds.query_all(1, 300)
            assert len(results) == 3
            ts_list = [r[0] for r in results]
            assert ts_list == sorted(ts_list)

    def test_continuous_duplicate_timestamp_rejected(self, tmpdir):
        """Continuous mode: duplicate timestamp still raises error."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("dup", "data", index_continuous=True)
            ds = store.open_dataset("dup", "data")
            ds.write(100, b"first")
            with pytest.raises(timslite.TmslInvalidDataError):
                ds.write(100, b"duplicate")
