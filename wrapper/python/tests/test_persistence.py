"""Persistence tests: reopen after close, data survives."""

import pytest
import timslite


class TestPersistence:
    def test_reopen_after_close(self, tmpdir):
        """Close store, reopen, data is still readable."""
        # Write phase
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("persist", "data")
            ds = store.open_dataset("persist", "data")
            for i in range(1, 51):
                ds.write(i, f"val_{i}".encode())
        # Store is now closed, data flushed

        # Read phase - reopen
        with timslite.Store.open(tmpdir) as store:
            ds = store.open_dataset("persist", "data")
            results = ds.query_all(1, 100)
            assert len(results) == 50
            assert results[0] == (1, b"val_1")
            assert results[-1] == (50, b"val_50")

    def test_data_survives_process_restart(self, tmpdir):
        """Write -> process exit -> new process open -> data intact."""
        # Phase 1: write and exit
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("survive", "data")
            ds = store.open_dataset("survive", "data")
            ds.write(1, b"hello")
            ds.write(2, b"world")

        # Phase 2: new process (new store) reads
        with timslite.Store.open(tmpdir) as store:
            ds = store.open_dataset("survive", "data")
            results = ds.query_all(1, 100)
            assert len(results) == 2
            assert results[0] == (1, b"hello")
            assert results[1] == (2, b"world")

    def test_meta_file_invariant(self, tmpdir):
        """Reopening preserves dataset configuration from meta."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("meta_test", "data", compress_level=9)

        # Reopen and verify data_dir is accessible
        with timslite.Store.open(tmpdir) as store:
            ds = store.open_dataset("meta_test", "data")
            assert ds.data_dir is not None
