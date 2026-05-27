"""Lifecycle tests: create/open/close/drop flows."""

import pytest
import timslite


class TestLifecycle:
    def test_create_open_write_close(self, tmpdir):
        """Complete create -> open -> write -> close lifecycle."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("test", "events")
            ds = store.open_dataset("test", "events")
            ds.write(1, b"hello")
            ds.write(2, b"world")
            ds.flush()

    def test_create_twice_raises(self, tmpdir):
        """Creating a dataset that already exists raises TmslAlreadyExistsError."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("test", "events")
            with pytest.raises(timslite.TmslAlreadyExistsError):
                store.create_dataset("test", "events")

    def test_open_nonexistent_raises(self, tmpdir):
        """Opening a nonexistent dataset raises TmslNotFoundError."""
        with timslite.Store.open(tmpdir) as store:
            with pytest.raises(timslite.TmslNotFoundError):
                store.open_dataset("nonexistent", "data")

    def test_drop_dataset(self, tmpdir):
        """Drop removes a dataset; can recreate afterwards."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("test", "events")
            ds = store.open_dataset("test", "events")
            ds.write(1, b"temp")
            store.drop_dataset("test", "events")
            # After drop, can create again
            store.create_dataset("test", "events")
            ds2 = store.open_dataset("test", "events")
            ds2.write(2, b"new")

    def test_operations_on_closed_store_raises(self, tmpdir):
        """Attempting operations on a closed store raises RuntimeError."""
        store = timslite.Store.open(tmpdir)
        store.close()
        with pytest.raises(RuntimeError, match="closed"):
            store.create_dataset("test", "events")
        with pytest.raises(RuntimeError, match="closed"):
            store.open_dataset("test", "events")
