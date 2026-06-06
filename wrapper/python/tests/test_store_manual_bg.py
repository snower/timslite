"""Tests for manual background task execution (Phase 22)."""

import timslite


class TestManualBackgroundTasks:
    """Verify enable_background_thread=False + manual tick API."""

    def test_disabled_thread_tick_and_next_delay(self, tmpdir):
        """enable=False → tick returns (executed, delay_ms), next_delay returns ms."""
        cfg = timslite.StoreConfig(
            enable_background_thread=False,
            flush_interval=0,  # 0s = always due on first tick
        )
        store = timslite.Store.open(str(tmpdir), cfg)

        store.create_dataset("ds1", "data")
        ds = store.open_dataset("ds1", "data")
        ds.write(1, b"hello")
        ds.write(2, b"world")

        # tick should execute flush immediately (interval=0)
        executed, delay_ms = store.tick_background_tasks()
        assert isinstance(executed, int), f"expected int, got {type(executed)}"
        assert isinstance(delay_ms, int), f"expected int, got {type(delay_ms)}"
        assert executed >= 1, f"expected >=1 tasks executed, got {executed}"
        assert delay_ms >= 0, f"delay should be >= 0, got {delay_ms}"

        # next_delay without executing
        delay = store.next_background_delay()
        assert isinstance(delay, int), f"expected int, got {type(delay)}"
        assert delay >= 0, f"delay should be >= 0, got {delay}"

        store.close()

    def test_disabled_thread_no_panic_on_repeated_ticks(self, tmpdir):
        """Repeated tick calls should not panic or deadlock."""
        cfg = timslite.StoreConfig(enable_background_thread=False)
        store = timslite.Store.open(str(tmpdir), cfg)

        for _ in range(5):
            executed, delay_ms = store.tick_background_tasks()
            # Without datasets, nothing is due — executed should be 0
            assert executed == 0

        store.close()

    def test_tick_flush_persists_data(self, tmpdir):
        """Write data, tick to flush, close, reopen — data must survive."""
        cfg = timslite.StoreConfig(
            enable_background_thread=False,
            flush_interval=0,  # 0s = always due
        )
        store = timslite.Store.open(str(tmpdir), cfg)
        store.create_dataset("ds2", "events")
        ds = store.open_dataset("ds2", "events")

        for i in range(30):
            ds.write(i + 1, f"val_{i}".encode())

        executed, _ = store.tick_background_tasks()
        assert executed >= 1, f"flush should have run, got {executed}"

        store.close()

        # Reopen and verify
        store2 = timslite.Store.open(str(tmpdir), timslite.StoreConfig.default())
        ds2 = store2.open_dataset("ds2", "events")
        entries = list(ds2.query(1, 30))
        assert len(entries) == 30, f"expected 30 entries, got {len(entries)}"

        store2.close()
