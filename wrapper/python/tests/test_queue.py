"""Functional tests for the Queue module (DatasetQueue + DatasetQueueConsumer)."""

import time

import pytest
import timslite


class TestQueueBasicFlow:
    def test_push_poll_ack(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("worker-1")

            ts = q.push(b"hello")
            assert ts > 0
            result = c.poll(100)
            assert result is not None
            rts, data = result
            assert rts == ts
            assert data == b"hello"

            c.ack(rts)
            assert c.poll(50) is None

            q.close()

    def test_multiple_pushes_sequential_poll(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("worker-1")

            for i in range(10):
                ts = q.push(f"msg_{i}".encode())
                assert ts == i + 1

            for i in range(10):
                result = c.poll(50)
                assert result is not None
                ts, data = result
                assert ts == i + 1
                assert data == f"msg_{i}".encode()
                c.ack(ts)

            assert c.poll(50) is None
            q.close()

    def test_empty_queue_poll_timeout(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("worker-1")
            assert c.poll(50) is None
            q.close()

    def test_push_various_data_types(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("worker-1")

            payloads = [b"", b"\x00\x01\x02\xff", b"a" * 1000]
            for data in payloads:
                ts = q.push(data)
                result = c.poll(100)
                assert result is not None
                rts, rdata = result
                assert rts == ts
                assert rdata == data
                c.ack(rts)

            q.close()

    def test_poll_callback_wakes_and_can_be_cleared(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("worker-callback")
            c2 = q.open_consumer("worker-callback-2")

            hits = []
            hits2 = []
            c.poll_callback(lambda: hits.append("wake"))
            with pytest.raises(timslite.TmslError):
                c.poll_callback(lambda: hits.append("duplicate"))
            c2.poll_callback(lambda: hits2.append("wake2"))
            q.push(b"row1")
            assert hits == ["wake"]
            assert hits2 == ["wake2"]
            assert c.poll(0) == (1, b"row1")
            c.ack(1)
            assert c2.poll(0) == (1, b"row1")
            c2.ack(1)

            c.poll_callback(None)
            q.push(b"row2")
            assert hits == ["wake"]
            assert hits2 == ["wake2", "wake2"]
            assert c.poll(0) == (2, b"row2")
            c.ack(2)
            assert c2.poll(0) == (2, b"row2")
            c2.ack(2)
            q.close()


class TestConsumerGroups:
    def test_independent_progress(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            ca = q.open_consumer("group_a")
            cb = q.open_consumer("group_b")

            q.push(b"item1")
            q.push(b"item2")

            r1 = ca.poll(50)
            assert r1 is not None and r1[1] == b"item1"
            ca.ack(r1[0])

            r2 = cb.poll(50)
            assert r2 is not None and r2[1] == b"item1"
            cb.ack(r2[0])

            r3 = ca.poll(50)
            assert r3 is not None and r3[1] == b"item2"

            r4 = cb.poll(50)
            assert r4 is not None and r4[1] == b"item2"

            q.close()

    def test_same_group_shared_progress(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c1 = q.open_consumer("shared")
            c2 = q.open_consumer("shared")

            q.push(b"shared_item")

            r1 = c1.poll(50)
            assert r1 is not None
            ts1, data1 = r1
            assert ts1 == 1
            assert data1 == b"shared_item"

            assert c2.poll(10) is None

            q.close()

    def test_same_group_unexpired_pending_allows_next_record(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c1 = q.open_consumer(
                "shared",
                running_expired_seconds=60,
                max_retry_count=3,
            )
            c2 = q.open_consumer(
                "shared",
                running_expired_seconds=60,
                max_retry_count=3,
            )

            q.push(b"first")
            q.push(b"second")

            assert c1.poll(50) == (1, b"first")
            assert c2.poll(50) == (2, b"second")

            c1.ack(1)
            c2.ack(2)
            q.close()

    def test_retry_limit_drops_expired_pending(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer(
                "retry",
                running_expired_seconds=1,
                max_retry_count=1,
            )

            q.push(b"first")
            q.push(b"second")

            assert c.poll(50) == (1, b"first")
            time.sleep(1.1)
            assert c.poll(50) == (1, b"first")
            time.sleep(1.1)
            assert c.poll(50) == (2, b"second")

            c.ack(2)
            q.close()

    def test_same_group_config_mismatch_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            q.open_consumer("shared")

            with pytest.raises(timslite.TmslError):
                q.open_consumer(
                    "shared",
                    running_expired_seconds=30,
                    max_retry_count=3,
                )

            q.close()

    def test_group_names_inspect_flush_and_close_release_pending(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c1 = q.open_consumer("shared")
            c1_alias = q.open_consumer("shared")
            q.open_consumer("other")

            assert q.get_consumer_group_names() == ["other", "shared"]

            q.push(b"first")
            assert c1.poll(50) == (1, b"first")
            c1.flush()

            inspected = c1.inspect()
            assert inspected.info.group_name == "shared"
            assert inspected.info.running_expired_seconds == 900
            assert inspected.info.max_retry_count == 3
            assert inspected.state.processed_ts == -(2**63)
            assert inspected.state.pending_entries[0].timestamp == 1

            c1.close()
            with pytest.raises(timslite.TmslQueueClosedError):
                c1_alias.poll(0)
            with pytest.raises(timslite.TmslQueueClosedError):
                c1.ack(1)

            reopened = q.open_consumer("shared")
            assert reopened.poll(50) == (1, b"first")
            reopened.ack(1)
            q.close()


class TestQueueErrors:
    def test_open_twice_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            store.open_queue(ds.id)
            with pytest.raises(timslite.TmslQueueAlreadyOpenError):
                store.open_queue(ds.id)

    def test_push_to_closed_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            q.close()
            with pytest.raises(timslite.TmslQueueClosedError):
                q.push(b"test")

    def test_poll_after_close_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("g1")
            q.close()
            with pytest.raises(timslite.TmslQueueClosedError):
                c.poll(50)

    def test_ack_nonexistent_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            c = q.open_consumer("g1")
            with pytest.raises(timslite.TmslError):
                c.ack(99999)

    def test_open_consumer_on_closed_raises(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)
            q.close()
            with pytest.raises(timslite.TmslQueueClosedError):
                q.open_consumer("g1")


class TestQueuePersistence:
    def test_pending_survives_reopen(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        # First session
        with timslite.Store.open(tmpdir, cfg) as store1:
            store1.create_dataset("events", "events")
            ds1 = store1.open_dataset("events", "events")
            q1 = store1.open_queue(ds1.id)
            c1 = q1.open_consumer("g1")

            q1.push(b"a")
            q1.push(b"b")
            q1.push(b"c")

            r = c1.poll(50)
            assert r is not None
            c1.ack(r[0])  # ack item "a"

            r = c1.poll(50)
            assert r is not None
            assert r[1] == b"b"
            # Don't ack "b" — leave it pending

            q1.close()

        # Second session
        with timslite.Store.open(tmpdir, cfg) as store2:
            ds2 = store2.open_dataset("events", "events")
            q2 = store2.open_queue(ds2.id)
            c2 = q2.open_consumer("g1")

            r = c2.poll(50)
            assert r is not None
            assert r[1] == b"b"  # the unacked pending item
            c2.ack(r[0])

            r = c2.poll(50)
            assert r is not None
            assert r[1] == b"c"  # next item after pending

            q2.close()

    def test_drop_and_recreate_consumer(self, tmpdir):
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)

            q.open_consumer("temp")
            q.drop_consumer("temp")
            q.open_consumer("temp")  # should work after drop

            q.close()


class TestQueueConcurrency:
    def test_queue_push_poll_roundtrip(self, tmpdir):
        """Basic push-poll roundtrip via a single queue instance."""
        cfg = timslite.StoreConfig(enable_background_thread=False)
        with timslite.Store.open(tmpdir, cfg) as store:
            store.create_dataset("events", "events")
            ds = store.open_dataset("events", "events")
            q = store.open_queue(ds.id)

            c = q.open_consumer("workers")

            # Queue is Clone-safe via Arc internally
            q.push(b"from_main")
            q.push(b"from_alias")

            r1 = c.poll(50)
            assert r1 is not None
            c.ack(r1[0])

            r2 = c.poll(50)
            assert r2 is not None
            c.ack(r2[0])

            assert c.poll(50) is None
            q.close()
