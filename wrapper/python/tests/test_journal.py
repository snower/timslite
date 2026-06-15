"""Functional tests for the dedicated journal API."""

import time

import pytest
import timslite


def test_journal_read_query_and_queue(tmpdir):
    cfg = timslite.StoreConfig(enable_background_thread=False)
    with timslite.Store.open(tmpdir, cfg) as store:
        store.create_dataset("journalpy", "data")
        latest = store.journal_latest_sequence()
        assert latest is not None and latest >= 1

        first = store.journal_read(1)
        assert first is not None
        sequence, payload = first
        assert sequence == 1
        assert payload

        rows = store.journal_query(1, latest)
        assert rows
        assert rows[0][0] == 1

        queue = store.open_journal_queue()
        consumer = queue.open_consumer("journal_py")

        ds = store.open_dataset("journalpy", "data")
        ds.write(10, b"payload")

        polled = consumer.poll(100)
        assert polled is not None
        journal_sequence, journal_payload = polled
        assert journal_sequence > latest
        assert journal_payload
        consumer.ack(journal_sequence)
        queue.close()


def test_journal_queue_configured_consumers_skip_unexpired_pending(tmpdir):
    cfg = timslite.StoreConfig(enable_background_thread=False)
    with timslite.Store.open(tmpdir, cfg) as store:
        store.create_dataset("journalpy", "data")
        queue = store.open_journal_queue()
        c1 = queue.open_consumer(
            "shared",
            running_expired_seconds=60,
            max_retry_count=3,
        )
        c2 = queue.open_consumer(
            "shared",
            running_expired_seconds=60,
            max_retry_count=3,
        )

        ds = store.open_dataset("journalpy", "data")
        ds.write(10, b"first")
        ds.write(11, b"second")

        seq1, payload1 = c1.poll(100)
        assert payload1
        seq2, payload2 = c2.poll(100)
        assert seq2 == seq1 + 1
        assert payload2

        c1.ack(seq1)
        c2.ack(seq2)
        queue.close()


def test_journal_queue_retry_limit_drops_expired_pending(tmpdir):
    cfg = timslite.StoreConfig(enable_background_thread=False)
    with timslite.Store.open(tmpdir, cfg) as store:
        store.create_dataset("journalpy", "data")
        queue = store.open_journal_queue()
        consumer = queue.open_consumer(
            "retry",
            running_expired_seconds=1,
            max_retry_count=1,
        )

        ds = store.open_dataset("journalpy", "data")
        ds.write(10, b"first")
        ds.write(11, b"second")

        seq1, payload1 = consumer.poll(100)
        assert payload1
        time.sleep(1.1)
        retry_seq, retry_payload = consumer.poll(100)
        assert retry_seq == seq1
        assert retry_payload
        time.sleep(1.1)
        seq2, payload2 = consumer.poll(100)
        assert seq2 == seq1 + 1
        assert payload2

        consumer.ack(seq2)
        queue.close()


def test_public_journal_dataset_is_not_openable(tmpdir):
    cfg = timslite.StoreConfig(enable_background_thread=False)
    with timslite.Store.open(tmpdir, cfg) as store:
        with pytest.raises(timslite.TmslError):
            store.open_dataset(".journal", "logs")
