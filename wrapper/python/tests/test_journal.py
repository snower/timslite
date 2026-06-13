"""Functional tests for the dedicated journal API."""

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


def test_public_journal_dataset_is_not_openable(tmpdir):
    cfg = timslite.StoreConfig(enable_background_thread=False)
    with timslite.Store.open(tmpdir, cfg) as store:
        with pytest.raises(timslite.TmslError):
            store.open_dataset(".journal", "logs")
