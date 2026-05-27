"""Multi-dataset isolation tests."""

import pytest
import timslite


class TestMultiDataset:
    def test_two_datasets_isolated(self, tmpdir):
        """Writing to dataset A does not affect dataset B."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("alpha", "data")
            store.create_dataset("beta", "data")
            a = store.open_dataset("alpha", "data")
            b = store.open_dataset("beta", "data")
            a.write(1, b"alpha_data")
            b.write(1, b"beta_data")
            a_results = a.query_all(1, 10)
            b_results = b.query_all(1, 10)
            assert a_results == [(1, b"alpha_data")]
            assert b_results == [(1, b"beta_data")]

    def test_same_name_different_type(self, tmpdir):
        """Same name with different types are completely isolated."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("sensor", "waveform")
            store.create_dataset("sensor", "events")
            wf = store.open_dataset("sensor", "waveform")
            ev = store.open_dataset("sensor", "events")
            wf.write(1, b"wave")
            ev.write(1, b"event")
            wf_results = wf.query_all(1, 10)
            ev_results = ev.query_all(1, 10)
            assert wf_results == [(1, b"wave")]
            assert ev_results == [(1, b"event")]
