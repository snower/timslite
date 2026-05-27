"""Configuration tests: StoreConfig fields, create_dataset kwargs."""

import pytest
import timslite


class TestConfig:
    def test_store_config_defaults(self):
        """All default values are correct."""
        config = timslite.StoreConfig.default()
        assert config.flush_interval == 600
        assert config.idle_timeout == 1800
        assert config.data_segment_size == 67108864
        assert config.index_segment_size == 4194304
        assert config.initial_data_segment_size == 262144
        assert config.initial_index_segment_size == 4096
        assert config.block_max_size == 65536
        assert config.compress_level == 6
        assert config.cache_max_memory == 268435456
        assert config.cache_idle_timeout == 1800

    def test_store_config_custom(self):
        """Custom config sets all fields."""
        config = timslite.StoreConfig(
            flush_interval=300,
            idle_timeout=900,
            data_segment_size=128 * 1024 * 1024,
            index_segment_size=8 * 1024 * 1024,
            initial_data_segment_size=512 * 1024,
            initial_index_segment_size=8192,
            block_max_size=32768,
            compress_level=9,
            cache_max_memory=512 * 1024 * 1024,
            cache_idle_timeout=600,
        )
        assert config.flush_interval == 300
        assert config.idle_timeout == 900
        assert config.data_segment_size == 128 * 1024 * 1024
        assert config.index_segment_size == 8 * 1024 * 1024
        assert config.initial_data_segment_size == 512 * 1024
        assert config.initial_index_segment_size == 8192
        assert config.block_max_size == 32768
        assert config.compress_level == 9
        assert config.cache_max_memory == 512 * 1024 * 1024
        assert config.cache_idle_timeout == 600

    def test_create_dataset_with_kwargs(self, tmpdir):
        """create_dataset kwargs override store defaults."""
        config = timslite.StoreConfig(compress_level=3)
        with timslite.Store.open(tmpdir, config=config) as store:
            store.create_dataset("k", "data", compress_level=9, data_segment_size=1024 * 1024)
            ds = store.open_dataset("k", "data")
            ds.write(1, b"test")

    def test_create_dataset_uses_store_defaults(self, tmpdir):
        """Not passing kwargs uses store config defaults."""
        config = timslite.StoreConfig(compress_level=9, cache_max_memory=0)
        with timslite.Store.open(tmpdir, config=config) as store:
            store.create_dataset("d", "data")
            ds = store.open_dataset("d", "data")
            ds.write(1, b"default_compress")

    def test_index_continuous_kwarg(self, tmpdir):
        """index_continuous=True enables continuous mode."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("seq", "data", index_continuous=True)
            ds = store.open_dataset("seq", "data")
            ds.write(100, b"first")
            ds.write(50, b"earlier")  # out-of-order allowed in continuous
            ds.write(200, b"last")
            results = ds.query_all(1, 300)
            assert len(results) == 3
