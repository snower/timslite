"""Configuration tests: StoreConfig fields, create_dataset kwargs."""

import pytest
import timslite


class TestConfig:
    def test_store_config_defaults(self):
        """All default values are correct."""
        config = timslite.StoreConfig.default()
        assert config.flush_interval == 15
        assert config.idle_timeout == 1800
        assert config.data_segment_size == 67108864
        assert config.index_segment_size == 4194304
        assert config.initial_data_segment_size == 262144
        assert config.initial_index_segment_size == 4096
        assert config.compress_level == 6
        assert config.cache_max_memory == 268435456
        assert config.cache_idle_timeout == 1800
        assert config.retention_check_hour == 0
        assert config.enable_background_thread is True
        assert config.enable_journal is True
        assert config.read_only is None

    def test_store_config_constructor_defaults_match_rust_defaults(self):
        """StoreConfig() defaults match StoreConfig.default()."""
        config = timslite.StoreConfig()
        assert config.flush_interval == 15
        assert config.idle_timeout == 1800
        assert config.enable_background_thread is True
        assert config.enable_journal is True
        assert config.read_only is None

    def test_store_config_custom(self):
        """Custom config sets all fields including retention_check_hour and enable_background_thread."""
        config = timslite.StoreConfig(
            flush_interval=300,
            idle_timeout=900,
            data_segment_size=128 * 1024 * 1024,
            index_segment_size=8 * 1024 * 1024,
            initial_data_segment_size=512 * 1024,
            initial_index_segment_size=8192,
            compress_level=9,
            cache_max_memory=512 * 1024 * 1024,
            cache_idle_timeout=600,
            retention_check_hour=3,
            enable_background_thread=False,
            enable_journal=False,
            read_only=True,
        )
        assert config.flush_interval == 300
        assert config.idle_timeout == 900
        assert config.data_segment_size == 128 * 1024 * 1024
        assert config.index_segment_size == 8 * 1024 * 1024
        assert config.initial_data_segment_size == 512 * 1024
        assert config.initial_index_segment_size == 8192
        assert config.compress_level == 9
        assert config.cache_max_memory == 512 * 1024 * 1024
        assert config.cache_idle_timeout == 600
        assert config.retention_check_hour == 3
        assert config.enable_background_thread is False
        assert config.enable_journal is False
        assert config.read_only is True

    def test_store_config_read_only_tristate(self):
        """read_only accepts None, False, and True."""
        assert timslite.StoreConfig(read_only=None).read_only is None
        assert timslite.StoreConfig(read_only=False).read_only is False
        assert timslite.StoreConfig(read_only=True).read_only is True

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
        """index_continuous=True enables continuous mode with logical gap."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("seq", "data", index_continuous=True)
            ds = store.open_dataset("seq", "data")
            ds.write(100, b"first")
            ds.write(200, b"last")
            ds.write(150, b"middle")  # fills gap within segment range
            results = ds.query_all(1, 300)
            assert len(results) == 3

    def test_create_dataset_enable_journal_false(self, tmpdir):
        """Dataset-level enable_journal=False suppresses this dataset's records."""
        with timslite.Store.open(tmpdir) as store:
            store.create_dataset("quiet", "data", enable_journal=False)
            inspect = store.inspect_dataset("quiet", "data")
            assert inspect.info.enable_journal is False
            assert inspect.state.has_journal is False

            ds = store.open_dataset("quiet", "data")
            ds.write(1, b"quiet")
            store.drop_dataset("quiet", "data")
            assert store.journal_query(1, 100) == []
