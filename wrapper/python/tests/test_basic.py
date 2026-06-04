"""Smoke tests: import, Store.open, Store.close."""

import pytest
import timslite


class TestBasic:
    def test_import(self, tmpdir):
        """All exported symbols are accessible."""
        assert timslite.Store is not None
        assert timslite.StoreConfig is not None
        assert timslite.Dataset is not None
        assert timslite.QueryIterator is not None
        assert timslite.TmslError is not None
        assert timslite.TmslNotFoundError is not None
        assert timslite.TmslAlreadyExistsError is not None
        assert timslite.TmslInvalidDataError is not None
        assert timslite.TmslIoError is not None
        assert timslite.TmslSegmentFullError is not None
        assert timslite.TmslMmapError is not None
        assert timslite.TmslCompressionError is not None
        assert timslite.TmslDecompressionError is not None

    def test_store_open_close(self, tmpdir):
        """Open and close a store without error."""
        store = timslite.Store.open(tmpdir)
        store.close()

    def test_store_context_manager(self, tmpdir):
        """Context manager enters and exits cleanly."""
        with timslite.Store.open(tmpdir) as store:
            assert isinstance(store, timslite.Store)

    def test_close_twice_raises(self, tmpdir):
        """Closing an already closed store raises RuntimeError."""
        store = timslite.Store.open(tmpdir)
        store.close()
        with pytest.raises(RuntimeError, match="already closed"):
            store.close()

    def test_store_config_default(self, tmpdir):
        """Default StoreConfig has sensible defaults."""
        config = timslite.StoreConfig.default()
        assert config.flush_interval == 600
        assert config.idle_timeout == 1800
        assert config.data_segment_size == 67108864
        assert config.index_segment_size == 4194304
        assert config.compress_level == 6
        assert config.cache_max_memory == 268435456
        assert config.enable_journal is True

    def test_store_config_custom(self, tmpdir):
        """Custom StoreConfig overrides selected fields."""
        config = timslite.StoreConfig(flush_interval=300, compress_level=9)
        assert config.flush_interval == 300
        assert config.idle_timeout == 1800  # default
        assert config.compress_level == 9
