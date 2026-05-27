"""Exception type tests."""

import pytest
import timslite


class TestExceptions:
    def test_all_exceptions_importable(self, tmpdir):
        """All 9 exception classes are importable from timslite module."""
        exceptions = [
            "TmslError",
            "TmslIoError",
            "TmslNotFoundError",
            "TmslAlreadyExistsError",
            "TmslInvalidDataError",
            "TmslSegmentFullError",
            "TmslMmapError",
            "TmslCompressionError",
            "TmslDecompressionError",
        ]
        for name in exceptions:
            assert hasattr(timslite, name), f"Missing exception: {name}"

    def test_exception_hierarchy(self, tmpdir):
        """All specific exceptions inherit from TmslError."""
        specific = [
            timslite.TmslIoError,
            timslite.TmslNotFoundError,
            timslite.TmslAlreadyExistsError,
            timslite.TmslInvalidDataError,
            timslite.TmslSegmentFullError,
            timslite.TmslMmapError,
            timslite.TmslCompressionError,
            timslite.TmslDecompressionError,
        ]
        for exc_cls in specific:
            assert issubclass(exc_cls, timslite.TmslError)

    def test_catch_specific_exception(self, tmpdir):
        """Can catch TmslNotFoundError specifically (not just TmslError)."""
        with timslite.Store.open(tmpdir) as store:
            with pytest.raises(timslite.TmslNotFoundError):
                store.open_dataset("nope", "data")

    def test_error_message_contains_description(self, tmpdir):
        """Exception message contains useful description."""
        with timslite.Store.open(tmpdir) as store:
            try:
                store.open_dataset("nonexistent", "data")
            except timslite.TmslNotFoundError as e:
                assert "nonexistent" in str(e)
