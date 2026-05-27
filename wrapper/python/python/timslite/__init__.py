"""High-performance time-series data storage.

timslite is a Python wrapper for the timslite Rust library,
providing fast, mmap-backed time-series data storage through
a Pythonic API with PyO3 bindings.

Usage:
    with timslite.Store.open("/data/timslite") as store:
        store.create_dataset("sensor", "waveform")
        ds = store.open_dataset("sensor", "waveform")
        ds.write(1, b"reading_1")
        for ts, data in ds.query(1, 100):
            print(f"ts={ts}, data={data}")
"""

from .timslite import (
    Store,
    StoreConfig,
    Dataset,
    QueryIterator,
    TmslError,
    TmslIoError,
    TmslNotFoundError,
    TmslAlreadyExistsError,
    TmslInvalidDataError,
    TmslSegmentFullError,
    TmslMmapError,
    TmslCompressionError,
    TmslDecompressionError,
)

__all__ = [
    "Store",
    "StoreConfig",
    "Dataset",
    "QueryIterator",
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
