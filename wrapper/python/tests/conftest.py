"""Conftest: fixtures and helpers for timslite tests."""

import pytest


@pytest.fixture
def tmpdir():
    """Temporary directory fixture with Windows-safe cleanup.

    On Windows, mmap files held by the detached background thread may prevent
    immediate deletion. This helper retries/rm -f style cleanup.
    """
    import tempfile
    import time

    name = tempfile.mkdtemp()
    try:
        yield name
    finally:
        import shutil
        import os

        if os.path.exists(name):
            try:
                shutil.rmtree(name)
            except OSError:
                time.sleep(0.1)
                try:
                    shutil.rmtree(name, ignore_errors=True)
                except OSError:
                    pass  # Give up on cleanup
