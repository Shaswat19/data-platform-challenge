"""Tests for steeleye package initialization."""

import steeleye


def test_version_exists() -> None:
    """Test that version is defined."""
    assert hasattr(steeleye, "__version__")
    assert isinstance(steeleye.__version__, str)


def test_package_imports() -> None:
    """Test that steeleye package can be imported."""
    assert steeleye is not None
