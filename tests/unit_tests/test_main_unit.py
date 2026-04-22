"""Unit tests for steeleye.main module functions."""

import argparse

import pytest

from steeleye.main import _build_arg_parser


class TestMainFunctions:
    """Test cases for main module functions."""

    def test_build_arg_parser(self) -> None:
        """Test argument parser configuration."""
        parser = _build_arg_parser()

        assert isinstance(parser, argparse.ArgumentParser)

        # Test parsing valid args
        args = parser.parse_args(
            [
                "--storage-type",
                "local",
                "--storage-path",
                "test_dir",
            ]
        )

        assert args.storage_type == "local"
        assert args.storage_path == "test_dir"
        assert args.timeout == 60
        assert args.index_url.startswith("https://registers.esma.europa.eu")

    def test_build_arg_parser_invalid_storage_type(self) -> None:
        """Test parser rejects invalid storage type."""
        parser = _build_arg_parser()

        with pytest.raises(SystemExit):  # argparse exits on error
            parser.parse_args(
                [
                    "--storage-type",
                    "invalid",
                    "--storage-path",
                    "test",
                ]
            )
