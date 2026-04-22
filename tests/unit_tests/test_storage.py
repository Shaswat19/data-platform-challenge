"""Unit tests for steeleye.storage module."""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from steeleye.storage import CloudStorage


class TestCloudStorage:
    """Test cases for CloudStorage class."""

    def test_init_default_options(self) -> None:
        """Test initialization with default storage options."""
        storage = CloudStorage("s3://bucket/path.csv")
        assert storage.storage_url == "s3://bucket/path.csv"
        assert storage.storage_options == {}

    def test_init_custom_options(self) -> None:
        """Test initialization with custom storage options."""
        options = {"key": "value"}
        storage = CloudStorage("s3://bucket/path.csv", storage_options=options)
        assert storage.storage_options == options

    @patch("steeleye.storage.fsspec.open")
    def test_upload_csv_success(self, mock_open: Mock) -> None:
        """Test successful CSV upload."""
        df = pd.DataFrame(
            {
                "col1": [1, 2],
                "col2": ["a", "b"],
            }
        )

        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        storage = CloudStorage("s3://bucket/path.csv")
        storage.upload_csv(df)

        mock_open.assert_called_once_with("s3://bucket/path.csv", mode="w")
        mock_file.to_csv.assert_called_once()
        args, kwargs = mock_file.to_csv.call_args
        assert kwargs["index"] is False

    @patch("steeleye.storage.fsspec.open")
    def test_upload_csv_with_options(self, mock_open: Mock) -> None:
        """Test CSV upload with storage options."""
        df = pd.DataFrame({"col1": [1]})
        options = {"account_name": "test", "account_key": "key"}

        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        storage = CloudStorage("az://container/path.csv", storage_options=options)
        storage.upload_csv(df)

        mock_open.assert_called_once_with(
            "az://container/path.csv", mode="w", **options
        )

    @patch("steeleye.storage.fsspec.open")
    def test_upload_csv_fsspec_error(self, mock_open: Mock) -> None:
        """Test handling of fsspec errors during upload."""
        df = pd.DataFrame({"col1": [1]})

        mock_open.side_effect = FileNotFoundError("Bucket not found")

        storage = CloudStorage("s3://bucket/path.csv")

        with pytest.raises(FileNotFoundError, match="Bucket not found"):
            storage.upload_csv(df)
