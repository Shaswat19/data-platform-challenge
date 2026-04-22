"""Module for cloud-agnostic CSV upload using ``fsspec``."""

import logging
from typing import Any

import fsspec
import pandas as pd

logger = logging.getLogger(__name__)


class CloudStorage:
    """Uploads a ``pd.DataFrame`` as a CSV to any cloud storage supported by ``fsspec``.

    Supports AWS S3 (``s3://``), Azure Blob Storage (``az://`` or ``abfs://``),
    GCS (``gcs://``), and any other ``fsspec``-compatible backend.

    Args:
        storage_url: Full destination path, e.g.
            - ``"s3://my-bucket/path/output.csv"``
            - ``"az://my-container/path/output.csv"``
        storage_options: Optional dictionary of credentials/configuration
            passed directly to ``fsspec.open()``.
            For S3: ``{"key": ..., "secret": ...}`` or rely on env/IAM.
            For Azure: ``{"account_name": ..., "account_key": ...}``.

    Example::

        # AWS S3 (credentials from environment / IAM role)
        storage = CloudStorage("s3://my-bucket/output/instruments.csv")
        storage.upload_csv(df)

        # Azure Blob Storage
        storage = CloudStorage(
            "az://my-container/instruments.csv",
            storage_options={"account_name": "myaccount", "account_key": "mykey"},
        )
        storage.upload_csv(df)
    """

    def __init__(
        self,
        storage_url: str,
        storage_options: dict[str, Any] | None = None,
    ) -> None:
        """Initialise CloudStorage.

        Args:
            storage_url: Fully qualified destination path.
            storage_options: Credentials and configuration for ``fsspec``.
        """
        self.storage_url = storage_url
        self.storage_options: dict[str, Any] = storage_options or {}

    def upload_csv(self, df: pd.DataFrame) -> None:
        """Serialise ``df`` to CSV and write it to the configured cloud path.

        Args:
            df: DataFrame to upload.

        Raises:
            FileNotFoundError: If the target bucket/container does not exist.
            PermissionError: On insufficient cloud credentials.
        """
        logger.info(
            "Uploading CSV (%d rows, %d cols) to %s",
            len(df),
            len(df.columns),
            self.storage_url,
        )
        with fsspec.open(self.storage_url, mode="w", **self.storage_options) as f:
            df.to_csv(f, index=False)

        logger.info("Upload complete: %s", self.storage_url)
