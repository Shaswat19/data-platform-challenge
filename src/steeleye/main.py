"""Entry-point for the ESMA FIRDS data pipeline.

All configuration (URLs, storage paths, credentials) is defined here and
passed explicitly into each component — no module-level constants live
inside the library classes.

Usage::

    python -m steeleye.main --storage-type cloud --storage-url s3://my-bucket/output/instruments.csv
    python -m steeleye.main --storage-type local --storage-path instruments
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from steeleye.downloader import ESMADownloader
from steeleye.parser import XMLParser
from steeleye.storage import CloudStorage
from steeleye.transformer import DataTransformer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — all URLs and defaults live here, not inside library modules
# ---------------------------------------------------------------------------

DEFAULT_ESMA_INDEX_URL = (
    "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select"
    "?q=*"
    "&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D"
    "&wt=xml&indent=true&start=0&rows=100"
)

DEFAULT_TIMEOUT = 60


def run_pipeline(
    index_url: str,
    storage_type: str,
    storage_path: str,
    timeout: int = DEFAULT_TIMEOUT,
    storage_options: dict[str, Any] | None = None,
) -> None:
    """Execute the full ESMA FIRDS ingestion pipeline end-to-end.

    All external configuration (URLs, timeouts, credentials) is accepted as
    arguments so that each component remains independently testable and free
    of hard-coded values.

    Steps:
        1. Fetch ESMA index XML using the provided ``index_url``.
        2. Resolve the second DLTINS download URL from the index.
        3. Download and extract the instrument XML from the ZIP.
        4. Parse the XML into a ``pd.DataFrame``.
        5. Apply ``a_count`` / ``contains_a`` transformations.
        6. Upload the resulting CSV to local or cloud storage.

    Args:
        index_url: ESMA Solr endpoint URL for the file index.
        storage_type: Storage destination type ('local' or 'cloud').
        storage_path: If local: folder name under output/ (e.g., "instruments").
                     If cloud: fsspec-compatible path
                     (e.g., "s3://bucket/key.csv" or "az://container/key.csv").
        timeout: HTTP request timeout in seconds.
        storage_options: Optional credentials/config forwarded to ``fsspec``.

    Raises:
        ValueError: If storage_type is not 'local' or 'cloud'.
    """
    if storage_type not in ("local", "cloud"):
        raise ValueError(
            f"storage_type must be 'local' or 'cloud', got '{storage_type}'"
        )

    logger.info("Pipeline started.")

    downloader = ESMADownloader(index_url=index_url, timeout=timeout)
    index_root = downloader.fetch_index_xml()
    dltins_url = downloader.get_second_dltins_url(index_root)
    xml_content = downloader.download_and_extract_xml(dltins_url)
    # noqa: E501
    df = XMLParser().parse(xml_content)
    df = DataTransformer().transform(df)

    if storage_type == "local":
        output_dir = Path("output") / storage_path
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / "instruments.csv"
        df.to_csv(csv_path, index=False)
        logger.info("Pipeline finished. CSV written to: %s", csv_path)
    else:  # storage_type == "cloud"
        CloudStorage(storage_path, storage_options).upload_csv(df)
        logger.info("Pipeline finished. Rows written: %d", len(df))


def _build_arg_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured ``argparse.ArgumentParser`` instance.
    """
    parser = argparse.ArgumentParser(
        description="ESMA FIRDS ingestion pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--index-url",
        default=DEFAULT_ESMA_INDEX_URL,
        help="ESMA Solr index endpoint URL.",
    )
    parser.add_argument(
        "--storage-type",
        choices=["local", "cloud"],
        required=True,
        help="Storage destination type.",
    )
    parser.add_argument(
        "--storage-path",
        required=True,
        help=(
            "If storage-type is 'local': folder name under output/ (e.g., 'instruments'). "
            "If storage-type is 'cloud': fsspec URL (e.g., 's3://bucket/key.csv' or 'az://container/key.csv')."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--account-name",
        default=None,
        help="Azure storage account name.",
    )
    parser.add_argument(
        "--account-key",
        default=None,
        help="Azure storage account key.",
    )
    return parser


def main() -> None:
    """Parse CLI arguments and run the pipeline."""
    args = _build_arg_parser().parse_args()

    storage_options: dict[str, Any] = {}
    if args.account_name:
        storage_options["account_name"] = args.account_name
    if args.account_key:
        storage_options["account_key"] = args.account_key

    try:
        run_pipeline(
            index_url=args.index_url,
            storage_type=args.storage_type,
            storage_path=args.storage_path,
            timeout=args.timeout,
            storage_options=storage_options or None,
        )
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
