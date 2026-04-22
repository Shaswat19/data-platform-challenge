"""Integration tests for steeleye.main module."""

import shutil
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from steeleye.main import run_pipeline


class TestPipelineIntegration:
    """Integration tests for run_pipeline function."""

    @patch("steeleye.main.ESMADownloader")
    @patch("steeleye.main.XMLParser")
    @patch("steeleye.main.DataTransformer")
    @patch("steeleye.main.CloudStorage")
    def test_run_pipeline_cloud_storage(
        self,
        mock_cloud_storage: Mock,
        mock_transformer: Mock,
        mock_parser: Mock,
        mock_downloader: Mock,
    ) -> None:
        """Test run_pipeline with cloud storage."""
        # Setup mocks
        mock_downloader_instance = Mock()
        mock_downloader.return_value = mock_downloader_instance
        mock_downloader_instance.fetch_index_xml.return_value = Mock()
        mock_downloader_instance.get_second_dltins_url.return_value = (
            "http://example.com/data.zip"
        )
        mock_downloader_instance.download_and_extract_xml.return_value = (
            "<xml>data</xml>"
        )

        mock_parser_instance = Mock()
        mock_parser.return_value = mock_parser_instance
        mock_df = pd.DataFrame({"col": [1, 2]})
        mock_parser_instance.parse.return_value = mock_df

        mock_transformer_instance = Mock()
        mock_transformer.return_value = mock_transformer_instance
        mock_transformer_instance.transform.return_value = mock_df

        mock_cloud_storage_instance = Mock()
        mock_cloud_storage.return_value = mock_cloud_storage_instance

        # Run pipeline
        run_pipeline(
            index_url="http://example.com/index",
            storage_type="cloud",
            storage_path="s3://bucket/path.csv",
            timeout=30,
            storage_options={"key": "value"},
        )

        # Verify calls
        mock_downloader.assert_called_once_with(
            index_url="http://example.com/index",
            timeout=30,
        )
        mock_downloader_instance.fetch_index_xml.assert_called_once()
        mock_downloader_instance.get_second_dltins_url.assert_called_once()
        mock_downloader_instance.download_and_extract_xml.assert_called_once_with(
            "http://example.com/data.zip"
        )

        mock_parser.assert_called_once()
        mock_parser_instance.parse.assert_called_once_with("<xml>data</xml>")

        mock_transformer.assert_called_once()
        mock_transformer_instance.transform.assert_called_once_with(mock_df)

        mock_cloud_storage.assert_called_once_with(
            "s3://bucket/path.csv", {"key": "value"}
        )
        mock_cloud_storage_instance.upload_csv.assert_called_once_with(mock_df)

    @patch("steeleye.main.ESMADownloader")
    @patch("steeleye.main.XMLParser")
    @patch("steeleye.main.DataTransformer")
    def test_run_pipeline_local_storage(
        self,
        mock_transformer: Mock,
        mock_parser: Mock,
        mock_downloader: Mock,
    ) -> None:
        """Test run_pipeline with local storage."""
        # Setup mocks
        mock_downloader_instance = Mock()
        mock_downloader.return_value = mock_downloader_instance
        mock_downloader_instance.fetch_index_xml.return_value = Mock()
        mock_downloader_instance.get_second_dltins_url.return_value = (
            "http://example.com/data.zip"
        )
        mock_downloader_instance.download_and_extract_xml.return_value = (
            "<xml>data</xml>"
        )

        mock_parser_instance = Mock()
        mock_parser.return_value = mock_parser_instance
        mock_df = pd.DataFrame({"col": [1, 2]})
        mock_parser_instance.parse.return_value = mock_df

        mock_transformer_instance = Mock()
        mock_transformer.return_value = mock_transformer_instance
        mock_transformer_instance.transform.return_value = mock_df

        # Run pipeline with actual temp directory
        test_dir = "test_output_storage"
        try:
            run_pipeline(
                index_url="http://example.com/index",
                storage_type="local",
                storage_path=test_dir,
                timeout=30,
            )

            # Verify CSV was created
            csv_path = Path("output") / test_dir / "instruments.csv"
            assert csv_path.exists()
        finally:
            # Clean up
            if Path("output").exists():
                shutil.rmtree("output")

    def test_run_pipeline_invalid_storage_type(self) -> None:
        """Test run_pipeline raises ValueError for invalid storage type."""
        with pytest.raises(ValueError, match="storage_type must be 'local' or 'cloud'"):
            run_pipeline(
                index_url="http://example.com",
                storage_type="invalid",
                storage_path="test",
            )


class TestIntegration:
    """Integration tests for the full pipeline."""

    @patch("steeleye.main.ESMADownloader")
    @patch("steeleye.main.XMLParser")
    @patch("steeleye.main.DataTransformer")
    def test_full_pipeline_integration(
        self,
        mock_transformer: Mock,
        mock_parser: Mock,
        mock_downloader: Mock,
    ) -> None:
        """Test the complete pipeline flow with mocked external components."""
        # Setup comprehensive mocks
        mock_downloader_instance = Mock()
        mock_downloader.return_value = mock_downloader_instance

        # Mock index XML response
        mock_index_root = Mock()
        mock_downloader_instance.fetch_index_xml.return_value = mock_index_root
        mock_downloader_instance.get_second_dltins_url.return_value = (
            "http://example.com/dltins.zip"
        )
        mock_downloader_instance.download_and_extract_xml.return_value = """<BizData>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
                <ModfdRcrd>
                    <FinInstrmGnlAttrbts>
                        <Id>INT001</Id>
                        <FullNm>Integration Test Instrument</FullNm>
                        <ClssfctnTp>INT</ClssfctnTp>
                        <CmmdtyDerivInd>false</CmmdtyDerivInd>
                        <NtnlCcy>USD</NtnlCcy>
                    </FinInstrmGnlAttrbts>
                    <Issr>INTISSUER</Issr>
                </ModfdRcrd>
            </Document>
        </BizData>"""

        # Mock parser
        mock_parser_instance = Mock()
        mock_parser.return_value = mock_parser_instance
        parsed_df = pd.DataFrame(
            {
                "FinInstrmGnlAttrbts.Id": ["INT001"],
                "FinInstrmGnlAttrbts.FullNm": ["Integration Test Instrument"],
                "FinInstrmGnlAttrbts.ClssfctnTp": ["INT"],
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": ["false"],
                "FinInstrmGnlAttrbts.NtnlCcy": ["USD"],
                "Issr": ["INTISSUER"],
            }
        )
        mock_parser_instance.parse.return_value = parsed_df

        # Mock transformer
        mock_transformer_instance = Mock()
        mock_transformer.return_value = mock_transformer_instance
        transformed_df = parsed_df.copy()
        transformed_df["a_count"] = [0]  # "Integration" starts with uppercase
        transformed_df["contains_a"] = ["NO"]
        mock_transformer_instance.transform.return_value = transformed_df

        # Run pipeline with actual temp directory
        test_dir = "test_output_integration"
        try:
            run_pipeline(
                index_url="http://example.com/index",
                storage_type="local",
                storage_path=test_dir,
                timeout=30,
            )

            # Verify the pipeline called all components
            mock_downloader.assert_called_once()
            mock_parser.assert_called_once()
            mock_transformer.assert_called_once()

            # Verify data flow
            mock_parser_instance.parse.assert_called_once()
            mock_transformer_instance.transform.assert_called_once_with(parsed_df)
        finally:
            # Clean up
            if Path("output").exists():
                shutil.rmtree("output")
