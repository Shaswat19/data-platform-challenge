"""Unit tests for steeleye.downloader module."""

import io
import xml.etree.ElementTree as ET  # noqa: N817
import zipfile
from unittest.mock import Mock, patch

import pytest
import requests
from steeleye.downloader import ESMADownloader


class TestESMADownloader:
    """Test cases for ESMADownloader class."""

    def test_init_default_session(self) -> None:
        """Test initialization with default session."""
        downloader = ESMADownloader("http://example.com")
        assert downloader.index_url == "http://example.com"
        assert downloader.timeout == 60
        assert isinstance(downloader.session, requests.Session)

    def test_init_custom_session(self) -> None:
        """Test initialization with custom session."""
        custom_session = requests.Session()
        downloader = ESMADownloader("http://example.com", session=custom_session)
        assert downloader.session is custom_session

    @patch("steeleye.downloader.requests.Session.get")
    def test_fetch_index_xml_success(self, mock_get: Mock) -> None:
        """Test successful fetch of index XML."""
        mock_response = Mock()
        mock_response.content = b"<root><doc></doc></root>"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        downloader = ESMADownloader("http://example.com")
        root = downloader.fetch_index_xml()

        assert ET.iselement(root)
        assert root.tag == "root"
        mock_get.assert_called_once_with("http://example.com", timeout=60)

    @patch("steeleye.downloader.requests.Session.get")
    def test_fetch_index_xml_http_error(self, mock_get: Mock) -> None:
        """Test HTTP error during fetch."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = mock_response

        downloader = ESMADownloader("http://example.com")

        with pytest.raises(requests.HTTPError):
            downloader.fetch_index_xml()

    def test_get_second_dltins_url_success(self) -> None:
        """Test extracting second DLTINS URL."""
        xml_content = """<root>
            <doc>
                <str name="file_type">OTHER</str>
                <str name="download_link">http://example.com/other.zip</str>
            </doc>
            <doc>
                <str name="file_type">DLTINS</str>
                <str name="download_link">http://example.com/dltins1.zip</str>
            </doc>
            <doc>
                <str name="file_type">DLTINS</str>
                <str name="download_link">http://example.com/dltins2.zip</str>
            </doc>
        </root>"""
        root = ET.fromstring(xml_content)

        downloader = ESMADownloader("http://example.com")
        url = downloader.get_second_dltins_url(root)

        assert url == "http://example.com/dltins2.zip"

    def test_get_second_dltins_url_insufficient_entries(self) -> None:
        """Test error when fewer than 2 DLTINS entries."""
        xml_content = """<root>
            <doc>
                <str name="file_type">DLTINS</str>
                <str name="download_link">http://example.com/dltins1.zip</str>
            </doc>
        </root>"""
        root = ET.fromstring(xml_content)

        downloader = ESMADownloader("http://example.com")

        with pytest.raises(ValueError, match="Expected at least 2 DLTINS entries"):
            downloader.get_second_dltins_url(root)

    @patch("steeleye.downloader.requests.Session.get")
    def test_download_and_extract_xml_success(self, mock_get: Mock) -> None:
        """Test successful download and extraction of XML from ZIP."""
        # Create a mock ZIP with XML content
        xml_content = "<test>data</test>"
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("data.xml", xml_content)
        zip_buffer.seek(0)

        mock_response = Mock()
        mock_response.content = zip_buffer.getvalue()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        downloader = ESMADownloader("http://example.com")
        result = downloader.download_and_extract_xml("http://example.com/test.zip")

        assert result == xml_content
        mock_get.assert_called_once_with("http://example.com/test.zip", timeout=60)

    @patch("steeleye.downloader.requests.Session.get")
    def test_download_and_extract_xml_no_xml(self, mock_get: Mock) -> None:
        """Test error when ZIP contains no XML file."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("data.txt", "text content")
        zip_buffer.seek(0)

        mock_response = Mock()
        mock_response.content = zip_buffer.getvalue()
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        downloader = ESMADownloader("http://example.com")

        with pytest.raises(ValueError, match="No XML file found"):
            downloader.download_and_extract_xml("http://example.com/test.zip")

    @patch("steeleye.downloader.requests.Session.get")
    def test_download_and_extract_xml_http_error(self, mock_get: Mock) -> None:
        """Test HTTP error during download."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")
        mock_get.return_value = mock_response

        downloader = ESMADownloader("http://example.com")

        with pytest.raises(requests.HTTPError):
            downloader.download_and_extract_xml("http://example.com/test.zip")
