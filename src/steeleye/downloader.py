"""Module responsible for downloading and extracting ESMA FIRDS data."""

import io
import logging
import xml.etree.ElementTree as ET  # noqa: N817
import zipfile

import requests

logger = logging.getLogger(__name__)


class ESMADownloader:
    """Downloads ESMA FIRDS index XML and extracts instrument ZIP files.

    Attributes:
        index_url: The ESMA Solr endpoint used to fetch the file index.
        timeout: HTTP request timeout in seconds.
        session: Shared requests.Session instance for connection reuse.

    Example::

        downloader = ESMADownloader(index_url="https://...")
        root = downloader.fetch_index_xml()
        url = downloader.get_second_dltins_url(root)
        xml_content = downloader.download_and_extract_xml(url)
    """

    def __init__(
        self,
        index_url: str,
        timeout: int = 60,
        session: requests.Session | None = None,
    ) -> None:
        """Initialise the downloader.

        Args:
            index_url: ESMA Solr endpoint URL. Passed explicitly from the caller.
            timeout: HTTP request timeout in seconds.
            session: Optional pre-configured requests.Session.
        """
        self.index_url = index_url
        self.timeout = timeout
        self.session: requests.Session = session or requests.Session()

    def fetch_index_xml(self) -> ET.Element:
        """Fetch the ESMA index XML and return its root element.

        Returns:
            Parsed root ET.Element of the index XML.

        Raises:
            requests.HTTPError: If the HTTP response contains an error status.
            ET.ParseError: If the response body is not valid XML.
        """
        logger.info("Fetching ESMA index XML from %s", self.index_url)
        response = self.session.get(self.index_url, timeout=self.timeout)
        response.raise_for_status()
        logger.debug("Index XML fetched; content length=%d", len(response.content))
        return ET.fromstring(response.content)

    def get_second_dltins_url(self, root: ET.Element) -> str:
        """Return the download URL of the second DLTINS entry in the index.

        Args:
            root: Root element of the ESMA index XML.

        Returns:
            Download URL string for the second DLTINS file.

        Raises:
            ValueError: If fewer than two DLTINS entries are found.
        """
        dltins_urls: list[str] = []

        for doc in root.findall(".//doc"):
            file_type_el = doc.find("str[@name='file_type']")
            download_link_el = doc.find("str[@name='download_link']")

            if (
                file_type_el is not None
                and file_type_el.text == "DLTINS"
                and download_link_el is not None
                and download_link_el.text
            ):
                dltins_urls.append(download_link_el.text)

        logger.info("Found %d DLTINS entries in index.", len(dltins_urls))

        if len(dltins_urls) < 2:
            raise ValueError(
                f"Expected at least 2 DLTINS entries, found {len(dltins_urls)}."
            )

        url = dltins_urls[1]
        logger.info("Using second DLTINS URL: %s", url)
        return url

    def download_and_extract_xml(self, url: str) -> str:
        """Download a ZIP archive and return the first XML file inside as a string.

        Args:
            url: Direct download URL of the ZIP file.

        Returns:
            UTF-8 decoded content of the first XML file found inside the ZIP.

        Raises:
            requests.HTTPError: On non-2xx HTTP response.
            ValueError: If no XML file is found inside the ZIP archive.
        """
        logger.info("Downloading ZIP from %s", url)
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            xml_files = [name for name in archive.namelist() if name.endswith(".xml")]

            if not xml_files:
                raise ValueError("No XML file found inside the downloaded ZIP archive.")

            xml_filename = xml_files[0]
            logger.info("Extracting XML file: %s", xml_filename)
            return archive.read(xml_filename).decode("utf-8")
