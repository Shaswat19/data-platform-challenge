"""Module for parsing ESMA FIRDS XML content into a Pandas DataFrame."""

import logging
import xml.etree.ElementTree as ET  # noqa: N817
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# The Document element inside the ZIP XML redeclares its own namespace.
# Using Clark notation {uri}Tag is the only reliable way to match elements
# when the root element belongs to a different namespace (head.003.001.01)
# than the data elements (auth.036.001.02). The NS-dict approach in findall()
# silently returns 0 results in this cross-namespace scenario.
_AUTH_NS = "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"

COLUMNS = [
    "FinInstrmGnlAttrbts.Id",
    "FinInstrmGnlAttrbts.FullNm",
    "FinInstrmGnlAttrbts.ClssfctnTp",
    "FinInstrmGnlAttrbts.CmmdtyDerivInd",
    "FinInstrmGnlAttrbts.NtnlCcy",
    "Issr",
]


def _tag(local: str) -> str:
    """Return a Clark-notation tag string for the auth.036 namespace.

    Args:
        local: Local element name e.g. ``"Id"``, ``"FullNm"``.

    Returns:
        Clark-notation string e.g. ``"{urn:iso:...}Id"``.
    """
    return f"{{{_AUTH_NS}}}{local}"


class XMLParser:
    """Parses ESMA FIRDS XML instrument data into a structured ``pd.DataFrame``.

    The real-world ESMA ZIP files use a dual-namespace structure:
    - Outer ``<BizData>`` is in ``urn:iso:std:iso:20022:tech:xsd:head.003.001.01``
    - Inner ``<Document>`` redeclares ``urn:iso:std:iso:20022:tech:xsd:auth.036.001.02``

    Because of this, ``findall()`` with a namespace-prefix dict silently
    returns zero results when called from the outer root. This parser uses
    Clark notation (``{uri}LocalName``) via ``ET.Element.iter()`` which
    correctly traverses the entire tree regardless of namespace boundaries.

    Handles both ``ModfdRcrd`` and ``TermntdRcrd`` record types.

    Example::

        parser = XMLParser()
        df = parser.parse(xml_string)
    """

    _RECORD_TAGS = {_tag("ModfdRcrd"), _tag("TermntdRcrd")}

    def parse(self, xml_content: str) -> pd.DataFrame:
        """Parse raw XML string and return a DataFrame with required columns.

        Args:
            xml_content: Raw XML string from the ESMA FIRDS ZIP archive.

        Returns:
            ``pd.DataFrame`` with columns defined in :data:`COLUMNS`.

        Raises:
            ET.ParseError: If ``xml_content`` is not valid XML.
        """
        logger.info("Parsing XML content (%d chars).", len(xml_content))
        root = ET.fromstring(xml_content)
        records: list[dict[str, Any]] = []

        for element in root.iter():
            if element.tag not in self._RECORD_TAGS:
                continue

            attribs = element.find(_tag("FinInstrmGnlAttrbts"))
            if attribs is None:
                logger.debug("Skipping record — FinInstrmGnlAttrbts not found.")
                continue

            records.append(
                {
                    "FinInstrmGnlAttrbts.Id": self._text(attribs, "Id"),
                    "FinInstrmGnlAttrbts.FullNm": self._text(attribs, "FullNm"),
                    "FinInstrmGnlAttrbts.ClssfctnTp": self._text(attribs, "ClssfctnTp"),
                    "FinInstrmGnlAttrbts.CmmdtyDerivInd": self._text(
                        attribs, "CmmdtyDerivInd"
                    ),
                    "FinInstrmGnlAttrbts.NtnlCcy": self._text(attribs, "NtnlCcy"),
                    "Issr": self._text(element, "Issr"),
                }
            )

        logger.info("Parsed %d instrument records.", len(records))
        return pd.DataFrame(records, columns=COLUMNS)

    @staticmethod
    def _text(element: ET.Element, local: str) -> str:
        """Extract text from a direct child using Clark notation.

        Args:
            element: Parent ``ET.Element`` to search within.
            local: Local tag name without namespace e.g. ``"Id"``.

        Returns:
            Stripped text content, or ``""`` if absent or empty.
        """
        found = element.find(_tag(local))
        return found.text.strip() if found is not None and found.text else ""
