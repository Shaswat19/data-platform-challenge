"""Unit tests for steeleye.parser module."""

import xml.etree.ElementTree as ET  # noqa: N817

import pandas as pd
import pytest

from steeleye.parser import XMLParser


class TestXMLParser:
    """Test cases for XMLParser class."""

    def test_parse_valid_xml(self) -> None:
        """Test parsing valid XML into DataFrame."""
        xml_content = """<BizData>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
                <ModfdRcrd>
                    <FinInstrmGnlAttrbts>
                        <Id>TEST123</Id>
                        <FullNm>Test Instrument</FullNm>
                        <ClssfctnTp>TEST</ClssfctnTp>
                        <CmmdtyDerivInd>true</CmmdtyDerivInd>
                        <NtnlCcy>USD</NtnlCcy>
                    </FinInstrmGnlAttrbts>
                    <Issr>ISSUER001</Issr>
                </ModfdRcrd>
                <TermntdRcrd>
                    <FinInstrmGnlAttrbts>
                        <Id>TEST456</Id>
                        <FullNm>Another Test</FullNm>
                        <ClssfctnTp>ANOTHER</ClssfctnTp>
                        <CmmdtyDerivInd>false</CmmdtyDerivInd>
                        <NtnlCcy>EUR</NtnlCcy>
                    </FinInstrmGnlAttrbts>
                    <Issr>ISSUER002</Issr>
                </TermntdRcrd>
            </Document>
        </BizData>"""

        parser = XMLParser()
        df = parser.parse(xml_content)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

        # Check first row
        assert df.iloc[0]["FinInstrmGnlAttrbts.Id"] == "TEST123"
        assert df.iloc[0]["FinInstrmGnlAttrbts.FullNm"] == "Test Instrument"
        assert df.iloc[0]["FinInstrmGnlAttrbts.ClssfctnTp"] == "TEST"
        assert df.iloc[0]["FinInstrmGnlAttrbts.CmmdtyDerivInd"] == "true"
        assert df.iloc[0]["FinInstrmGnlAttrbts.NtnlCcy"] == "USD"
        assert df.iloc[0]["Issr"] == "ISSUER001"

        # Check second row
        assert df.iloc[1]["FinInstrmGnlAttrbts.Id"] == "TEST456"
        assert df.iloc[1]["FinInstrmGnlAttrbts.FullNm"] == "Another Test"

    def test_parse_empty_xml(self) -> None:
        """Test parsing XML with no records."""
        xml_content = """<BizData>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
            </Document>
        </BizData>"""

        parser = XMLParser()
        df = parser.parse(xml_content)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0
        assert list(df.columns) == [
            "FinInstrmGnlAttrbts.Id",
            "FinInstrmGnlAttrbts.FullNm",
            "FinInstrmGnlAttrbts.ClssfctnTp",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd",
            "FinInstrmGnlAttrbts.NtnlCcy",
            "Issr",
        ]

    def test_parse_missing_elements(self) -> None:
        """Test parsing XML with missing elements."""
        xml_content = """<BizData>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
                <ModfdRcrd>
                    <FinInstrmGnlAttrbts>
                        <Id>TEST123</Id>
                        <!-- Missing FullNm, etc. -->
                    </FinInstrmGnlAttrbts>
                    <!-- Missing Issr -->
                </ModfdRcrd>
            </Document>
        </BizData>"""

        parser = XMLParser()
        df = parser.parse(xml_content)

        assert len(df) == 1
        assert df.iloc[0]["FinInstrmGnlAttrbts.Id"] == "TEST123"
        assert df.iloc[0]["FinInstrmGnlAttrbts.FullNm"] == ""
        assert df.iloc[0]["Issr"] == ""

    def test_parse_invalid_xml(self) -> None:
        """Test parsing invalid XML raises ParseError."""
        xml_content = "<invalid>"

        parser = XMLParser()

        with pytest.raises(ET.ParseError):
            parser.parse(xml_content)

    def test_parse_record_without_fin_instrm_gnl_attrbts(self) -> None:
        """Test skipping records without FinInstrmGnlAttrbts."""
        xml_content = """<BizData>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02">
                <ModfdRcrd>
                    <!-- No FinInstrmGnlAttrbts -->
                    <Issr>ISSUER001</Issr>
                </ModfdRcrd>
                <ModfdRcrd>
                    <FinInstrmGnlAttrbts>
                        <Id>TEST123</Id>
                        <FullNm>Test</FullNm>
                    </FinInstrmGnlAttrbts>
                    <Issr>ISSUER002</Issr>
                </ModfdRcrd>
            </Document>
        </BizData>"""

        parser = XMLParser()
        df = parser.parse(xml_content)

        assert len(df) == 1
        assert df.iloc[0]["FinInstrmGnlAttrbts.Id"] == "TEST123"
