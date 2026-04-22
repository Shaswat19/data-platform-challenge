"""Unit tests for steeleye.transformer module."""

import pandas as pd
import pytest
from steeleye.transformer import DataTransformer


class TestDataTransformer:
    """Test cases for DataTransformer class."""

    def test_transform_success(self) -> None:
        """Test successful transformation with valid DataFrame."""
        df = pd.DataFrame(
            {
                "FinInstrmGnlAttrbts.Id": ["ID1", "ID2", "ID3"],
                "FinInstrmGnlAttrbts.FullNm": ["Apple", "Banana", "Cherry"],
                "FinInstrmGnlAttrbts.ClssfctnTp": ["A", "B", "C"],
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": ["true", "false", "true"],
                "FinInstrmGnlAttrbts.NtnlCcy": ["USD", "EUR", "GBP"],
                "Issr": ["ISS1", "ISS2", "ISS3"],
            }
        )

        transformer = DataTransformer()
        result = transformer.transform(df)

        assert len(result) == 3
        assert "a_count" in result.columns
        assert "contains_a" in result.columns

        # Check a_count
        assert result["a_count"].tolist() == [1, 3, 0]  # Apple:1, Banana:3, Cherry:0

        # Check contains_a
        assert result["contains_a"].tolist() == ["YES", "YES", "NO"]

        # Original columns preserved
        assert "FinInstrmGnlAttrbts.FullNm" in result.columns

    def test_transform_missing_column(self) -> None:
        """Test error when required column is missing."""
        df = pd.DataFrame(
            {
                "FinInstrmGnlAttrbts.Id": ["ID1"],
                # Missing FinInstrmGnlAttrbts.FullNm
                "FinInstrmGnlAttrbts.ClssfctnTp": ["A"],
            }
        )

        transformer = DataTransformer()

        with pytest.raises(
            KeyError, match="Expected column 'FinInstrmGnlAttrbts.FullNm' not found"
        ):
            transformer.transform(df)

    def test_transform_empty_dataframe(self) -> None:
        """Test transformation on empty DataFrame."""
        df = pd.DataFrame(
            columns=[
                "FinInstrmGnlAttrbts.Id",
                "FinInstrmGnlAttrbts.FullNm",
                "FinInstrmGnlAttrbts.ClssfctnTp",
                "FinInstrmGnlAttrbts.CmmdtyDerivInd",
                "FinInstrmGnlAttrbts.NtnlCcy",
                "Issr",
            ]
        )

        transformer = DataTransformer()
        result = transformer.transform(df)

        assert len(result) == 0
        assert "a_count" in result.columns
        assert "contains_a" in result.columns

    def test_transform_nan_values(self) -> None:
        """Test transformation handles NaN and empty strings."""
        df = pd.DataFrame(
            {
                "FinInstrmGnlAttrbts.FullNm": ["Apple", None, "", "Banana"],
            }
        )

        transformer = DataTransformer()
        result = transformer.transform(df)

        assert result["a_count"].tolist() == [1, 0, 0, 3]
        assert result["contains_a"].tolist() == ["YES", "NO", "NO", "YES"]

    def test_transform_case_sensitivity(self) -> None:
        """Test that 'a' count is case-sensitive (only lowercase)."""
        df = pd.DataFrame(
            {
                "FinInstrmGnlAttrbts.FullNm": ["Apple", "APPLE", "aPpLe"],
            }
        )

        transformer = DataTransformer()
        result = transformer.transform(df)

        assert result["a_count"].tolist() == [1, 0, 1]  # Only lowercase 'a'
        assert result["contains_a"].tolist() == ["YES", "NO", "YES"]
