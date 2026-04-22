"""Module for applying business transformations to the instruments DataFrame."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


class DataTransformer:
    """Applies derived-column transformations to the instruments ``pd.DataFrame``.

    Adds:
    - ``a_count``: number of lowercase ``"a"`` characters
      in ``FinInstrmGnlAttrbts.FullNm``.
    - ``contains_a``: ``"YES"`` when ``a_count > 0``, ``"NO"`` otherwise.

    Example::

        transformer = DataTransformer()
        df = transformer.transform(df)
    """

    _FULLNM_COL = "FinInstrmGnlAttrbts.FullNm"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ``a_count`` and ``contains_a`` columns to the DataFrame.

        Args:
            df: Input DataFrame expected to contain the column
                ``FinInstrmGnlAttrbts.FullNm``.

        Returns:
            A new ``pd.DataFrame`` with two additional columns appended.

        Raises:
            KeyError: If ``FinInstrmGnlAttrbts.FullNm`` is not present in ``df``.
        """
        if self._FULLNM_COL not in df.columns:
            raise KeyError(
                f"Expected column '{self._FULLNM_COL}' not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )

        logger.info("Applying 'a_count' and 'contains_a' transformations.")

        result = df.copy()
        result["a_count"] = result[self._FULLNM_COL].apply(
            lambda x: str(x).count("a") if pd.notna(x) and x != "" else 0
        )
        result["contains_a"] = result["a_count"].apply(
            lambda count: "YES" if count > 0 else "NO"
        )

        logger.info("Transformation complete. %d rows processed.", len(result))
        return result
