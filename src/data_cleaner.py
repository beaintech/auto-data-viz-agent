import re
import warnings
from typing import Iterable

import pandas as pd


class DataCleaner:
    """
    Lightweight data cleaning model for tabular uploads.

    - Normalizes column names and trims whitespace
    - Strips strings + converts empty strings to NA
    - Coerces price/amount-like columns to numerics (handles €, commas)
    - Optionally coerces other numeric/date-ish object columns heuristically
    - Drops fully empty rows/cols and duplicates
    """

    def __init__(self, min_numeric_ratio: float = 0.7, min_date_ratio: float = 0.7):
        self.min_numeric_ratio = min_numeric_ratio
        self.min_date_ratio = min_date_ratio

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            raise ValueError("DataFrame is None")

        cleaned = df.copy()
        cleaned = self._standardize_column_names(cleaned)
        cleaned = self._strip_strings(cleaned)
        cleaned = self._coerce_price_like(cleaned)
        cleaned = self._coerce_numeric(cleaned)
        cleaned = self._coerce_dates(cleaned)
        cleaned = cleaned.dropna(how="all")
        cleaned = cleaned.dropna(axis=1, how="all")
        cleaned = cleaned.drop_duplicates()
        return cleaned

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object"]).columns
        for col in obj_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("\u00A0", " ", regex=False)  # non-breaking space -> space
                .str.strip()
                .replace({"": pd.NA})
            )
        return df

    def _coerce_price_like(self, df: pd.DataFrame) -> pd.DataFrame:
        price_hints: Iterable[str] = ("price", "preis", "amount", "cost", "total")
        for col in df.columns:
            col_lower = str(col).lower()
            if any(hint in col_lower for hint in price_hints):
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[€$£¥]", "", regex=True)
                    .str.replace("\u00A0", "", regex=False)
                    .str.replace(" ", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _coerce_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object"]).columns
        for col in obj_cols:
            series = df[col].astype(str)
            normalized = (
                series.str.replace("\u00A0", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            numerics = pd.to_numeric(normalized, errors="coerce")
            if numerics.notna().mean() >= self.min_numeric_ratio:
                df[col] = numerics
        return df

    def _coerce_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object"]).columns
        for col in obj_cols:
            series = (
                df[col]
                .astype(str)
                .str.replace("\u00A0", " ", regex=False)
                .str.replace("/", "-", regex=False)  # handle 2024/01/01 style
                .str.strip()
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Could not infer format.*")
                parsed = pd.to_datetime(series, errors="coerce")
            if parsed.notna().mean() >= self.min_date_ratio:
                df[col] = parsed
        return df


# Export a ready-to-use singleton
cleaner = DataCleaner()
