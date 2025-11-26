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
        def normalize(name: str) -> str:
            base = str(name).replace("\u00A0", " ").strip().lower()
            base = re.sub(r"[^\w]+", "_", base)
            base = re.sub(r"_+", "_", base).strip("_")
            return base

        df.columns = [normalize(c) for c in df.columns]
        return df

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        obj_cols = df.select_dtypes(include=["object"]).columns
        for col in obj_cols:
            series = df[col]
            as_str = series.astype(str)
            cleaned = (
                as_str.str.replace("\u00A0", " ", regex=False)
                .str.strip()
                .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
            )
            df[col] = cleaned.where(series.notna(), pd.NA)
        return df

    def _coerce_price_like(self, df: pd.DataFrame) -> pd.DataFrame:
        price_hints: Iterable[str] = ("price", "preis", "amount", "cost", "total", "umsatz", "betrag", "summe")
        for col in df.columns:
            col_lower = str(col).lower()
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                continue

            parsed = self._normalize_monetary_series(series)
            ratio = parsed.notna().mean()
            if any(hint in col_lower for hint in price_hints) or ratio >= self.min_numeric_ratio:
                df[col] = parsed
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

    def _normalize_monetary_series(self, series: pd.Series) -> pd.Series:
        def parse_value(val):
            if pd.isna(val):
                return pd.NA
            if isinstance(val, (int, float)):
                return val

            text = str(val).strip()
            if not text:
                return pd.NA

            text = re.sub(r"[€$£¥]", "", text)
            text = text.replace("\u00A0", "").replace(" ", "")

            if "," in text and "." in text:
                # Decide decimal separator by position (last separator is decimal)
                last_comma = text.rfind(",")
                last_dot = text.rfind(".")
                if last_comma > last_dot:
                    text = text.replace(".", "").replace(",", ".")
                else:
                    text = text.replace(",", "")
            else:
                text = text.replace(",", ".")

            try:
                return float(text)
            except Exception:
                return pd.NA

        return series.apply(parse_value)


# Export a ready-to-use singleton
cleaner = DataCleaner()


def clean_tabular(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience wrapper to clean tabular uploads:
    - standardizes column names (lower + snake_case)
    - trims string cells and normalizes empties to NA
    - coerces price/amount-like columns to numeric (€, thousand separators, comma decimals)
    - heuristically coerces other numeric/date-ish object columns
    - drops empty rows/columns and duplicates
    """
    return cleaner.clean(df)
