import re
import warnings
from typing import Iterable

import pandas as pd


class DataCleaner:
    """
    Robust tabular cleaner (non-mutating to callers).

    Order:
      1) standardize column names
      2) strip/normalize strings → NA
      3) monetary parsing
      4) generic numeric coercion
      5) date coercion (with dayfirst fallback)
      6) year-month normalization
      7) drop empty rows/cols
      8) drop rows missing key fields
      9) drop duplicates
    """

    def __init__(self, min_numeric_ratio: float = 0.7, min_date_ratio: float = 0.7):
        self.min_numeric_ratio = min_numeric_ratio
        self.min_date_ratio = min_date_ratio
        self.key_fields = {"date", "amount", "amount_net", "amount_gross"}
        
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None:
            raise ValueError("DataFrame is None")

        # Start pipeline
        cleaned = df.copy()
        cleaned = self._standardize_column_names(cleaned)
        cleaned = self._strip_strings(cleaned)
        cleaned = self._coerce_price_like(cleaned)
        cleaned = self._coerce_numeric(cleaned)
        cleaned = self._coerce_dates(cleaned)
        cleaned = self._normalize_year_month(cleaned)

        # Drop empty rows/cols
        cleaned = cleaned.dropna(how="all")
        cleaned = cleaned.dropna(axis=1, how="all")

        # Drop invalid key rows
        cleaned = self._drop_missing_key_fields(cleaned)

        # Drop duplicates
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
                as_str.str.replace(r"[\u00A0\u200b\u200c\u200d]", " ", regex=True)
                .str.strip()
                .replace(
                    {
                        "": pd.NA,
                        "nan": pd.NA,
                        "None": pd.NA,
                        "none": pd.NA,
                        "null": pd.NA,
                        "n/a": pd.NA,
                        "na": pd.NA,
                    }
                )
            )
            df[col] = cleaned.where(series.notna(), pd.NA)
        return df

    def _coerce_price_like(self, df: pd.DataFrame) -> pd.DataFrame:
        price_hints: Iterable[str] = (
            "price",
            "preis",
            "amount",
            "cost",
            "total",
            "umsatz",
            "betrag",
            "summe",
            "gross",
            "net",
            "vat",
            "tax",
            "payroll",
        )
        for col in df.columns:
            col_lower = str(col).lower()
            series = df[col]
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
                series.str.replace(r"[\u00A0\s]", "", regex=True)
                .str.replace(",", ".", regex=False)
            )
            numerics = pd.to_numeric(normalized, errors="coerce")
            if numerics.notna().mean() >= self.min_numeric_ratio:
                df[col] = numerics
        return df

    def _coerce_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            name = col.lower()
            if not any(hint in name for hint in ["date", "datum", "posted"]):
                continue

            series = df[col]
            if pd.api.types.is_datetime64_any_dtype(series):
                parsed = pd.to_datetime(series, errors="coerce")
            else:
                series = (
                    series.astype(str)
                    .str.replace(r"[\u00A0]", " ", regex=True)
                    .str.replace("/", "-", regex=False)
                    .str.strip()
                )
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Could not infer format.*")
                    parsed = pd.to_datetime(series, errors="coerce")
                    parsed = parsed.fillna(pd.to_datetime(series, errors="coerce", dayfirst=True))
                    for fmt in ("%d.%m.%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M"):
                        parsed = parsed.fillna(pd.to_datetime(series, errors="coerce", format=fmt))
            df[col] = parsed.dt.normalize().astype("datetime64[ns]")
        return df

    def _normalize_year_month(self, df: pd.DataFrame) -> pd.DataFrame:
        ym_cols = [c for c in df.columns if "yearmonth" in c.lower() or "year_month" in c.lower() or "year-month" in c.lower()]
        for col in ym_cols:
            series = (
                df[col]
                .astype(str)
                .str.replace("\u00A0", " ", regex=False)
                .str.replace("/", "-", regex=False)
                .str.strip()
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Could not infer format.*")
                parsed = pd.to_datetime(series, errors="coerce")
                parsed = parsed.fillna(pd.to_datetime(series, errors="coerce", format="%b-%Y", dayfirst=False))
                parsed = parsed.fillna(pd.to_datetime(series, errors="coerce", format="%m-%Y"))
                parsed = parsed.fillna(pd.to_datetime(series, errors="coerce", format="%Y-%m"))
            df[col] = parsed.dt.to_period("M").astype(str).where(parsed.notna(), pd.NA)
        return df

    def _drop_missing_key_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # Always require date if present
        if "date" in df.columns:
            df = df.dropna(subset=["date"], how="any")

        amount_like = [c for c in df.columns if any(hint in c.lower() for hint in ["amount", "gross", "net", "betrag"])]
        amount_like = list(dict.fromkeys(amount_like))

        if not amount_like:
            return df

        # Prefer strict drop on amount_gross if present (user expectation)
        if "amount_gross" in amount_like:
            df = df.dropna(subset=["amount_gross"], how="any")

        # Then ensure at least one other amount-like field is present
        remaining = [c for c in amount_like if c != "amount_gross"]
        if remaining:
            if len(remaining) == 1:
                df = df.dropna(subset=remaining, how="any")
            else:
                df = df.dropna(subset=remaining, how="all")
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


cleaner = DataCleaner()


def clean_tabular(df: pd.DataFrame) -> pd.DataFrame:
    """Convenience wrapper around DataCleaner.clean."""
    return cleaner.clean(df)
