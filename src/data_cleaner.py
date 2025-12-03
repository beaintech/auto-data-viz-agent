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
      5) date coercion (with dayfirst fallback, localized to DE)
      6) year-month normalization
      7) drop empty rows/cols
      8) drop empty finance rows
      9) drop duplicates
    """

    def __init__(self, min_numeric_ratio: float = 0.7, min_date_ratio: float = 0.7):
        self.min_numeric_ratio = min_numeric_ratio
        self.min_date_ratio = min_date_ratio
        self.key_fields = {"date", "amount", "amount_net", "amount_gross"}
        self.local_timezone = "Europe/Berlin"
        
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
        cleaned = self._normalize_hr_fields(cleaned)
        cleaned = self._normalize_country(cleaned)


        # Drop empty rows/cols
        cleaned = cleaned.dropna(how="all")
        cleaned = cleaned.dropna(axis=1, how="all")

        # Drop invalid key rows
        cleaned = self._drop_missing_key_fields(cleaned)

        # Drop duplicates
        cleaned = cleaned.drop_duplicates()

        # --- enforce chronological order ---
        if "date" in cleaned.columns:
            # try to convert to datetime for correct sorting
            dt = pd.to_datetime(cleaned["date"], errors="coerce")
            cleaned["date"] = dt
            cleaned = cleaned.sort_values("date", ascending=True)

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
            localized = self._localize_datetime(parsed)
            df[col] = localized.dt.normalize().dt.tz_localize(None)
        return df

    def _localize_datetime(self, series: pd.Series) -> pd.Series:
        """
        Convert parsed datetime series to Germany local time without losing time-of-day.
        """
        if series.dt.tz is None:
            return series.dt.tz_localize(self.local_timezone, nonexistent="NaT", ambiguous="NaT")
        return series.dt.tz_convert(self.local_timezone)

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
        """
        Only drop placeholder rows for financial-looking tables:
        rows where the date is missing AND all amount-like columns are missing.
        Non-financial tables skip this step entirely.
        """
        finance_markers = {"amount", "amount_net", "amount_gross", "vat_amount"}
        if not (set(df.columns) & finance_markers):
            return df

        amount_like = [c for c in df.columns if any(hint in c.lower() for hint in ["amount", "gross", "net", "betrag"])]
        amount_like = list(dict.fromkeys(amount_like))

        if "date" not in df.columns or not amount_like:
            return df

        missing_date = df["date"].isna()
        missing_amounts = df[amount_like].isna().all(axis=1)
        keep_mask = ~(missing_date & missing_amounts)
        return df.loc[keep_mask]

    def _normalize_monetary_series(self, series: pd.Series) -> pd.Series:
        def parse_value(val):
            if pd.isna(val):
                return pd.NA
            if isinstance(val, (int, float)):
                return val

            text = str(val).strip()
            if not text:
                return pd.NA

            # 去掉货币符号
            text = re.sub(r"[€$£¥]", "", text)
            # 去掉不间断空格和普通空格
            text = text.replace("\u00A0", "").replace(" ", "")
            # 关键：去掉所有字母等非数字/符号（吃掉 "ca.", "EUR" 等）
            text = re.sub(r"[^0-9,.\-]", "", text)

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

    def _normalize_hr_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_lower = {c.lower(): c for c in df.columns}

        # 性别：F/M
        for key in ["geschlecht", "gender"]:
            if key in cols_lower:
                col = cols_lower[key]
                mapping = {
                    "female": "Female", "f": "Female", "w": "Female", "frau": "Female",
                    "male": "Male", "m": "Male", "mann": "Male"
                }
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .replace(mapping)
                    .where(df[col].notna(), pd.NA)
                )

        # 合同类型：full_time / part_time / temporary / other
        for key in ["vertragsart", "employment_type", "contract_type"]:
            if key in cols_lower:
                col = cols_lower[key]
                s = df[col].astype(str).str.strip().str.lower()
                def map_contract(x: str) -> str | type(pd.NA):
                    if x in ("nan", ""):
                        return pd.NA
                    if "full" in x:
                        return "full_time"
                    if "part" in x:
                        return "part_time"
                    if "temp" in x:
                        return "temporary"
                    return x  # keep as-is for now

                df[col] = s.map(map_contract)

        # 货币：统一成大写代码
        for key in ["waehrung", "currency"]:
            if key in cols_lower:
                col = cols_lower[key]
                mapping = {
                    "eur": "EUR",
                    "€": "EUR",
                    "euro": "EUR",
                }
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .str.lower()
                    .replace(mapping)
                    .str.upper()
                    .where(df[col].notna(), pd.NA)
                )

        # 支付频率：统一成 monthly / yearly 等
        for key in ["zahlfrequenz", "pay_frequency", "payment_frequency"]:
            if key in cols_lower:
                col = cols_lower[key]
                s = df[col].astype(str).str.strip().str.lower()
                def map_freq(x: str) -> str | type(pd.NA):
                    if x in ("nan", ""):
                        return pd.NA
                    if any(word in x for word in ["monat", "monthly", "monatl"]):
                        return "monthly"
                    if any(word in x for word in ["jahr", "year", "annual"]):
                        return "yearly"
                    return x

                df[col] = s.map(map_freq)

        for key in ["status", "employment_status"]:
            if key in cols_lower:
                col = cols_lower[key]

                s = df[col].astype(str).str.strip().str.lower()

                def map_status(x: str):
                    if x in ("", "nan"):
                        return pd.NA
                    # active 族
                    if x in ("active", "aktiv", "true", "yes", "y", "1"):
                        return "active"
                    # inactive 族
                    if x in ("inactive", "inaktiv", "false", "no", "n", "0"):
                        return "inactive"
                    # 其它值原样保留（以防有第三种状态，比如 "on_leave"）
                    return x

                df[col] = s.map(map_status)

        return df
    
    def _normalize_country(self, df: pd.DataFrame) -> pd.DataFrame:
        # detect a possible country column
        country_candidates = [c for c in df.columns if c.lower() in ["land", "country", "location", "standort"]]
        if not country_candidates:
            return df

        col = country_candidates[0]

        mapping = {
            "de": "Germany",
            "ger": "Germany",
            "deutschland": "Germany",
            "at": "Austria",
            "österreich": "Austria",
            "osterreich": "Austria",
            "ch": "Switzerland",
            "schweiz": "Switzerland",
            "it": "Italy",
            "italien": "Italy",
            "es": "Spain",
            "spanien": "Spain",
            "spain": "Spain",
            "usa": "United States",
            "us": "United States",
            "uk": "United Kingdom",
            "england": "United Kingdom",
            "in": "India",
            "indien": "India",
            "india": "India",
            "cn": "China",
            "china": "China",
            "kr": "South Korea",
            "südkorea": "South Korea",
            "suedkorea": "South Korea",
        }

        s = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r"[\u00A0\s]+", "", regex=True)
            .str.lower()
        )

        df[col] = s.replace(mapping).where(df[col].notna(), pd.NA)

        return df

cleaner = DataCleaner()


def clean_tabular(df: pd.DataFrame) -> pd.DataFrame:
    """Convenience wrapper around DataCleaner.clean."""
    return cleaner.clean(df)
