import warnings
import pandas as pd

def detect_time_column(df: pd.DataFrame):
    for col in df.columns:
        s = df[col]
        # If it's already datetime → accept
        if pd.api.types.is_datetime64_any_dtype(s):
            return col

        # ❌ Do NOT try to parse numeric columns as dates
        if pd.api.types.is_numeric_dtype(s):
            continue

        # Try parsing only object/string columns
        try:
            series = (
                s.astype(str)
                .str.replace("\u00A0", " ", regex=False)
                .str.replace("/", "-", regex=False)
                .str.strip()
            )
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Could not infer format.*")
                parsed = pd.to_datetime(series, errors="coerce")
            ok_ratio = parsed.notna().mean()
            # require enough valid dates and sensible years
            if ok_ratio > 0.8:
                years = parsed.dt.year.dropna()
                if (years.between(1900, 2100).mean() > 0.95) and (years.nunique() >= 3):
                    df[col] = parsed
                    return col
        except Exception:
            pass
    return None
