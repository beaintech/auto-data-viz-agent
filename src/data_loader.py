import io, requests, csv
import pandas as pd
from typing import Tuple

def _detect_delimiter(raw: bytes) -> str:
    head = raw[:2048].decode("utf-8", errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(head, delimiters=[",", ";"])
        if dialect.delimiter in (",", ";"):
            return dialect.delimiter
    except Exception:
        pass
    # 回退：谁更多用谁
    return ";" if head.count(";") > head.count(",") else ","

def _clean_price_column(df: pd.DataFrame) -> pd.DataFrame:
    if "Preis" in df.columns:
        df["Preis"] = (
            df["Preis"]
            .astype(str)
            .str.replace(r"[€\s]", "", regex=True)   # 去掉 € 和空格
            .str.replace(",", ".", regex=False)      # 逗号小数换成点
        )
        df["Preis"] = pd.to_numeric(df["Preis"], errors="coerce")
    return df

import pandas as pd

def _strip_and_empty_to_nan(df: pd.DataFrame) -> pd.DataFrame:
    # strip whitespace in column names
    df.columns = [str(c).strip() for c in df.columns]

    # strip whitespace in all string/object cells and convert "" -> NaN
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = (
            df[c]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)  # non-breaking space -> space
            .str.strip()
            .replace({"": pd.NA})
        )

    # optional: drop rows/cols that are entirely empty after stripping
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df

def load_from_upload(uploaded) -> Tuple[pd.DataFrame, str]:
    name = uploaded.name
    if not name.lower().endswith((".csv", ".xlsx")):
        raise ValueError("Unsupported file type. Use .csv or .xlsx")

    # Excel 直接读
    if name.lower().endswith(".xlsx"):
        df = pd.read_excel(uploaded)
        df = _clean_price_column(df)
        df = _strip_and_empty_to_nan(df)
        return df, name
    
    # CSV：先读原始字节，再判定分隔符
    raw = uploaded.getvalue()  # Streamlit UploadedFile -> bytes
    sep = _detect_delimiter(raw)

    if sep == ",":
        # 逗号 CSV：直接按逗号读（不做任何清洗）
        df = pd.read_csv(io.BytesIO(raw), sep=",", engine="python")
        df = _clean_price_column(df)
        df = _strip_and_empty_to_nan(df)

        # —— 可选：若你“必须统一转成分号”，取消下面三行的注释即可 —— 
        # buf = io.StringIO()
        # df.to_csv(buf, index=False, sep=";")  # 用 pandas 安全转分隔符，不会影响小数逗号
        # df = pd.read_csv(io.StringIO(buf.getvalue()), sep=";", engine="python")

        return df, name

    else:
        # 分号 CSV：按分号读
        df = pd.read_csv(io.BytesIO(raw), sep=";", engine="python")
        df = _clean_price_column(df)
        df = _strip_and_empty_to_nan(df)

    df = _clean_price_column(df)
    df = _strip_and_empty_to_nan(df)
    return df, name

def load_from_gsheet_url(url: str) -> Tuple[pd.DataFrame, str]:
    # Expecting a CSV export URL
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    content = r.content
    df = pd.read_csv(io.BytesIO(content))
    df = _clean_price_column(df)
    df = _strip_and_empty_to_nan(df)
    return df, "GoogleSheets.csv"

def brief_summary(df: pd.DataFrame) -> str:
    parts = []
    parts.append(f"Rows: {len(df)}, Columns: {len(df.columns)}")
    parts.append("Columns: " + ", ".join(map(str, df.columns[:20])) + ("..." if len(df.columns) > 20 else ""))
    nulls = df.isna().mean().round(3).to_dict()
    top_nulls = sorted(nulls.items(), key=lambda x: x[1], reverse=True)[:5]
    parts.append("Top missing-value columns: " + ", ".join(f"{k}: {v:.0%}" for k,v in top_nulls))
    return "\n".join(parts)