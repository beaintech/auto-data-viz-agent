from typing import List, Optional
import pandas as pd
import re
from pydantic import BaseModel

from .bookkeeping import _normalize_column_key


class ChartSpec(BaseModel):
    kind: str                 # line | bar | pie | waterfall
    x: Optional[str] = None
    y: Optional[str] = None
    category: Optional[str] = None
    agg: str = "sum"
    title: Optional[str] = None


# Allowed axis keywords
X_TIME_KEYS = ("date", "yearmonth", "year_month", "year-month")
X_CATEGORY_KEYS = ("customer", "project", "bk_category", "category", "type")
Y_MONEY_KEYS = ("amount", "net", "gross", "revenue", "income", "sales", "cost", "expense", "profit", "vat")
ID_LIKE_KEYS = ("iban", "document", "doc", "id", "nr", "no")


def _is_id_like(col: str) -> bool:
    key = _normalize_column_key(col)
    return any(k in key for k in ID_LIKE_KEYS)


def _is_time_col(df: pd.DataFrame, col: str) -> bool:
    if col not in df.columns:
        return False
    key = _normalize_column_key(col)
    if any(token in key for token in X_TIME_KEYS):
        return True
    return pd.api.types.is_datetime64_any_dtype(df[col])


def get_valid_x_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []
    for col in df.columns:
        if _is_id_like(col):
            continue
        key = _normalize_column_key(col)
        if any(token in key for token in X_TIME_KEYS + X_CATEGORY_KEYS):
            cols.append(col)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            cols.append(col)
    # Preserve original order; prioritize time columns first
    time_cols = [c for c in cols if _is_time_col(df, c)]
    other_cols = [c for c in cols if c not in time_cols]
    prioritized = time_cols + other_cols

    # Fallback: if nothing matched heuristics, pick first non-id column(s)
    if not prioritized:
        fallback = [c for c in df.columns if not _is_id_like(c)]
        prioritized = fallback

    # Ensure remaining non-id columns are also considered (for categorical pies)
    for col in df.columns:
        if col not in prioritized and not _is_id_like(col):
            prioritized.append(col)

    return prioritized


def get_valid_y_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []
    for col in df.columns:
        if _is_id_like(col):
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        key = _normalize_column_key(col)
        if any(token in key for token in Y_MONEY_KEYS):
            cols.append(col)
    # Fallback: if no money-like numeric columns, accept any numeric non-id columns
    if not cols:
        for col in df.columns:
            if _is_id_like(col):
                continue
            if pd.api.types.is_numeric_dtype(df[col]):
                cols.append(col)
    # Last resort: allow a row-count pseudo column to enable charts
    if not cols:
        cols = ["__row_count__"]
    return cols


def filter_x_candidates(df: pd.DataFrame, x_cols: List[str]) -> List[str]:
    """Drop X columns with fewer than 2 unique values to avoid flat charts."""
    filtered: List[str] = []
    for col in x_cols:
        if col not in df.columns:
            continue
        if df[col].nunique(dropna=True) >= 2:
            filtered.append(col)
    return filtered


def _series_almost_equal(a: pd.Series, b: pd.Series) -> bool:
    """Heuristic: treat two numeric series as duplicates if their difference is zero (ignoring NaNs)."""
    if a.empty and b.empty:
        return True
    diff = (pd.to_numeric(a, errors="coerce") - pd.to_numeric(b, errors="coerce")).abs()
    # If all non-null differences are zero, consider equal
    non_null = diff.dropna()
    return not non_null.empty and (non_null == 0).all()


def _simplify_name(name: str) -> str:
    key = _normalize_column_key(name)
    # remove common suffixes to catch near-duplicates like amount_total, amount_net
    key = re.sub(r"_(total|gross|net)$", "", key)
    return key


def deduplicate_y_candidates(df: pd.DataFrame, y_cols: List[str]) -> List[str]:
    """
    Remove near-duplicate Y columns (same values or very similar names).
    Keeps the first occurrence.
    """
    kept: List[str] = []
    simplified_keys: List[str] = []
    for col in y_cols:
        if col == "__row_count__":
            if "__row_count__" not in kept:
                kept.append(col)
            continue
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        simple = _simplify_name(col)

        is_dup = False
        for kept_col, kept_key in zip(kept, simplified_keys):
            other = pd.to_numeric(df[kept_col], errors="coerce")
            if _series_almost_equal(series, other):
                is_dup = True
                break
            if simple == kept_key or simple in kept_key or kept_key in simple:
                is_dup = True
                break
        if not is_dup:
            kept.append(col)
            simplified_keys.append(simple)
    return kept


def build_chart_data(df: pd.DataFrame, x: str, y: str) -> ChartSpec:
    """
    Build a ChartSpec for a given x/y following rule:
      - time on x → line
      - else category → bar
    """
    kind = "line" if _is_time_col(df, x) else "bar"
    if y == "__row_count__":
        title = f"Count over {x}" if kind == "line" else f"Count by {x}"
    else:
        title = f"{y} over {x}" if kind == "line" else f"{y} by {x}"
    return ChartSpec(kind=kind, x=x, y=y, agg="sum", title=title)


def generate_all_charts(df: pd.DataFrame) -> List[ChartSpec]:
    x_cols = filter_x_candidates(df, get_valid_x_columns(df))
    y_cols = deduplicate_y_candidates(df, get_valid_y_columns(df))
    charts: List[ChartSpec] = []

    # Simple heuristic: consider a dataset "financial" if it has money markers
    money_markers = {"amount", "amount_net", "amount_gross", "vat_amount"}
    is_financial = bool(set(df.columns) & money_markers)

    for x in x_cols:
        for y in y_cols:
            if x != y:
                spec = build_chart_data(df, x, y)
                charts.append(spec)
                # Prefer pie on categorical/grouped axes with manageable cardinality
                if not _is_time_col(df, x):
                    try:
                        n_unique = df[x].nunique(dropna=False)
                    except Exception:
                        n_unique = None
                    if n_unique is None or n_unique <= 20:
                        try:
                            # Attempt pie; if later rendering fails, waterfall remains as a backup
                            title = (f"Count share by {x}" if y == "__row_count__" else f"{y} share by {x}")
                            charts.append(
                                ChartSpec(
                                    kind="pie",
                                    category=x,
                                    y=y if y != "__row_count__" else "__row_count__",
                                    agg="sum",
                                    title=title,
                                )
                            )
                        except Exception:
                            pass
                        # Always add a waterfall fallback for finance datasets
                        if is_financial and y != "__row_count__":
                            charts.append(
                                ChartSpec(
                                    kind="waterfall",
                                    category=x,
                                    y=y,
                                    agg="sum",
                                    title=f"{y} contributions by {x}",
                                )
                            )
    return charts


def suggest_charts(df: pd.DataFrame) -> List[ChartSpec]:
    """
    Backward-compatible wrapper used by the app.
    """
    return generate_all_charts(df)
