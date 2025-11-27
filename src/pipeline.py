from __future__ import annotations

from pathlib import Path
import warnings
from typing import Dict, Iterable, List, Optional, Union

import pandas as pd

from .bookkeeping import (
    COLUMN_ALIASES,
    categorize_transactions,
    compute_bookkeeping_summaries,
    detect_recurring,
    standardize_columns,
    _normalize_column_key,
)
from .data_cleaner import cleaner


def _log(debug: bool, logs: List[str], msg: str) -> None:
    """Collect debug logs and optionally print for local debugging."""
    logs.append(msg)
    if debug:
        print(f"[pipeline] {msg}")


def _load_source(source: Union[str, Path, pd.DataFrame]) -> pd.DataFrame:
    if isinstance(source, pd.DataFrame):
        return source.copy()

    path = Path(source)
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError("Unsupported file type. Use .csv or .xlsx")


def _find_candidates(columns: Iterable[str], targets: Iterable[str]) -> List[str]:
    target_set = set(targets)
    matches: List[str] = []
    for col in columns:
        key = _normalize_column_key(col)
        if key in target_set:
            matches.append(col)
    return matches


def detect_bookkeeping_table(df: pd.DataFrame) -> Dict:
    """
    Heuristic detection of transaction-like tables.
    Returns candidates and a looks_bookkeeping flag to steer auto mode.
    """
    if df is None or df.empty:
        return {
            "looks_bookkeeping": False,
            "reason": "empty",
            "amount_candidates": [],
            "date_candidates": [],
            "description_candidates": [],
        }

    normalized_cols = {_normalize_column_key(c): c for c in df.columns}
    amount_candidates = _find_candidates(df.columns, COLUMN_ALIASES["amount"])
    date_candidates = _find_candidates(df.columns, COLUMN_ALIASES["date"])
    desc_candidates = _find_candidates(df.columns, COLUMN_ALIASES["description"])

    # Also consider numeric columns with money-ish names
    for col in df.columns:
        key = _normalize_column_key(col)
        if "amount" in key or "umsatz" in key or "betrag" in key or "total" in key:
            if col not in amount_candidates:
                amount_candidates.append(col)

    date_like = []
    for col in df.columns:
        series = df[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            date_like.append(col)
            continue
        if pd.api.types.is_numeric_dtype(series):
            continue
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Could not infer format.*")
            parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().mean() >= 0.6:
            date_like.append(col)
    if not date_candidates:
        date_candidates = date_like

    # sanity check: do we have a numeric amount column?
    numeric_amounts = []
    for col in amount_candidates:
        numerics = pd.to_numeric(df[col], errors="coerce")
        if numerics.notna().mean() >= 0.5:
            numeric_amounts.append(col)
    amount_candidates = numeric_amounts

    looks_bookkeeping = bool(amount_candidates) and (bool(date_candidates) or bool(desc_candidates))
    reason = "found amount/date/description pattern" if looks_bookkeeping else "missing amount/date pattern"

    selected = {
        "amount": amount_candidates[0] if amount_candidates else None,
        "date": date_candidates[0] if date_candidates else None,
        "description": desc_candidates[0] if desc_candidates else None,
    }

    return {
        "looks_bookkeeping": looks_bookkeeping,
        "reason": reason,
        "amount_candidates": amount_candidates,
        "date_candidates": date_candidates,
        "description_candidates": desc_candidates,
        "selected": selected,
        "normalized_columns": list(normalized_cols.keys()),
    }


def process_uploaded_file(
    source: Union[str, Path, pd.DataFrame],
    mode: str = "auto",
    tax_rate: float = 0.19,
    debug: bool = False,
) -> Dict:
    """
    Unified entrypoint for uploaded tables.

    Modes:
      - auto: detect bookkeeping vs generic based on columns
      - bookkeeping: force transaction pipeline (standardize -> categorize -> KPIs)
      - generic: clean only; do not categorize or compute KPIs
    """
    if mode not in {"auto", "bookkeeping", "generic"}:
        raise ValueError("mode must be 'auto', 'bookkeeping', or 'generic'")

    logs: List[str] = []
    raw = _load_source(source)
    cleaned = cleaner.clean(raw)
    _log(debug, logs, f"Cleaned DataFrame head:\n{cleaned.head(5)}")

    detection = detect_bookkeeping_table(cleaned)
    chosen_mode = mode
    if mode == "auto":
        chosen_mode = "bookkeeping" if detection["looks_bookkeeping"] else "generic"
        _log(debug, logs, f"Auto-detected mode: {chosen_mode} ({detection['reason']})")

    result: Dict[str, Optional[object]] = {
        "mode_requested": mode,
        "mode_used": chosen_mode,
        "detection": detection,
        "cleaned": cleaned,
        "df_final": cleaned,
        "bookkeeping": None,
        "logs": logs,
    }

    if chosen_mode != "bookkeeping":
        _log(debug, logs, "Using generic mode: returning cleaned DataFrame without categorization or KPIs.")
        return result

    if not detection["looks_bookkeeping"]:
        _log(debug, logs, "Requested bookkeeping but table does not look like transactions. Falling back to generic.")
        result["mode_used"] = "generic"
        return result

    standardized = standardize_columns(cleaned)
    bk_detect = detect_bookkeeping_table(standardized)
    _log(
        debug,
        logs,
        f"Detected bookkeeping columns after standardization: {bk_detect['selected']}, "
        f"candidates amount={bk_detect['amount_candidates']}, date={bk_detect['date_candidates']}",
    )

    if not bk_detect["looks_bookkeeping"]:
        _log(debug, logs, "Standardization removed bookkeeping structure; falling back to generic mode.")
        result["mode_used"] = "generic"
        return result

    if "amount" not in standardized.columns:
        _log(debug, logs, "Bookkeeping mode requested but no 'amount' column after standardization. Falling back to generic.")
        result["mode_used"] = "generic"
        result["df_final"] = standardized
        return result

    categorized = categorize_transactions(standardized)
    recurring = detect_recurring(categorized)
    summaries = compute_bookkeeping_summaries(recurring, tax_rate=tax_rate)
    _log(debug, logs, f"KPI cards: {summaries['cards']}")

    result["df_final"] = recurring
    result["bookkeeping"] = {"summaries": summaries, "cards": summaries.get("cards", {})}
    return result
