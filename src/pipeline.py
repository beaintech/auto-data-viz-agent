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


def detect_pnl_summary(df: pd.DataFrame) -> Dict:
    """
    Detect pre-aggregated P&L-style summaries (e.g., Revenue_Net/Cost_Net/Payroll_Net/Profit).
    """
    if df is None or df.empty:
        return {"looks_pnl": False, "columns": {}, "cards": {}}

    norm_map = {_normalize_column_key(c): c for c in df.columns}

    def pick(keys: Iterable[str]) -> Optional[str]:
        for k in keys:
            if k in norm_map:
                return norm_map[k]
        return None

    revenue_col = pick(["revenue_net", "revenue", "income", "sales", "umsatz_netto"])
    cost_col = pick(["cost_net", "cost", "expenses", "expense"])
    payroll_col = pick(["payroll_net", "payroll", "salaries", "salary"])
    profit_col = pick(["profit_after_tax", "profit_before_tax", "profit"])
    vat_col = pick(["vat_paid", "vat_amount", "total_vat_collected", "vat"])

    looks_pnl = any([revenue_col, cost_col, payroll_col, profit_col, vat_col])
    columns = {
        "revenue": revenue_col,
        "cost": cost_col,
        "payroll": payroll_col,
        "profit": profit_col,
        "vat": vat_col,
    }

    if not looks_pnl:
        return {"looks_pnl": False, "columns": columns, "cards": {}}

    def sum_col(col_name: Optional[str]) -> float:
        if not col_name or col_name not in df.columns:
            return 0.0
        return float(pd.to_numeric(df[col_name], errors="coerce").sum())

    revenue = sum_col(revenue_col)
    cost = sum_col(cost_col)
    payroll = sum_col(payroll_col)
    profit = sum_col(profit_col) if profit_col else revenue + cost + payroll
    vat_amount = sum_col(vat_col)
    vat_base = revenue / 1.19 if revenue else 0.0

    cards = {
        "revenue": revenue,
        "cost": cost,
        "payroll": payroll,
        "profit": profit,
        "vat_base": vat_base,
        "vat_amount": vat_amount,
    }

    return {"looks_pnl": True, "columns": columns, "cards": cards}


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
    # If a BK category and amount-like columns exist but detection failed, allow bookkeeping
    if not detection["looks_bookkeeping"]:
        bk_cols = [c for c in cleaned.columns if _normalize_column_key(c) in {"bk_category", "category"}]
        amount_like = [c for c in cleaned.columns if "amount" in _normalize_column_key(c)]
        if bk_cols and amount_like:
            detection["looks_bookkeeping"] = True
            detection["reason"] = "found bk_category with amount columns"
            detection["selected"]["amount"] = amount_like[0]
            detection["selected"]["description"] = bk_cols[0]
    pnl_summary = detect_pnl_summary(cleaned)
    chosen_mode = mode
    if mode == "auto":
        chosen_mode = "bookkeeping" if (detection["looks_bookkeeping"] or pnl_summary["looks_pnl"]) else "generic"
        reason = detection["reason"] if detection["looks_bookkeeping"] else "found P&L summary"
        _log(debug, logs, f"Auto-detected mode: {chosen_mode} ({reason})")

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
        if pnl_summary["looks_pnl"]:
            _log(debug, logs, f"Detected P&L summary columns: {pnl_summary['columns']}")
            result["mode_used"] = "bookkeeping"
            result["bookkeeping"] = {
                "summaries": {"pnl_summary": cleaned, "columns": pnl_summary["columns"]},
                "cards": pnl_summary["cards"],
            }
            return result
        _log(debug, logs, "Using generic mode: returning cleaned DataFrame without categorization or KPIs.")
        return result

    if not detection["looks_bookkeeping"]:
        if pnl_summary["looks_pnl"]:
            _log(debug, logs, f"Bookkeeping requested; using P&L summary columns: {pnl_summary['columns']}")
            result["bookkeeping"] = {
                "summaries": {"pnl_summary": cleaned, "columns": pnl_summary["columns"]},
                "cards": pnl_summary["cards"],
            }
            return result
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
        if pnl_summary["looks_pnl"]:
            result["bookkeeping"] = {
                "summaries": {"pnl_summary": standardized, "columns": pnl_summary["columns"]},
                "cards": pnl_summary["cards"],
            }
            return result
        _log(debug, logs, "Standardization removed bookkeeping structure; falling back to generic mode.")
        result["mode_used"] = "generic"
        return result

    if "amount" not in standardized.columns:
        amount_like = [c for c in standardized.columns if "amount" in _normalize_column_key(c)]
        if amount_like:
            standardized["amount"] = pd.to_numeric(standardized[amount_like[0]], errors="coerce")
            _log(debug, logs, f"Filled missing 'amount' from column {amount_like[0]}")
        else:
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
