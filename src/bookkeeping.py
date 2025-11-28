import re
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from .data_cleaner import clean_tabular

# Common column aliases mapped to canonical bookkeeping names
COLUMN_ALIASES: Dict[str, Iterable[str]] = {
    "date": ("date", "datum", "buchungsdatum", "valutadatum", "transaction_date", "posted_date"),
    "amount": ("amount", "betrag", "umsatz", "value", "summe", "payment_amount"),
    "bk_category": ("bk_category", "bk category", "category", "kategorie"),
    "description": ("description", "verwendungszweck", "memo", "text", "notes", "zweck", "buchungstext"),
    "currency": ("currency", "waehrung", "währung", "cur", "curr"),
    "iban": ("iban", "account", "konto", "kontonummer", "account_number"),
}

RULES: List[Dict] = [
    {"name": "payroll_keywords", "category": "payroll", "keywords": ["salary", "payroll", "gehalt", "lohn", "wage"]},
    {"name": "tax_keywords", "category": "tax", "keywords": ["tax", "vat", "ust", "mwst", "finanzamt"]},
    {"name": "supermarkets", "category": "cost", "keywords": ["rewe", "aldi", "lidl", "edeka", "kaufland", "carrefour", "tesco", "supermarkt"]},
    {"name": "online_shops", "category": "cost", "keywords": ["amazon", "zalando", "etsy", "otto", "ikea", "decathlon"]},
    {"name": "saas_cloud", "category": "cost", "keywords": ["aws", "azure", "gcp", "google cloud", "digitalocean", "vercel", "heroku"]},
    {"name": "delivery", "category": "cost", "keywords": ["uber eats", "lieferando", "doordash", "deliveroo"]},
    {"name": "payments_income", "category": "income", "keywords": ["stripe", "paypal", "klarna", "adyen", "shopify", "invoice", "customer payment"]},
    {"name": "bank_interest", "category": "income", "keywords": ["interest", "zinsen"]},
]


def _normalize_column_key(name: str) -> str:
    base = str(name).replace("\u00A0", " ").strip().lower()
    base = re.sub(r"[^\w]+", "_", base)
    base = re.sub(r"_+", "_", base)
    return base.strip("_")


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename common variants to canonical bookkeeping columns (date, amount, description, currency, iban).
    Keeps unknown columns intact.
    """
    if df is None:
        raise ValueError("DataFrame is None")

    standardized = df.copy()
    rename_map: Dict[str, str] = {}
    for col in standardized.columns:
        key = _normalize_column_key(col)
        for canonical, aliases in COLUMN_ALIASES.items():
            if key == canonical or key in aliases:
                rename_map[col] = canonical
                break

    standardized = standardized.rename(columns=rename_map)

    # Normalize date column if present
    if "date" in standardized.columns:
        standardized["date"] = pd.to_datetime(standardized["date"], errors="coerce")
        # retry with dayfirst for European-style dates
        if standardized["date"].notna().mean() < 0.9:
            standardized["date"] = standardized["date"].fillna(pd.to_datetime(standardized["date"], errors="coerce", dayfirst=True))

    # Derive year_month if available
    if "year_month" in standardized.columns:
        ym_raw = standardized["year_month"].astype(str).str.replace("/", "-", regex=False).str.strip()
        parsed = pd.to_datetime(ym_raw, errors="coerce")
        parsed = parsed.fillna(pd.to_datetime(ym_raw, errors="coerce", format="%b-%Y", dayfirst=False))
        parsed = parsed.fillna(pd.to_datetime(ym_raw, errors="coerce", format="%Y-%m"))
        standardized["year_month"] = parsed.dt.to_period("M").astype(str).where(parsed.notna(), pd.NA)
    elif "date" in standardized.columns:
        standardized["year_month"] = standardized["date"].dt.to_period("M").astype(str)

    # Create canonical amount if missing but net/gross columns exist
    if "amount" not in standardized.columns:
        for candidate in ["amount_net", "net_amount", "amount_gross", "gross_amount"]:
            if candidate in standardized.columns:
                standardized["amount"] = standardized[candidate]
                break

    if "currency" in standardized.columns:
        standardized["currency"] = (
            standardized["currency"]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"": pd.NA, "NONE": pd.NA, "NAN": pd.NA})
        )

    if "bk_category" in standardized.columns:
        standardized["bk_category"] = (
            standardized["bk_category"]
            .astype(str)
            .str.strip()
            .str.lower()
            .replace(
                {
                    "income": "income",
                    "cost": "cost",
                    "expenses": "cost",
                    "expense": "cost",
                    "payroll": "payroll",
                    "salary": "payroll",
                    "salaries": "payroll",
                    "tax": "vat_payment",
                    "vat_payment": "vat_payment",
                }
            )
        )
    return standardized


def _keyword_mask(series: pd.Series, keywords: Iterable[str]) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=bool)
    pattern = "|".join(re.escape(k) for k in keywords if k)
    if not pattern:
        return pd.Series(False, index=series.index)
    return series.str.contains(pattern, case=False, na=False)


def categorize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rule-based categorization using description/IBAN and amount sign as fallback.
    Adds bk_category and bk_rule columns.
    """
    if df is None:
        raise ValueError("DataFrame is None")

    enriched = df.copy()
    desc = enriched["description"].astype(str) if "description" in enriched else pd.Series("", index=enriched.index)
    iban = enriched["iban"].astype(str) if "iban" in enriched else pd.Series("", index=enriched.index)

    enriched["bk_category"] = pd.NA
    enriched["bk_rule"] = pd.NA

    for rule in RULES:
        field = iban if rule["name"] == "iban" else desc
        mask = enriched["bk_category"].isna() & _keyword_mask(field.str.lower(), rule["keywords"])
        if mask.any():
            enriched.loc[mask, "bk_category"] = rule["category"]
            enriched.loc[mask, "bk_rule"] = rule["name"]

    if "amount" in enriched.columns:
        amounts = pd.to_numeric(enriched["amount"], errors="coerce")
        uncategorized = enriched["bk_category"].isna()
        pos_mask = uncategorized & (amounts > 0)
        neg_mask = uncategorized & (amounts < 0)
        enriched.loc[pos_mask, "bk_category"] = "income"
        enriched.loc[pos_mask, "bk_rule"] = "sign_positive"
        enriched.loc[neg_mask, "bk_category"] = "cost"
        enriched.loc[neg_mask, "bk_rule"] = "sign_negative"

    return enriched


def compute_bookkeeping_summaries(df: pd.DataFrame, tax_rate: float = 0.19) -> Dict:
    """
    Compute revenue, cost, profit, VAT base/amount and grouped rollups (monthly, quarterly, by category).
    """
    if df is None:
        raise ValueError("DataFrame is None")
    if "amount" not in df.columns:
        raise ValueError("DataFrame must include an 'amount' column")

    data = df.copy()
    data["amount"] = pd.to_numeric(data["amount"], errors="coerce")
    data["bk_category"] = data.get("bk_category", pd.Series(pd.NA, index=data.index))

    revenue = data.loc[data["bk_category"] == "income", "amount"].sum()
    payroll = data.loc[data["bk_category"] == "payroll", "amount"].sum()
    cost = data.loc[data["bk_category"].isin(["cost", "payroll"]), "amount"].sum()
    profit = revenue + cost
    vat_base = revenue / (1 + tax_rate) if tax_rate else revenue
    vat_amount = revenue - vat_base

    cards = {
        "revenue": float(revenue) if pd.notna(revenue) else 0.0,
        "cost": float(cost) if pd.notna(cost) else 0.0,
        "profit": float(profit) if pd.notna(profit) else 0.0,
        "vat_base": float(vat_base) if pd.notna(vat_base) else 0.0,
        "vat_amount": float(vat_amount) if pd.notna(vat_amount) else 0.0,
        "payroll": float(payroll) if pd.notna(payroll) else 0.0,
    }

    by_category = (
        data.groupby("bk_category", dropna=False)["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "total"})
    )

    monthly = pd.DataFrame()
    quarterly = pd.DataFrame()
    if "date" in data.columns and pd.api.types.is_datetime64_any_dtype(data["date"]):
        dated = data.dropna(subset=["date"]).copy()
        dated["year_month"] = dated["date"].dt.to_period("M").astype(str)
        dated["year_quarter"] = dated["date"].dt.to_period("Q").astype(str)
        monthly = (
            dated.groupby(["year_month", "bk_category"], dropna=False)["amount"]
            .sum()
            .reset_index()
            .sort_values(["year_month", "bk_category"])
        )
        quarterly = (
            dated.groupby(["year_quarter", "bk_category"], dropna=False)["amount"]
            .sum()
            .reset_index()
            .sort_values(["year_quarter", "bk_category"])
        )

    return {
        "cards": cards,
        "monthly": monthly,
        "quarterly": quarterly,
        "by_category": by_category,
        "raw": data,
    }


def build_pnl_table(summaries: Dict) -> pd.DataFrame:
    """
    Build a minimal Profit & Loss table from compute_bookkeeping_summaries output.
    """
    cards = summaries.get("cards", {}) if summaries else {}
    rows = [
        {"item": "Revenue", "amount": cards.get("revenue", 0.0)},
        {"item": "Cost", "amount": cards.get("cost", 0.0)},
        {"item": "Payroll", "amount": cards.get("payroll", 0.0)},
        {"item": "Profit", "amount": cards.get("profit", 0.0)},
        {"item": "VAT base", "amount": cards.get("vat_base", 0.0)},
        {"item": "VAT amount", "amount": cards.get("vat_amount", 0.0)},
    ]
    return pd.DataFrame(rows, columns=["item", "amount"])


def detect_recurring(df: pd.DataFrame, min_count: int = 3) -> pd.DataFrame:
    """
    Flag recurring transactions by (iban, amount, bk_category) frequency.
    """
    if df is None:
        raise ValueError("DataFrame is None")
    data = df.copy()
    if "iban" not in data.columns or "amount" not in data.columns:
        data["is_recurring"] = False
        return data

    key_cols = ["iban", "amount"]
    if "bk_category" in data.columns:
        key_cols.append("bk_category")

    grouped = data.groupby(key_cols).size().reset_index(name="cnt")
    recurring = grouped[grouped["cnt"] >= min_count]

    data = data.merge(recurring[key_cols].assign(is_recurring=True), on=key_cols, how="left")
    data["is_recurring"] = data["is_recurring"].fillna(False)
    return data


def load_raw_transactions(path: str | Path) -> pd.DataFrame:
    """
    Load CSV/Excel, clean, and standardize core bookkeeping columns.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    elif path.suffix.lower() in {".csv"}:
        df = pd.read_csv(path)
    else:
        raise ValueError("Unsupported file type. Use .csv or .xlsx")

    df = clean_tabular(df)
    df = standardize_columns(df)
    return df


def process_tabular(df: pd.DataFrame, tax_rate: float = 0.19) -> Dict:
    """
    End-to-end helper: clean → standardize → categorize → detect recurring → compute summaries.
    """
    cleaned = clean_tabular(df)
    standardized = standardize_columns(cleaned)
    categorized = categorize_transactions(standardized)
    recurring = detect_recurring(categorized)
    summaries = compute_bookkeeping_summaries(recurring, tax_rate=tax_rate)
    summaries["raw"] = recurring
    return summaries
