import pandas as pd

from src.data_cleaner import cleaner


def test_data_cleaner_normalizes_and_coerces_types():
    raw = pd.DataFrame(
        {
            " price ": ["€1,20", " €2.50 ", "€1,20"],
            " date ": ["2024-01-01", "2024/01/02", "2024-01-01"],
            "City": [" Berlin ", "Berlin", "Berlin"],
            "notes": ["", None, ""],
        }
    )

    cleaned = cleaner.clean(raw)

    # column names get stripped
    assert "price" in cleaned.columns
    assert "date" in cleaned.columns

    # price-like column coerced to numeric even with currency + comma decimals
    assert pd.api.types.is_numeric_dtype(cleaned["price"])
    assert float(cleaned["price"].iloc[0]) == 1.20

    # date-like column coerced to datetime
    assert pd.api.types.is_datetime64_any_dtype(cleaned["date"])
    assert cleaned["date"].iloc[0].year == 2024

    # trimmed text and duplicate rows removed
    assert cleaned["city"].iloc[0] == "Berlin"
    assert len(cleaned) == 2


def test_finance_rows_only_dropped_when_date_and_amounts_missing():
    raw = pd.DataFrame(
        {
            "Date": [None, "2024-02-01", None, None],
            "Amount": [None, None, 5, None],
            "Note": ["header-ish", None, "has amount", "only meta"],
        }
    )

    cleaned = cleaner.clean(raw)

    # keep row with date but missing amount, and row with amount but missing date
    assert len(cleaned) == 2
    assert cleaned["amount"].isna().sum() == 1
    assert (cleaned["amount"] == 5).sum() == 1

    # drop rows where both date and all amount-like fields are missing
    assert pd.isna(cleaned["note"].iloc[0])
    assert cleaned["note"].iloc[1] == "has amount"
