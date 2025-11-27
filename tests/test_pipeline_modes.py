import pandas as pd

from src.pipeline import process_uploaded_file


def test_bookkeeping_mode_computes_expected_cards():
    df = pd.DataFrame(
        {
            "Date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Description": ["Invoice 1", "Office supplies", "Snacks"],
            "Amount": [100.0, -40.0, -10.0],
        }
    )

    result = process_uploaded_file(df, mode="bookkeeping", debug=False)

    assert result["mode_used"] == "bookkeeping"
    cards = result["bookkeeping"]["cards"]
    assert cards["revenue"] == 100.0
    assert cards["cost"] == -50.0
    assert cards["profit"] == 50.0


def test_auto_mode_stays_generic_for_product_sales_table():
    df = pd.DataFrame(
        {
            "Product": ["A", "B", "C"],
            "Quarter": ["Q1", "Q2", "Q3"],
            "Sales": [1200, 1500, 900],
        }
    )

    result = process_uploaded_file(df, mode="auto", debug=False)

    assert result["mode_used"] == "generic"
    assert result["bookkeeping"] is None
    assert "sales" in result["df_final"].columns
    assert not result["detection"]["looks_bookkeeping"]


def test_auto_mode_stays_generic_for_medical_log():
    df = pd.DataFrame(
        {
            "patient": ["Alice", "Bob", "Alice"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "dosage_mg": [10, 12, 8],
            "note": ["checkup", "adjustment", "follow-up"],
        }
    )

    result = process_uploaded_file(df, mode="auto", debug=False)

    assert result["mode_used"] == "generic"
    assert not result["detection"]["looks_bookkeeping"]
    assert result["bookkeeping"] is None
