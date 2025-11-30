import pandas as pd
import pytest
from src.chart_suggester import (
    ChartSpec,
    build_chart_data,
    filter_x_candidates,
    deduplicate_y_candidates,
    generate_all_charts,
    get_valid_x_columns,
    get_valid_y_columns,
    suggest_charts,
)
from src.report import build_pdf_report
from src.viz import render_chart

def test_suggest_and_render():
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=5, freq="D"),
        "sales": [10, 12, 11, 15, 14],
        "region": ["A","B","A","B","A"],
    })
    specs = suggest_charts(df)
    assert specs, "No chart suggestions"
    fig = render_chart(df, specs[0])
    assert fig is not None

def test_pdf_report():
    pytest.importorskip("kaleido")
    import pandas as pd
    df = pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=3, freq="D"),
        "value": [1,3,2],
        "cat": ["x","y","x"]
    })
    spec = ChartSpec(kind="line", x="date", y="value", title="Value over time")
    try:
        pdf = build_pdf_report(df, [spec], "Test Report", "Auto Viz Agent", theme="Default", insights="Test")
    except RuntimeError as e:
        if "kaleido" in str(e).lower():
            pytest.skip(f"Kaleido export unavailable in test environment: {e}")
        raise
    assert isinstance(pdf, (bytes, bytearray)) and len(pdf) > 1000


def test_axis_filters_and_generation():
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", periods=3, freq="D"),
            "bk_category": ["a", "a", "a"],  # should be filtered out (only one unique)
            "customer": ["A", "B", "C"],
            "amount_net": [10, 20, 30],
            "revenue": [10, 20, 30],  # duplicate of amount_net
            "salesamount": [10, 20, 30],  # name-similar duplicate
            "iban": ["x", "y", "z"],  # should be excluded
            "document_no": [1, 2, 3],  # should be excluded
        }
    )
    x_cols = filter_x_candidates(df, get_valid_x_columns(df))
    y_cols = deduplicate_y_candidates(df, get_valid_y_columns(df))

    assert "Date" in x_cols
    assert "bk_category" not in x_cols  # filtered out for low cardinality
    assert "customer" in x_cols
    assert "iban" not in x_cols and "document_no" not in x_cols

    assert set(y_cols) == {"amount_net"}  # deduplicated revenue/salesamount

    charts = generate_all_charts(df)
    assert charts, "No charts generated from valid columns"
    for spec in charts:
        # pie/waterfall store dimension in category
        if spec.kind in {"pie", "waterfall"}:
            assert spec.category in x_cols and spec.y in y_cols
        else:
            assert spec.x in x_cols and spec.y in y_cols
            if spec.x == "Date":
                assert spec.kind == "line"
            else:
                assert spec.kind == "bar"


def test_generic_dataframe_without_finance_columns():
    df = pd.DataFrame(
        {
            "created_on": pd.date_range("2024-01-01", periods=4, freq="D"),
            "category": ["a", "b", "a", "b"],
            "score": [1, 2, 3, 4],
            "visits": [10, 20, 30, 40],
        }
    )
    charts = generate_all_charts(df)
    assert charts, "Charts should be generated even without financial columns"
    xs = {spec.x for spec in charts}
    ys = {spec.y for spec in charts}
    assert "created_on" in xs or "category" in xs
    assert "score" in ys or "visits" in ys


def test_pie_chart_added_for_categorical():
    df = pd.DataFrame(
        {
            "category": ["a", "b", "a", "b"],
            "value": [1, 2, 3, 4],
        }
    )
    charts = generate_all_charts(df)
    kinds = [c.kind for c in charts]
    assert "pie" in kinds, "Pie chart should be generated for categorical data"


def test_waterfall_for_finance_categorical():
    df = pd.DataFrame(
        {
            "category": ["a", "b", "a", "b"],
            "amount": [100, -50, 30, -10],
        }
    )
    charts = generate_all_charts(df)
    kinds = [c.kind for c in charts]
    assert "waterfall" in kinds, "Waterfall chart should be included for finance categorical data"
