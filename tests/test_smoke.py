import pandas as pd
import pytest
from src.chart_suggester import suggest_charts
from src.report import build_pdf_report
from src.viz import render_chart
from src.chart_suggester import ChartSpec

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
