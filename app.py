import io
import os
import time
import json
from datetime import datetime
import streamlit as st
import pandas as pd

from src.data_loader import load_from_upload, load_from_gsheet_url, brief_summary
from src.chart_suggester import suggest_charts, ChartSpec
from src.viz import render_chart, THEMES
from src.insights import generate_insights
from src.report import build_pdf_report

st.set_page_config(page_title="Auto Data Visualization Agent", layout="wide")

st.title("🤖 AI Auto Data Visualization Agent — Excel → Charts → PDF")
st.caption("Upload a CSV/Excel or paste a Google Sheets CSV URL. Auto charts + optional AI insights + PDF export.")

with st.sidebar:
    st.header("⚙️ Settings")
    theme = st.selectbox("Theme", options=list(THEMES.keys()), index=0)
    enable_ai = st.toggle("AI Insights (OpenAI)", value=False, help="Requires OPENAI_API_KEY in environment")
    insight_language = st.selectbox("Insight Language", ["English", "Deutsch", "中文"], index=0)
    st.divider()
    st.markdown("**Data Input**")
    uploaded = st.file_uploader("Upload CSV/Excel", type=["csv", "xlsx"])
    gsheet_url = st.text_input("Google Sheets CSV URL (optional)")

df = None
src_name = None

if uploaded is not None:
    df, src_name = load_from_upload(uploaded)
elif gsheet_url.strip():
    try:
        df, src_name = load_from_gsheet_url(gsheet_url.strip())
    except Exception as e:
        st.error(f"Failed to load Google Sheets: {e}")

if df is not None:
    st.success(f"Loaded **{src_name}** with shape {df.shape[0]} rows × {df.shape[1]} cols")
    with st.expander("Data Preview", expanded=False):
        st.dataframe(df.head(50), use_container_width=True)
    with st.expander("ℹ️ Summary", expanded=False):
        st.write(brief_summary(df))

    # Chart suggestions
    specs = suggest_charts(df)
    st.subheader("🧠 Suggested Charts")
    chosen = []
    for spec in specs:
        with st.container(border=True):
            colL, colR = st.columns([3,2])
            with colL:
                fig = render_chart(df, spec, theme=theme)
                st.plotly_chart(fig, use_container_width=True)
            with colR:
                st.write(f"**Type**: {spec.kind}")
                st.json(spec.model_dump())
                add = st.checkbox("Include in report", value=True, key=f"include_{spec.kind}_{spec.y}_{spec.x}")
                if add:
                    chosen.append(spec)

    # Insights
    insights_text = ""
    if st.button("✍️ Generate Insights") or (chosen and st.session_state.get("auto_insights_once") is None and enable_ai):
        st.session_state["auto_insights_once"] = True
        with st.spinner("Generating insights..."):
            insights_text = generate_insights(df, chosen, language=insight_language if enable_ai else None)
    if insights_text:
        st.subheader("Insights")
        st.write(insights_text)

    # PDF Export
    st.subheader("Export")
    report_title = st.text_input("Report Title", value=f"Auto Data Visualization Agent — {datetime.now().strftime('%Y-%m-%d')}")
    brand = st.text_input("Brand/Author", value="Auto Data Visualization Agent")
    from src.report import kaleido_available
    kaleido_ok = kaleido_available()
    if not kaleido_ok:
        st.warning(
            "`kaleido`/Chrome not detected — install deps via `pip install -r requirements.txt` "
            "and ensure Google Chrome/Chromium is installed (or run `plotly_get_chrome`) to enable PDF export."
        )

    if st.button("Download PDF", disabled=not kaleido_ok):
        with st.spinner("Building PDF..."):
            try:
                pdf_bytes = build_pdf_report(df, chosen, report_title, brand, theme=theme, insights=insights_text)
            except Exception as e:
                st.error(f"Failed to build PDF: {e}")
                pdf_bytes = None
        if pdf_bytes:
            st.download_button("Download PDF", data=pdf_bytes, file_name="auto_viz_report.pdf", mime="application/pdf")

else:
    st.info("Upload a file or paste a Google Sheets CSV URL in the sidebar to begin.")
