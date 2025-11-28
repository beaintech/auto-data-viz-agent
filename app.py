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
from src.pipeline import process_uploaded_file

LANGUAGE_NAMES = {"en": "English", "de": "Deutsch", "zh": "中文"}

TEXT = {
    "en": {
        "app_title": "Bookkeeping Automation ",
        "app_caption": "Upload a CSV/Excel or Google Sheets CSV URL. We clean transactions, auto-categorize, compute P&L KPIs, and can still export charts/PDF.",
        "settings": "Settings",
        "ui_language": "UI Language",
        "theme": "Theme",
        "enable_ai": "AI Insights (OpenAI)",
        "enable_ai_help": "Requires OPENAI_API_KEY in environment",
        "insight_language": "Insight Language",
        "processing_mode": "Processing mode",
        "processing_mode_help": "Auto detects bookkeeping-style sheets; bookkeeping forces KPIs; generic only cleans data.",
        "kpi_revenue": "Revenue",
        "kpi_cost": "Cost",
        "kpi_payroll": "Payroll",
        "kpi_profit": "Profit",
        "kpi_vat_base": "VAT base",
        "kpi_vat_amount": "VAT amount",
        "mode_auto": "Auto",
        "mode_bookkeeping": "Bookkeeping",
        "mode_generic": "Generic",
        "data_input": "Data Input",
        "upload_csv": "Upload CSV/Excel",
        "gsheet_url": "Google Sheets CSV URL (optional)",
        "drag_drop_title": "Drag & Drop Upload",
        "drag_drop_caption": "Drop files here or use the sidebar input; both feed the same pipeline.",
        "loaded_message": "Loaded **{name}** with shape {rows} rows × {cols} cols",
        "load_error": "Failed to load Google Sheets: {error}",
        "data_preview": "Data Preview",
        "summary": "ℹ️ Summary",
        "pipeline_debug": "Pipeline debug",
        "pipeline_used": "Mode requested: `{requested}` → used: `{used}`",
        "bookkeeping_title": "💼 Bookkeeping KPIs",
        "bookkeeping_not_applied": "Bookkeeping pipeline not applied (mode is generic or auto chose generic).",
        "bookkeeping_unavailable": "Bookkeeping KPIs unavailable: {error}",
        "suggested_charts": "Suggested Charts",
        "chart_type": "Type",
        "include_in_report": "Include in report",
        "generate_insights": "✍️ Generate Insights",
        "generating_insights": "Generating insights...",
        "insights": "Insights",
        "export": "Export",
        "report_title": "Report Title",
        "brand_author": "Brand/Author",
        "kaleido_warning": "`kaleido` is not available. Install dependencies via `pip install -r requirements.txt` in the same environment to enable PDF export.",
        "download_pdf": "Download PDF",
        "build_pdf_btn": "Generate PDF",
        "building_pdf": "Building PDF...",
        "build_pdf_fail": "Failed to build PDF: {error}",
        "upload_prompt": "Upload a file or paste a Google Sheets CSV URL in the sidebar to begin.",
        "cleaned_data": "Cleaned Data",
    },
    "de": {
        "app_title": "Buchhaltungsautomatisierung",
        "app_caption": "Laden Sie eine CSV/Excel oder Google-Sheets-CSV-URL hoch. Wir bereinigen Transaktionen, kategorisieren automatisch, berechnen GuV-KPIs und exportieren trotzdem Charts/PDF.",
        "settings": "Einstellungen",
        "ui_language": "Oberflächensprache",
        "theme": "Theme",
        "enable_ai": "KI-Insights (OpenAI)",
        "enable_ai_help": "Erfordert OPENAI_API_KEY in der Umgebung",
        "insight_language": "Insight-Sprache",
        "processing_mode": "Verarbeitungsmodus",
        "processing_mode_help": "Erkennt Buchhaltungs-Tabellen automatisch; 'bookkeeping' erzwingt KPIs; 'generic' bereinigt nur Daten.",
        "kpi_revenue": "Umsatz",
        "kpi_cost": "Kosten",
        "kpi_payroll": "Personalaufwand",
        "kpi_profit": "Gewinn",
        "kpi_vat_base": "MwSt-Basis",
        "kpi_vat_amount": "MwSt-Betrag",
        "mode_auto": "Auto",
        "mode_bookkeeping": "Buchhaltung",
        "mode_generic": "Generisch",
        "data_input": "Dateneingabe",
        "upload_csv": "CSV/Excel hochladen",
        "gsheet_url": "Google-Sheets-CSV-URL (optional)",
        "drag_drop_title": "Drag-&-Drop-Upload",
        "drag_drop_caption": "Dateien hier ablegen oder den Sidebar-Upload nutzen; beides nutzt denselben Ablauf.",
        "loaded_message": "Geladen: **{name}** mit {rows} Zeilen × {cols} Spalten",
        "load_error": "Google Sheets konnten nicht geladen werden: {error}",
        "data_preview": "Datenvorschau",
        "summary": "ℹ️ Zusammenfassung",
        "pipeline_debug": "Pipeline-Debug",
        "pipeline_used": "Modus angefordert: `{requested}` → verwendet: `{used}`",
        "bookkeeping_title": "💼 Buchhaltungs-KPIs",
        "bookkeeping_not_applied": "Buchhaltungspipeline nicht angewendet (Modus ist 'generic' oder Auto wählte 'generic').",
        "bookkeeping_unavailable": "Buchhaltungs-KPIs nicht verfügbar: {error}",
        "suggested_charts": "Vorgeschlagene Diagramme",
        "chart_type": "Typ",
        "include_in_report": "In Bericht aufnehmen",
        "generate_insights": "✍️ Insights erzeugen",
        "generating_insights": "Insights werden erzeugt...",
        "insights": "Insights",
        "export": "Export",
        "report_title": "Berichtstitel",
        "brand_author": "Marke/Autor",
        "kaleido_warning": "`kaleido` ist nicht verfügbar. Installieren Sie Abhängigkeiten mit `pip install -r requirements.txt` in derselben Umgebung, um PDF-Export zu aktivieren.",
        "download_pdf": "PDF herunterladen",
        "build_pdf_btn": "PDF erstellen",
        "building_pdf": "PDF wird erstellt...",
        "build_pdf_fail": "PDF konnte nicht erstellt werden: {error}",
        "upload_prompt": "Laden Sie eine Datei hoch oder fügen Sie eine Google-Sheets-CSV-URL in der Sidebar ein, um zu starten.",
        "cleaned_data": "Bereinigte Daten",
    },
    "zh": {
        "app_title": "财务自动化",
        "app_caption": "上传 CSV/Excel 或 Google Sheets CSV 链接。我们清洗交易、自动分类、计算损益 KPI，并可导出图表/PDF。",
        "settings": "设置",
        "ui_language": "界面语言",
        "theme": "主题",
        "enable_ai": "AI 洞察（OpenAI）",
        "enable_ai_help": "需要在环境中设置 OPENAI_API_KEY",
        "insight_language": "洞察语言",
        "processing_mode": "处理模式",
        "processing_mode_help": "自动检测财务表；bookkeeping 强制生成 KPI；generic 仅做清洗。",
        "kpi_revenue": "收入",
        "kpi_cost": "成本",
        "kpi_payroll": "工资",
        "kpi_profit": "利润",
        "kpi_vat_base": "增值税基数",
        "kpi_vat_amount": "增值税额",
        "mode_auto": "自动",
        "mode_bookkeeping": "财务",
        "mode_generic": "通用",
        "data_input": "数据输入",
        "upload_csv": "上传 CSV/Excel",
        "gsheet_url": "Google Sheets CSV 链接（可选）",
        "drag_drop_title": "拖放上传",
        "drag_drop_caption": "把文件拖到这里或用侧边栏上传，都会进入同一流程。",
        "loaded_message": "已加载 **{name}**，包含 {rows} 行 × {cols} 列",
        "load_error": "加载 Google Sheets 失败：{error}",
        "data_preview": "数据预览",
        "summary": "ℹ️ 摘要",
        "pipeline_debug": "流程调试",
        "pipeline_used": "请求的模式：`{requested}` → 使用：`{used}`",
        "bookkeeping_title": "💼 财务 KPI",
        "bookkeeping_not_applied": "未应用财务流程（模式为通用或自动选择了通用）。",
        "bookkeeping_unavailable": "财务 KPI 不可用：{error}",
        "suggested_charts": "推荐图表",
        "chart_type": "类型",
        "include_in_report": "加入报告",
        "generate_insights": "✍️ 生成洞察",
        "generating_insights": "正在生成洞察...",
        "insights": "洞察",
        "export": "导出",
        "report_title": "报告标题",
        "brand_author": "品牌/作者",
        "kaleido_warning": "`kaleido` 不可用。请在同一环境运行 `pip install -r requirements.txt` 以启用 PDF 导出。",
        "download_pdf": "下载 PDF",
        "build_pdf_btn": "生成 PDF",
        "building_pdf": "正在生成 PDF...",
        "build_pdf_fail": "生成 PDF 失败：{error}",
        "upload_prompt": "请先上传文件或在侧边栏粘贴 Google Sheets CSV 链接。",
        "cleaned_data": "清洗后的数据",
    },
}


def translate(key: str, lang: str, **kwargs) -> str:
    catalog = TEXT.get(lang, TEXT["en"])
    template = catalog.get(key) or TEXT["en"].get(key) or key
    return template.format(**kwargs)


def generate_data_summary(df: pd.DataFrame) -> str:
    rows, cols = df.shape
    parts = [f"{rows} rows, {cols} columns."]

    date_cols = [c for c in df.columns if "date" in c.lower() or "yearmonth" in c.lower()]
    if date_cols:
        col = date_cols[0]
        series = df[col]
        parsed = pd.to_datetime(series, errors="coerce")
        if parsed.notna().any():
            parts.append(f"Date range: {parsed.min().date()} → {parsed.max().date()}.")

    amount_cols = [c for c in df.columns if "amount" in c.lower() or "revenue" in c.lower() or "cost" in c.lower()]
    if amount_cols:
        samples = []
        for col in amount_cols[:3]:
            total = pd.to_numeric(df[col], errors="coerce").sum()
            samples.append(f"{col}: {total:,.2f}")
        parts.append("Key totals: " + "; ".join(samples))

    return " ".join(parts)


def apply_brand_palette(selected_theme: str):
    primary = "#0f9d58"  # money green
    if selected_theme == "Dark":
        background = "#0f1115"   # softer midnight
        secondary = "#161b22"    # gentle contrast for panels
        text = "#e7f1eb"
    else:
        background = "#ffffff"    # clean white canvas
        secondary = "#e6f4ec"     # light money green wash
        text = "#0c1c15"
    st.markdown(
        f"""
        <style>
        :root {{
            --primary-color: {primary};
            --text-color: {text};
            --secondary-background-color: {secondary};
            --background-color: {background};
        }}
        html, body, .stApp, .block-container {{
            background-color: var(--background-color) !important;
            color: var(--text-color) !important;
        }}
        [data-testid="stSidebar"] {{
            background-color: var(--secondary-background-color) !important;
            color: var(--text-color) !important;
        }}
        [data-testid="stHeader"] {{
            background-color: var(--secondary-background-color) !important;
        }}
        .stButton>button, .stDownloadButton>button {{
            background-color: {primary} !important;
            border: none !important;
            color: #ffffff !important;
        }}
        .stSelectbox div[data-baseweb="select"], .stTextInput input, .stFileUploader div[role="button"] {{
            color: var(--text-color) !important;
        }}
        .stFileUploader label div:first-child {{
            border: 1px dashed #ffffff !important;
            background-color: {secondary} !important;
            color: #ffffff !important;
        }}
        .st-bb {{ background-color: var(--background-color) !important; }}
        .st-cx {{ color: var(--text-color) !important; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_eur(value: float) -> str:
    return f"€{value:,.2f}"


def render_kpi_cards(cards: dict, translate_fn):
    """
    Render bookkeeping KPIs as small metric cards.
    """
    labels = [
        (translate_fn("kpi_revenue"), "revenue"),
        (translate_fn("kpi_cost"), "cost"),
        (translate_fn("kpi_payroll"), "payroll"),
        (translate_fn("kpi_profit"), "profit"),
        (translate_fn("kpi_vat_base"), "vat_base"),
        (translate_fn("kpi_vat_amount"), "vat_amount"),
    ]

    # Keep two rows of three cards for compact layout
    for chunk_start in range(0, len(labels), 3):
        chunk = labels[chunk_start : chunk_start + 3]
        cols = st.columns(len(chunk))
        for col, (label, key) in zip(cols, chunk):
            with col:
                st.metric(label=label, value=format_eur(cards.get(key, 0.0)))

st.set_page_config(
    page_title="Auto Data Visualization Agent | Buchhaltungs-Automatisierung | 财务自动化",
    page_icon="📈",
    layout="wide",
)

if "ui_language" not in st.session_state:
    st.session_state["ui_language"] = "en"

ui_language = st.sidebar.selectbox(
    "UI Language / Sprache / 语言",
    options=list(TEXT.keys()),
    index=list(TEXT.keys()).index(st.session_state["ui_language"]) if st.session_state.get("ui_language") in TEXT else 0,
    format_func=lambda code: LANGUAGE_NAMES.get(code, code),
    key="ui_language",
)


def t(key: str, **kwargs) -> str:
    return translate(key, ui_language, **kwargs)


theme_options = list(THEMES.keys())
default_theme = "Default"

st.title(t("app_title"))
st.caption(t("app_caption"))

with st.sidebar:
    st.header(f"⚙️ {t('settings')}")
    if "theme_choice" not in st.session_state:
        st.session_state["theme_choice"] = default_theme
    theme = st.selectbox(
        t("theme"),
        options=theme_options,
        index=theme_options.index(st.session_state["theme_choice"])
        if st.session_state.get("theme_choice") in theme_options
        else theme_options.index(default_theme),
        key="theme_choice",
    )
    enable_ai = st.toggle(t("enable_ai"), value=False, help=t("enable_ai_help"))
    insight_language = st.selectbox(t("insight_language"), list(LANGUAGE_NAMES.values()), index=0)
    processing_mode = st.selectbox(
        t("processing_mode"),
        options=["auto", "bookkeeping", "generic"],
        index=0,
        format_func=lambda mode: t(f"mode_{mode}"),
        help=t("processing_mode_help"),
    )
    st.divider()
    st.markdown(f"**{t('data_input')}**")
    uploaded_sidebar = st.file_uploader(t("upload_csv"), type=["csv", "xlsx"], key="sidebar_upload")
    gsheet_url_sidebar = st.text_input(t("gsheet_url"), key="sidebar_gsheet")

apply_brand_palette(theme)

with st.container(border=True):
    st.subheader(t("drag_drop_title"))
    st.caption(t("drag_drop_caption"))
    uploaded_main = st.file_uploader(t("upload_csv"), type=["csv", "xlsx"], key="main_upload")
    gsheet_url_main = st.text_input(t("gsheet_url"), key="main_gsheet")

df = None
src_name = None
uploaded = uploaded_main or uploaded_sidebar
gsheet_url = gsheet_url_main or gsheet_url_sidebar or ""

if uploaded is not None:
    df, src_name = load_from_upload(uploaded, clean=False)
elif gsheet_url.strip():
    try:
        df, src_name = load_from_gsheet_url(gsheet_url.strip(), clean=False)
    except Exception as e:
        st.error(t("load_error", error=e))

if df is not None:
    st.success(t("loaded_message", name=src_name, rows=df.shape[0], cols=df.shape[1]))
    pipeline_result = process_uploaded_file(df, mode=processing_mode, debug=False)
    df_for_viz = pipeline_result["df_final"]
    with st.expander(t("data_preview"), expanded=True):
        st.dataframe(df.head(50), use_container_width=True)
    with st.expander(t("cleaned_data"), expanded=True):
        st.dataframe(pipeline_result["cleaned"].head(200), use_container_width=True)

    # Bookkeeping KPI cards (only when bookkeeping path used)
    with st.expander(t("bookkeeping_title"), expanded=True):
        if pipeline_result["mode_used"] != "bookkeeping" or not pipeline_result.get("bookkeeping"):
            st.info(t("bookkeeping_not_applied"))
        else:
            try:
                cards = pipeline_result["bookkeeping"]["cards"] or {}
                defaults = {
                    "revenue": 0.0,
                    "cost": 0.0,
                    "payroll": 0.0,
                    "profit": cards.get("revenue", 0.0) + cards.get("cost", 0.0),
                    "vat_base": 0.0,
                    "vat_amount": 0.0,
                }
                for k, v in defaults.items():
                    cards.setdefault(k, v)

                render_kpi_cards(cards, t)
            except Exception as e:
                st.info(t("bookkeeping_unavailable", error=e))

    # Chart suggestions
    specs = suggest_charts(df_for_viz)
    st.subheader(t("suggested_charts"))
    chosen = []
    for idx, spec in enumerate(specs):
        with st.container(border=True):
            colL, colR = st.columns([3,2])
            with colL:
                fig = render_chart(df_for_viz, spec, theme=theme)
                st.plotly_chart(fig, use_container_width=True)
            with colR:
                st.write(f"**{t('chart_type')}**: {spec.kind}")
                desc_parts = []
                if spec.kind == "line":
                    desc_parts.append(f"Line chart: track {spec.y or 'y'} over {spec.x or 'x'} (agg={spec.agg}).")
                elif spec.kind == "bar":
                    desc_parts.append(f"Bar chart: compare {spec.y or 'y'} across {spec.x or 'x'} (agg={spec.agg}).")
                elif spec.kind == "pie":
                    desc_parts.append(f"Pie chart: share of {spec.y or 'y'} by {spec.category or spec.x}.")
                if spec.title:
                    desc_parts.append(f"Title: \"{spec.title}\".")
                st.write(" ".join(desc_parts))
                add = st.checkbox(t("include_in_report"), value=True, key=f"include_{idx}_{spec.kind}")
                if add:
                    chosen.append(spec)
    if not chosen:
        st.info("Select at least one chart with 'Include in report' to add it to the PDF.")

    # Insights
    insights_text = ""
    if st.button(t("generate_insights")) or (chosen and st.session_state.get("auto_insights_once") is None and enable_ai):
        st.session_state["auto_insights_once"] = True
        with st.spinner(t("generating_insights")):
            insights_text = generate_insights(df_for_viz, chosen, language=insight_language if enable_ai else None)
    if insights_text:
        st.subheader(t("insights"))
        st.write(insights_text)

    # Summary (moved near export)
    with st.expander(t("summary"), expanded=True):
        st.write(generate_data_summary(df))
        st.write(brief_summary(df))

    # PDF Export
    st.subheader(t("export"))
    report_title = st.text_input(t("report_title"), value=f"{t('app_title')} — {datetime.now().strftime('%Y-%m-%d')}")
    brand = st.text_input(t("brand_author"), value=t("app_title"))
    from src.report import kaleido_available
    kaleido_ok = kaleido_available()
    if not kaleido_ok:
        st.warning(t("kaleido_warning"))

    if st.button(t("build_pdf_btn"), disabled=not (kaleido_ok and chosen), key="build_pdf"):
        st.session_state.pop("pdf_bytes", None)
        with st.spinner(t("building_pdf")):
            try:
                pdf_bytes = build_pdf_report(df_for_viz, chosen, report_title, brand, theme=theme, insights=insights_text)
                st.session_state["pdf_bytes"] = pdf_bytes
            except Exception as e:
                st.error(t("build_pdf_fail", error=e))
                st.session_state.pop("pdf_bytes", None)

    if st.session_state.get("pdf_bytes"):
        st.download_button(
            t("download_pdf"),
            data=st.session_state["pdf_bytes"],
            file_name="auto_viz_report.pdf",
            mime="application/pdf",
            key="download_pdf_ready",
        )

else:
    st.info(t("upload_prompt"))
