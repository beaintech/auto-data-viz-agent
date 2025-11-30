from typing import Dict
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from .chart_suggester import ChartSpec

MONEY_GREEN = ["#0f9d58", "#1fa776", "#2fc494", "#3fe1b2", "#61f3cd", "#7df6d9"]
MONEY_GREEN_DARK = ["#4ae3a8", "#35c38e", "#24a072", "#1b7f5b", "#145f44"]
DEFAULT_THEME = "Default"

THEMES: Dict[str, Dict] = {
    "Default": {"template": "plotly", "color_discrete_sequence": MONEY_GREEN},
    "Dark": {"template": "plotly_dark", "color_discrete_sequence": MONEY_GREEN_DARK},
}

THEME_STYLES: Dict[str, Dict] = {
    "Default": {
        "paper_bgcolor": "#ffffff",
        "plot_bgcolor": "#ffffff",
        "font_color": "#0c1c15",
        "gridcolor": "#d7e9de",
    },
    "Dark": {
        "paper_bgcolor": "#0f1115",
        "plot_bgcolor": "#0f1115",
        "font_color": "#e7f1eb",
        "gridcolor": "#1f2a32",
    },
}

def _prepare_line_data(df: pd.DataFrame, spec: ChartSpec):
    """
    Aggregate time series before plotting:
      - prefer the date column; fall back to spec.x if date is missing
      - coerce dates for proper sorting
      - sum the metric
    """
    df_local = df.copy()
    group_col = "date" if "date" in df_local.columns else spec.x
    if group_col not in df_local.columns:
        raise KeyError(f"Line chart requires time column '{group_col}' to exist.")

    df_local[group_col] = pd.to_datetime(df_local[group_col], errors="coerce")
    if spec.y == "__row_count__":
        agg = (
            df_local.dropna(subset=[group_col])
            .groupby(group_col, dropna=False)
            .size()
            .reset_index(name="value")
            .sort_values(group_col, ascending=True)
        )
        value_col = "value"
    else:
        df_local[spec.y] = pd.to_numeric(df_local[spec.y], errors="coerce")
        agg = (
            df_local.dropna(subset=[group_col, spec.y])
            .groupby(group_col, dropna=False)[spec.y]
            .sum(min_count=1)
            .reset_index()
            .sort_values(group_col, ascending=True)
        )
        value_col = spec.y
    return agg, group_col, value_col

def render_chart(df: pd.DataFrame, spec: ChartSpec, theme: str = "Default"):
    theme_cfg = THEMES.get(theme) or THEMES[DEFAULT_THEME]
    tpl = theme_cfg["template"]
    seq = theme_cfg["color_discrete_sequence"]

    def _safe_cat(series):
        return series.fillna("Missing").astype(str)

    # Avoid pd.NA issues in grouping/color
    df_local = df.copy()
    if spec.kind != "line" and spec.x and spec.x in df_local.columns:
        df_local[spec.x] = _safe_cat(df_local[spec.x])
    if spec.category and spec.category in df_local.columns:
        df_local[spec.category] = _safe_cat(df_local[spec.category])

    if spec.kind == "line":
        agg, group_col, value_col = _prepare_line_data(df_local, spec)
        fig = px.line(agg, x=group_col, y=value_col, template=tpl, color_discrete_sequence=seq)
        # Normalize title when we force date grouping
        if group_col == "date":
            title = "Count over date" if spec.y == "__row_count__" else f"{value_col} over date"
            fig.update_layout(title=title)
    elif spec.kind == "bar":
        if spec.y == "__row_count__":
            grouped = (
                df_local.dropna(subset=[spec.x])
                .groupby(spec.x)
                .size()
                .reset_index(name="value")
            )
            fig = px.bar(
                grouped,
                x=spec.x,
                y="value",
                template=tpl,
                color=spec.x if spec.x and grouped[spec.x].nunique() < 20 else None,
                color_discrete_sequence=seq,
            )
        else:
            fig = px.bar(
                df_local,
                x=spec.x,
                y=spec.y,
                template=tpl,
                color=spec.x if spec.x and df_local[spec.x].nunique() < 20 else None,
                color_discrete_sequence=seq,
            )
    elif spec.kind == "pie":
        if spec.category and spec.category in df_local.columns:
            df_local[spec.category] = _safe_cat(df_local[spec.category])
        if spec.y == "__row_count__":
            agg = (
                df_local.dropna(subset=[spec.category])
                .groupby(spec.category, dropna=False)
                .size()
                .reset_index(name="value")
            )
            value_col = "value"
        else:
            agg = df_local.groupby(spec.category)[spec.y].sum(min_count=1).reset_index()
            value_col = spec.y
        fig = px.pie(agg, names=spec.category, values=value_col, template=tpl, color_discrete_sequence=seq)
    elif spec.kind == "waterfall":
        agg = df_local.groupby(spec.category)[spec.y].sum().reset_index()
        fig = go.Figure(
            go.Waterfall(
                name="Contribution",
                orientation="v",
                x=agg[spec.category],
                y=agg[spec.y],
                decreasing={"marker": {"color": "#EF553B"}},
                increasing={"marker": {"color": "#00CC96"}},
                totals={"marker": {"color": "#636EFA"}},
            )
        )
        fig.update_layout(template=tpl)
    else:
        fig = px.scatter(df, x=spec.x, y=spec.y, template=tpl, color_discrete_sequence=seq)

    # Apply single-trace color fallback so money green is visible even without categories
    if seq:
        if spec.kind in {"line"}:
            for tr in fig.data:
                if getattr(tr, "line", None) and not tr.line.color:
                    tr.line.color = seq[0]
        elif spec.kind in {"bar", "scatter"}:
            for tr in fig.data:
                if getattr(tr, "marker", None) and not tr.marker.color:
                    tr.marker.color = seq[0]

    if spec.title:
        fig.update_layout(title=spec.title)
    # Fix height and disable autosize to avoid repeated auto-margin redraw warnings in Streamlit
    layout_style = THEME_STYLES.get(theme, {})
    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=50, b=20),
        autosize=False,
        colorway=seq if seq else None,
        paper_bgcolor=layout_style.get("paper_bgcolor"),
        plot_bgcolor=layout_style.get("plot_bgcolor"),
        font=dict(color=layout_style.get("font_color")),
        xaxis=dict(gridcolor=layout_style.get("gridcolor")),
        yaxis=dict(gridcolor=layout_style.get("gridcolor")),
    )
    return fig
