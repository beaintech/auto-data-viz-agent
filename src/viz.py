from typing import Dict
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from .chart_suggester import ChartSpec

THEMES: Dict[str, Dict] = {
    "Default": {"template": "plotly", "color_discrete_sequence": None},
    "Dark": {"template": "plotly_dark", "color_discrete_sequence": None},
    "Brand Blue": {"template": "plotly", "color_discrete_sequence": px.colors.qualitative.Plotly},
}

def render_chart(df: pd.DataFrame, spec: ChartSpec, theme: str = "Default"):
    theme_cfg = THEMES.get(theme, THEMES["Default"])
    tpl = theme_cfg["template"]
    seq = theme_cfg["color_discrete_sequence"]

    def _safe_cat(series):
        return series.fillna("Missing").astype(str)

    # Avoid pd.NA issues in grouping/color
    df_local = df.copy()
    if spec.x and spec.x in df_local.columns:
        df_local[spec.x] = _safe_cat(df_local[spec.x])
    if spec.category and spec.category in df_local.columns:
        df_local[spec.category] = _safe_cat(df_local[spec.category])

    if spec.kind == "line":
        fig = px.line(df_local, x=spec.x, y=spec.y, template=tpl)
    elif spec.kind == "bar":
        fig = px.bar(
            df_local,
            x=spec.x,
            y=spec.y,
            template=tpl,
            color=spec.x if spec.x and df_local[spec.x].nunique() < 20 else None,
            color_discrete_sequence=seq,
        )
    elif spec.kind == "pie":
        agg = df_local.groupby(spec.category)[spec.y].sum().reset_index()
        fig = px.pie(agg, names=spec.category, values=spec.y, template=tpl, color_discrete_sequence=seq)
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

    if spec.title:
        fig.update_layout(title=spec.title)
    # Fix height and disable autosize to avoid repeated auto-margin redraw warnings in Streamlit
    fig.update_layout(height=420, margin=dict(l=20, r=20, t=50, b=20), autosize=False)
    return fig
