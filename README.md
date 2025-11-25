# Auto Viz Agent (Excel → Charts → PDF)

A Streamlit MVP that turns Excel/CSV/Google Sheets into auto-generated visualizations and a downloadable PDF report. 
Includes optional AI-generated insights (OpenAI) and GitHub CI.

## Features
- Upload CSV/Excel (`.csv`, `.xlsx`) or paste Google Sheets URL
- Auto schema inference + quick summary
- Suggested charts: line, bar, pie; switchable
- Theme selection (light/dark/brand color)
- Built-in cleaner: trims headers/cells, coerces price/amount fields, parses numeric + datetime-ish columns
- One-click PDF report export (charts + insights)
- Optional AI insights via `OPENAI_API_KEY`
- Ready-to-deploy on Render / Hugging Face Spaces / Streamlit Community
- GitHub Actions CI (lint + basic tests)
- PDF export uses Plotly + Kaleido (no external Chrome required)

## Quickstart

```bash
# 1) Setup
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2) Run
streamlit run app.py

# 3) (Optional) AI insights
export OPENAI_API_KEY=sk-...   # Windows PowerShell: $env:OPENAI_API_KEY="sk-..."
```

### PDF export (Kaleido)
- Kaleido ships a headless Chromium binary; just install Python deps via `pip install -r requirements.txt`.
- If export fails, try upgrading Kaleido: `pip install -U kaleido`.
- On minimal Linux images you may still need basic GTK/NSS libs (`libnss3`, `libatk`, `libgtk3`, `libasound2`).

### Google Sheets
Paste a viewable CSV export link like:
```
https://docs.google.com/spreadsheets/d/<SHEET_ID>/gviz/tq?tqx=out:csv
```
(Or convert to CSV locally.)

## Project Structure
```
auto-viz-agent/
├─ app.py
├─ requirements.txt
├─ .gitignore
├─ .streamlit/config.toml
├─ src/
│  ├─ __init__.py
│  ├─ data_loader.py
│  ├─ chart_suggester.py
│  ├─ viz.py
│  ├─ insights.py
│  ├─ report.py
│  └─ utils.py
├─ assets/
│  └─ sample.csv
├─ tests/
│  └─ test_smoke.py
└─ .github/workflows/ci.yml
```

## Deploy

### Render
- Create a new Web Service
- Start command: `streamlit run app.py --server.port $PORT --server.address 0.0.0.0`

### Hugging Face Spaces
- Space type: **Streamlit**
- Add your files and `requirements.txt`

### Streamlit Community Cloud
- Connect your GitHub repo and pick `app.py`

## GitHub
```bash
git init
git add .
git commit -m "feat: MVP auto viz agent"
git branch -M main
git remote add origin <YOUR_REPO_GIT_URL>
git push -u origin main
```
# python -m venv .venv && source .venv/bin/activate
## streamlit run app.py --server.port $PORT --server.address 0.0.0.0

# python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
# pip install -r requirements.txt
# streamlit run app.py

https://auto-data-viz-agent-euthwbdgvjnaaalqbhmv92.streamlit.app/

https://beaintech-claude-data-viz-app-ojcplh.streamlit.app/

## Data cleaning model (Excel/CSV)
- The loader routes uploads through `src/data_cleaner.py`'s `cleaner.clean(df)` pipeline.
- It strips whitespace, converts empty strings to `NaN`, coerces price/amount columns (€, comma decimals), tries numeric/date conversion, and drops empty rows/columns + duplicates.
- Reuse it directly in your own code:
```python
from src.data_cleaner import cleaner
clean_df = cleaner.clean(df_from_excel_or_csv)
```
