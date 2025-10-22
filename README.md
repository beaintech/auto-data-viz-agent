# Auto Viz Agent (Excel → Charts → PDF)

A Streamlit MVP that turns Excel/CSV/Google Sheets into auto-generated visualizations and a downloadable PDF report. 
Includes optional AI-generated insights (OpenAI) and GitHub CI.

## Features
- Upload CSV/Excel (`.csv`, `.xlsx`) or paste Google Sheets URL
- Auto schema inference + quick summary
- Suggested charts: line, bar, pie; switchable
- Theme selection (light/dark/brand color)
- One-click PDF report export (charts + insights)
- Optional AI insights via `OPENAI_API_KEY`
- Ready-to-deploy on Render / Hugging Face Spaces / Streamlit Community
- GitHub Actions CI (lint + basic tests)

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