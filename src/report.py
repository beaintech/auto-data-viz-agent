# src/report.py
import io
import os
import shutil
import sys
from typing import List, Optional

import pandas as pd
import plotly.io as pio

from .chart_suggester import ChartSpec
from .viz import render_chart


def _require_reportlab():
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
        from reportlab.lib.units import cm
        from reportlab.lib.utils import ImageReader
        return A4, canvas, cm, ImageReader
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "PDF export requires `reportlab`. Install dependencies with `pip install -r requirements.txt`."
        ) from e


def _require_pillow():
    try:
        from PIL import Image
        return Image
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "PDF export requires `Pillow`. Install dependencies with `pip install -r requirements.txt`."
        ) from e


def kaleido_available() -> bool:
    """
    Returns True if kaleido is importable and Plotly can see its scope.
    """
    try:
        import kaleido  # noqa: F401
    except Exception:
        return False
    scope = getattr(pio, "kaleido", None)
    kaleido_scope = getattr(scope, "scope", None) if scope else None
    return kaleido_scope is not None and _find_chromium_executable(kaleido_scope) is not None


def _find_chromium_executable(kaleido_scope=None) -> Optional[str]:
    """
    Try to locate a Chrome/Chromium executable that Kaleido can use.
    Checks Kaleido's configured path first, then common executables on PATH.
    """
    scope = kaleido_scope or getattr(getattr(pio, "kaleido", None), "scope", None)
    if scope:
        exec_path = getattr(scope, "chromium_executable", None)
        if exec_path and os.path.exists(exec_path):
            return exec_path

    for name in ("chrome", "google-chrome", "chromium", "chromium-browser"):
        path = shutil.which(name)
        if path:
            return path

    for path in _known_chrome_locations():
        if os.path.exists(path):
            return path
    return None


def _known_chrome_locations() -> List[str]:
    """
    OS-specific Chrome/Chromium install locations that may not be on PATH
    (e.g., default macOS app bundles or Windows Program Files).
    """
    home = os.path.expanduser("~")
    if sys.platform == "darwin":
        return [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/Applications/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
            os.path.join(home, "Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
            os.path.join(home, "Applications/Chromium.app/Contents/MacOS/Chromium"),
        ]

    if sys.platform.startswith("win"):
        program_files = os.environ.get("PROGRAMFILES", r"C:\\Program Files")
        program_files_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")
        return [
            os.path.join(program_files, "Google/Chrome/Application/chrome.exe"),
            os.path.join(program_files_x86, "Google/Chrome/Application/chrome.exe"),
            os.path.join(program_files, "Chromium/Application/chrome.exe"),
            os.path.join(program_files_x86, "Chromium/Application/chrome.exe"),
        ]

    return [
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
    ]


def _fig_to_png_bytes(fig) -> bytes:
    """
    Convert a Plotly figure to PNG bytes using kaleido backend.
    Requires `kaleido` (listed in requirements.txt).
    """
    if not kaleido_available():
        raise RuntimeError(
            "Plotly static export failed because `kaleido` is missing or not detected. "
            "Install with `pip install -r requirements.txt` in the same environment and restart the app."
        )
    chrome_path = _find_chromium_executable()
    if chrome_path is None:
        raise RuntimeError(
            "Plotly static export failed because no Chrome/Chromium executable was found. "
            "Kaleido needs Google Chrome or Chromium on the host. Install Chrome manually or run `plotly_get_chrome` "
            "to download a headless build, then restart the app. On Linux, also ensure system libs are installed "
            "(libnss3, libatk, libgtk3, libasound2)."
        )
    try:
        return pio.to_image(fig, format="png", engine="kaleido", scale=2, validate=False)
    except Exception as e_high_res:
        # Retry with a smaller export to avoid memory/driver issues on some hosts (e.g., Streamlit Cloud)
        try:
            return pio.to_image(fig, format="png", engine="kaleido", scale=1, validate=False)
        except Exception as e:
            raise RuntimeError(
                "Plotly static export failed even though `kaleido` is installed. "
                f"Underlying error: {e}. Ensure Chrome/Chromium is installed (or run `plotly_get_chrome` to download "
                "a headless build). On hosted environments, also ensure system libs for headless Chrome are present "
                "(libnss3, libatk, libgtk3, libasound2) and that `kaleido` is up to date (`pip install -U kaleido`)."
            ) from e


def build_pdf_report(
    df: pd.DataFrame,
    specs: List[ChartSpec],
    title: str,
    brand: str,
    theme: str,
    insights: Optional[str] = None,
) -> bytes:
    """
    Build a multi-page PDF:
      - Cover page with title, brand, and optional insights
      - One page per chart in `specs`
    Returns PDF bytes.
    """
    A4, canvas, cm, ImageReader = _require_reportlab()
    Image = _require_pillow()

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    W, H = A4

    # Cover page
    c.setFont("Helvetica-Bold", 20)
    c.drawString(2 * cm, H - 3 * cm, title)
    c.setFont("Helvetica", 12)
    c.drawString(2 * cm, H - 4 * cm, f"Generated by {brand}")
    if insights:
        textobj = c.beginText(2 * cm, H - 6 * cm)
        textobj.setFont("Helvetica", 11)
        for line in insights.splitlines()[:25]:
            textobj.textLine(line[:110])
        c.drawText(textobj)
    c.showPage()

    # Chart pages
    for spec in specs:
        fig = render_chart(df, spec, theme=theme)
        png_bytes = _fig_to_png_bytes(fig)

        img = Image.open(io.BytesIO(png_bytes))
        # Fit to page margins
        max_w, max_h = W - 3 * cm, H - 4 * cm
        img.thumbnail((int(max_w), int(max_h)))

        img_bytes = io.BytesIO()
        img.save(img_bytes, format="PNG")
        img_bytes.seek(0)

        c.setFont("Helvetica-Bold", 14)
        c.drawString(2 * cm, H - 2 * cm, spec.title or f"{spec.kind} chart")
        c.drawImage(
            ImageReader(img_bytes),
            1.5 * cm,
            2.5 * cm,
            width=img.width,
            height=img.height,
            preserveAspectRatio=True,
            mask="auto",
        )
        c.showPage()

    c.save()
    buf.seek(0)
    return buf.read()
