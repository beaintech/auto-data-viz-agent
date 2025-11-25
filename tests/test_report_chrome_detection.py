from types import SimpleNamespace

import pytest

from src import report


def test_known_locations_mac(monkeypatch):
    monkeypatch.setattr(report.sys, "platform", "darwin")
    paths = report._known_chrome_locations()
    assert "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" in paths
    assert "/Applications/Chromium.app/Contents/MacOS/Chromium" in paths


def test_known_locations_windows(monkeypatch):
    monkeypatch.setattr(report.sys, "platform", "win32")
    monkeypatch.setenv("PROGRAMFILES", r"C:\\Program Files")
    monkeypatch.setenv("PROGRAMFILES(X86)", r"C:\\Program Files (x86)")
    paths = report._known_chrome_locations()
    assert r"C:\\Program Files/Google/Chrome/Application/chrome.exe" in paths
    assert r"C:\\Program Files (x86)/Google/Chrome/Application/chrome.exe" in paths


def test_find_chrome_falls_back_to_known_locations(monkeypatch):
    # Simulate macOS where Chrome is installed in the default app bundle but not on PATH.
    dummy_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    monkeypatch.setattr(report.sys, "platform", "darwin")
    monkeypatch.setattr(report.shutil, "which", lambda _: None)
    monkeypatch.setattr(report.os.path, "exists", lambda path: path == dummy_path)
    fake_scope = SimpleNamespace(chromium_executable=None)
    monkeypatch.setattr(report.pio, "kaleido", SimpleNamespace(scope=fake_scope), raising=False)

    assert report._find_chromium_executable() == dummy_path
